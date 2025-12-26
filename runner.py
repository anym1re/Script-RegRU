import ipaddress
import random
import time
from typing import Dict, List, Optional, Set, Tuple

from playwright.sync_api import sync_playwright

from auth import ensure_logged_in
from config import Config, URL_FLOATING_IPS
from ip_ops import create_one_ip_moscow, delete_ip, has_fatal_error, match_target_network
from logging_utils import acquire_lock, release_lock, setup_logging
from stats import (
    format_stats_table,
    get_known_subnets,
    select_rare_subnets,
    update_daily_stats,
)
from telegram_utils import format_pre, send_telegram_message
from timing_utils import cooldown_between_mutations
from ui import list_ips_from_table, list_rows_from_table, wait_page_ready


def choose_strategy(cfg: Config, logger) -> Tuple[str, List[ipaddress.IPv4Network]]:
    mode = cfg.strategy_mode
    if mode not in ("auto", "main", "rare", "single"):
        logger.warning("Unknown strategy_mode=%s; treating as main.", mode)
        return "main", []

    if mode == "main":
        return "main", []

    if mode == "single":
        return "single", []

    if mode == "rare":
        rare_networks = select_rare_subnets(cfg, logger)
        if not rare_networks:
            logger.warning(
                "Rare strategy requested but no rare subnets found; falling back to main."
            )
            return "main", []
        return "rare", rare_networks

    if random.random() >= cfg.strategy_auto_probability:
        return "main", []

    rare_networks = select_rare_subnets(cfg, logger)
    if not rare_networks:
        return "main", []

    return "rare", rare_networks


def update_cycle_stats(ip: str, cycle_counts: Dict[str, int]) -> None:
    try:
        subnet = str(ipaddress.ip_network(f"{ip}/24", strict=False))
    except ValueError:
        return
    cycle_counts[subnet] = cycle_counts.get(subnet, 0) + 1


def notify_cycle_stats(logger, cycle_counts: Dict[str, int]) -> None:
    summary = (
        "Статистика подсетей за текущий цикл. "
        "subnet = подсеть /24, total_count = сколько раз IP из этой подсети создан в этом цикле."
    )
    send_telegram_message(logger, summary)
    table = format_stats_table(cycle_counts).rstrip("\n")
    send_telegram_message(logger, format_pre(table), parse_mode="HTML")


def notify_error(logger, message: str, *, fatal: bool = True) -> None:
    prefix = "Фатальная ошибка" if fatal else "Ошибка"
    lower_message = message.lower()
    if fatal and lower_message.startswith("фатальная ошибка"):
        text = message
    else:
        text = f"{prefix}: {message}"
    send_telegram_message(logger, text)


def read_current_state(page) -> Tuple[List[str], int]:
    rows = list_rows_from_table(page)
    current_ips = [r.ip for r in rows if r.ip]
    pending_slots = sum(1 for r in rows if r.status == "Создается" and not r.ip)
    return current_ips, pending_slots


def should_stop_due_to_target_slot(
    cfg: Config,
    current_ips: List[str],
    pending_slots: int,
    matched_target_ips: Set[str],
) -> bool:
    if not matched_target_ips:
        return False
    total_slots = len(current_ips) + pending_slots
    if total_slots < cfg.account_limit:
        return False
    current_set = set(current_ips)
    return any(ip in current_set for ip in matched_target_ips)


def wait_for_new_ip_single(
    page,
    cfg: Config,
    logger,
    before_ips: Set[str],
) -> Tuple[str, Optional[str]]:
    reloads = 0
    next_reload_at = time.time() + cfg.single_reload_every_s

    while True:
        if has_fatal_error(page, cfg, logger):
            return "fatal", None

        if URL_FLOATING_IPS not in page.url:
            page.goto(URL_FLOATING_IPS)
            wait_page_ready(page)

        try:
            current_ips = set(list_ips_from_table(page))
        except Exception as e:
            logger.warning("Ошибка чтения списка IP, пробуем перезагрузку: %s", e)
            page.reload()
            wait_page_ready(page)
            reloads += 1
            if reloads >= cfg.single_max_reload_attempts:
                return "restart", None
            continue

        new_ips = current_ips - before_ips
        if new_ips:
            return "ok", next(iter(new_ips))

        if reloads >= cfg.single_max_reload_attempts:
            return "restart", None

        if time.time() >= next_reload_at:
            logger.info("Single wait: reload page while waiting for creation.")
            page.reload()
            wait_page_ready(page)
            reloads += 1
            next_reload_at = time.time() + cfg.single_reload_every_s
            continue

        time.sleep(random.uniform(cfg.poll_sleep_min, cfg.poll_sleep_max))


def wait_for_ip_removal_single(
    page,
    cfg: Config,
    logger,
    ip: str,
) -> str:
    reloads = 0
    next_reload_at = time.time() + cfg.single_reload_every_s

    while True:
        if has_fatal_error(page, cfg, logger):
            return "fatal"

        if URL_FLOATING_IPS not in page.url:
            page.goto(URL_FLOATING_IPS)
            wait_page_ready(page)

        try:
            current_ips = set(list_ips_from_table(page))
        except Exception as e:
            logger.warning("Ошибка чтения списка IP, пробуем перезагрузку: %s", e)
            page.reload()
            wait_page_ready(page)
            reloads += 1
            if reloads >= cfg.single_max_reload_attempts:
                return "restart"
            continue

        if ip not in current_ips:
            return "ok"

        if reloads >= cfg.single_max_reload_attempts:
            return "restart"

        if time.time() >= next_reload_at:
            logger.info("Single wait: reload page while waiting for deletion.")
            page.reload()
            wait_page_ready(page)
            reloads += 1
            next_reload_at = time.time() + cfg.single_reload_every_s
            continue

        time.sleep(random.uniform(cfg.poll_sleep_min, cfg.poll_sleep_max))


def wait_for_new_ip(
    page,
    cfg: Config,
    logger,
    before_ips: Set[str],
    timeout_s: int,
) -> Optional[str]:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if URL_FLOATING_IPS not in page.url:
            page.goto(URL_FLOATING_IPS)
            wait_page_ready(page)
        try:
            current_ips = set(list_ips_from_table(page))
        except Exception as e:
            logger.warning("Ошибка чтения списка IP, пробуем перезагрузку: %s", e)
            page.reload()
            wait_page_ready(page)
            time.sleep(random.uniform(cfg.poll_sleep_min, cfg.poll_sleep_max))
            continue
        new_ips = current_ips - before_ips
        if new_ips:
            return next(iter(new_ips))
        time.sleep(random.uniform(cfg.poll_sleep_min, cfg.poll_sleep_max))
    return None


def wait_for_ip_removal(
    page,
    cfg: Config,
    logger,
    ip: str,
    timeout_s: int,
) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if URL_FLOATING_IPS not in page.url:
            page.goto(URL_FLOATING_IPS)
            wait_page_ready(page)
        try:
            current_ips = set(list_ips_from_table(page))
        except Exception as e:
            logger.warning("Ошибка чтения списка IP, пробуем перезагрузку: %s", e)
            page.reload()
            wait_page_ready(page)
            time.sleep(random.uniform(cfg.poll_sleep_min, cfg.poll_sleep_max))
            continue
        if ip not in current_ips:
            return True
        time.sleep(random.uniform(cfg.poll_sleep_min, cfg.poll_sleep_max))
    return False


def format_duration(seconds: float) -> str:
    if seconds >= 3600:
        return f"{seconds / 3600:.1f} ч"
    if seconds >= 60:
        return f"{seconds / 60:.1f} мин"
    return f"{seconds:.1f} сек"


def notify_status(logger, message: str) -> None:
    send_telegram_message(logger, message)


def notify_pause(logger, reason: str, seconds: float) -> None:
    text = f"Пауза: {reason}. Длительность: {format_duration(seconds)}."
    send_telegram_message(logger, text)


def notify_target_hit(
    logger,
    ip: str,
    subnet: ipaddress.IPv4Network,
    total_ips: int,
    total_subnets: int,
) -> None:
    text = (
        f"Найден целевой IP: {ip} в {subnet}. "
        f"Итог: IP={total_ips}, подсетей={total_subnets}."
    )
    send_telegram_message(logger, text)


def exit_with_error(cfg: Config, logger, code: int, message: str) -> int:
    notify_error(logger, message)
    return code


def cleanup_non_target_ips(
    page,
    cfg: Config,
    logger,
    target_networks: List[ipaddress.IPv4Network],
) -> bool:
    if URL_FLOATING_IPS not in page.url:
        page.goto(URL_FLOATING_IPS)
        wait_page_ready(page)

    try:
        current_ips = list_ips_from_table(page)
    except Exception as e:
        logger.warning("Не удалось прочитать список IP перед очисткой: %s", e)
        try:
            page.reload()
            wait_page_ready(page)
            current_ips = list_ips_from_table(page)
        except Exception as e2:
            logger.warning("Повторное чтение списка IP не удалось: %s", e2)
            return False

    for ip in current_ips:
        if match_target_network(ip, target_networks):
            continue
        cooldown_between_mutations(cfg, logger)
        delete_result = delete_ip(page, cfg, logger, ip)
        if delete_result.status == "pending":
            logger.info("Удаление в процессе, слот занят. Продолжаю очистку.")
            wait_page_ready(page)
            continue
        if delete_result.status != "deleted":
            logger.warning("Delete failed during final cleanup.")
            return False
        wait_page_ready(page)

    return True


def run(cfg: Config) -> int:
    logger = setup_logging()
    acquire_lock(logger)

    p = sync_playwright().start()

    browser = p.chromium.launch(
        headless=cfg.headless,
        args=["--no-sandbox", "--disable-setuid-sandbox"],
    )

    context = browser.new_context(viewport={"width": 1400, "height": 900})
    page = context.new_page()

    try:
        target_networks: List[ipaddress.IPv4Network] = []
        for cidr in cfg.target_cidrs:
            try:
                target_networks.append(ipaddress.ip_network(cidr))
            except ValueError:
                logger.warning("Некорректный CIDR: %s", cidr)

        matched_target_ips: Set[str] = set()
        matched_target_subnets: Set[str] = set()
        paused_after_first_target = False
        cycle_counts: Dict[str, int] = {}
        cycle_index = 0

        while True:
            cycle_index += 1
            ensure_logged_in(page, cfg, logger)

            if URL_FLOATING_IPS not in page.url:
                page.goto(URL_FLOATING_IPS)
                wait_page_ready(page)

            base_ips = set(list_ips_from_table(page))
            logger.info("Base IPs (protected): %d", len(base_ips))
            last_ips: Set[str] = set(base_ips)

            strategy, rare_networks = choose_strategy(cfg, logger)
            logger.info("Strategy for this run: %s", strategy)
            cycle_details = [
                f"Старт цикла #{cycle_index}.",
                f"Стратегия: {strategy}.",
                f"Базовых IP: {len(base_ips)}.",
                f"Целевых CIDR: {len(target_networks)}.",
            ]
            if strategy == "rare":
                cycle_details.append(
                    f"Редких подсетей в бакете: {len(rare_networks)}."
                )
            notify_status(logger, " ".join(cycle_details))

            restart_cycle = False

            if strategy == "rare":
                total_created = 0
                probe_slots = max(1, cfg.rare_rotation_slots)
                rare_keep_max = max(0, cfg.rare_keep_max)
                rare_goal = random.randint(
                    cfg.rare_goal_created_min, cfg.rare_goal_created_max
                )
                keep_cap = cfg.account_limit - probe_slots - len(base_ips) - len(
                    matched_target_ips
                )
                keep_cap = max(0, min(rare_keep_max, keep_cap))
                matched_rare_ips: Set[str] = set()
                known_subnets = get_known_subnets(cfg, logger)
                pause_after_cleanup_s: Optional[float] = None
                pause_reason: Optional[str] = None

                logger.info(
                    "Rare strategy: keep up to %d rare (probe slots=%d)",
                    keep_cap,
                    probe_slots,
                )
                logger.info(
                    "Rare strategy: goal %d created (range %d-%d)",
                    rare_goal,
                    cfg.rare_goal_created_min,
                    cfg.rare_goal_created_max,
                )
                logger.info("Rare subnets in bucket: %d", len(rare_networks))

                while True:
                    if total_created >= rare_goal:
                        logger.info(
                            "Rare strategy: goal reached (%d/%d).",
                            total_created,
                            rare_goal,
                        )
                        break
                    if URL_FLOATING_IPS not in page.url:
                        page.goto(URL_FLOATING_IPS)
                        wait_page_ready(page)

                    current_ips, pending_slots = read_current_state(page)
                    new_ips = [ip for ip in current_ips if ip not in last_ips]
                    if new_ips:
                        stop_rare = False
                        for ip in new_ips:
                            total_created += 1
                            logger.info("Detected new IP: %s", ip)
                            update_daily_stats(ip, cfg, logger)
                            update_cycle_stats(ip, cycle_counts)

                            hit_net = (
                                match_target_network(ip, target_networks)
                                if target_networks
                                else None
                            )
                            if hit_net:
                                matched_target_ips.add(ip)
                                matched_target_subnets.add(str(hit_net))
                                logger.info("Target CIDR hit: %s in %s", ip, hit_net)
                                notify_target_hit(
                                    logger,
                                    ip,
                                    hit_net,
                                    len(matched_target_ips),
                                    len(matched_target_subnets),
                                )
                                logger.info(
                                    "Переборный слот закрыт, редкая стратегия завершена."
                                )
                                stop_rare = True
                                break

                            rare_hit = (
                                match_target_network(ip, rare_networks)
                                if rare_networks
                                else None
                            )
                            try:
                                subnet = str(
                                    ipaddress.ip_network(f"{ip}/24", strict=False)
                                )
                            except ValueError:
                                subnet = ""
                            is_new_subnet = subnet and subnet not in known_subnets
                            if is_new_subnet:
                                known_subnets.add(subnet)
                                logger.info("New subnet in stats: %s", subnet)

                            if rare_hit:
                                if len(matched_rare_ips) < keep_cap:
                                    matched_rare_ips.add(ip)
                                    logger.info("Rare subnet hit: %s in %s", ip, rare_hit)
                                    logger.info(
                                        "Rare keep: %d/%d",
                                        len(matched_rare_ips),
                                        keep_cap,
                                    )
                                else:
                                    logger.info(
                                        "Rare subnet hit (limit %d reached); keep probing.",
                                        keep_cap,
                                    )
                            elif is_new_subnet:
                                if len(matched_rare_ips) < keep_cap:
                                    matched_rare_ips.add(ip)
                                    logger.info("Rare by new subnet: %s", subnet)
                                    logger.info(
                                        "Rare keep: %d/%d",
                                        len(matched_rare_ips),
                                        keep_cap,
                                    )
                                else:
                                    logger.info(
                                        "New subnet (limit %d reached); keep probing.",
                                        keep_cap,
                                    )
                        if stop_rare:
                            last_ips = set(current_ips)
                            break
                    last_ips = set(current_ips)
                    protected_ips = set(base_ips) | matched_target_ips | matched_rare_ips
                    probe_ips = [ip for ip in current_ips if ip not in protected_ips]

                    total_slots = len(current_ips) + pending_slots
                    if total_slots >= cfg.account_limit:
                        if not probe_ips:
                            logger.info(
                                "Нет свободных слотов для перебора. Завершаю редкую стратегию."
                            )
                            break
                        cooldown_between_mutations(cfg, logger)
                        delete_result = delete_ip(page, cfg, logger, probe_ips[0])
                        if delete_result.status == "pending":
                            logger.info(
                                "Удаление в процессе, слот занят. Продолжаю работу."
                            )
                            wait_page_ready(page)
                            continue
                        if delete_result.status != "deleted":
                            logger.warning("Delete failed.")
                            if has_fatal_error(page, cfg, logger):
                                logger.error("Фатальная ошибка. Выход.")
                                return exit_with_error(
                                    cfg,
                                    logger,
                                    3,
                                    "Фатальная ошибка при удалении IP (редкая стратегия).",
                                )
                            pause_after_cleanup_s = cfg.failure_pause_s
                            pause_reason = (
                                "Нефатальная ошибка при удалении IP (редкая стратегия); "
                                "перезапуск цикла"
                            )
                            restart_cycle = True
                            break
                        wait_page_ready(page)
                        continue

                    if len(probe_ips) >= probe_slots:
                        cooldown_between_mutations(cfg, logger)
                        delete_result = delete_ip(page, cfg, logger, probe_ips[0])
                        if delete_result.status == "pending":
                            logger.info(
                                "Удаление в процессе, слот занят. Продолжаю работу."
                            )
                            wait_page_ready(page)
                            continue
                        if delete_result.status != "deleted":
                            logger.warning("Delete failed.")
                            if has_fatal_error(page, cfg, logger):
                                logger.error("Фатальная ошибка. Выход.")
                                return exit_with_error(
                                    cfg,
                                    logger,
                                    3,
                                    "Фатальная ошибка при удалении IP (редкая стратегия).",
                                )
                            pause_after_cleanup_s = cfg.failure_pause_s
                            pause_reason = (
                                "Нефатальная ошибка при удалении IP (редкая стратегия); "
                                "перезапуск цикла"
                            )
                            restart_cycle = True
                            break
                        wait_page_ready(page)
                        continue

                    cooldown_between_mutations(cfg, logger)

                    result = create_one_ip_moscow(page, cfg, logger)
                    if result.status == "created" and result.ip:
                        ip = result.ip
                        last_ips.add(ip)
                        total_created += 1
                        logger.info("Created: %s", ip)
                        update_daily_stats(ip, cfg, logger)
                        update_cycle_stats(ip, cycle_counts)

                        hit_net = (
                            match_target_network(ip, target_networks)
                            if target_networks
                            else None
                        )
                        if hit_net:
                            matched_target_ips.add(ip)
                            matched_target_subnets.add(str(hit_net))
                            logger.info("Target CIDR hit: %s in %s", ip, hit_net)
                            notify_target_hit(
                                logger,
                                ip,
                                hit_net,
                                len(matched_target_ips),
                                len(matched_target_subnets),
                            )
                            logger.info("Переборный слот закрыт, редкая стратегия завершена.")
                            break

                        rare_hit = (
                            match_target_network(ip, rare_networks)
                            if rare_networks
                            else None
                        )
                        try:
                            subnet = str(ipaddress.ip_network(f"{ip}/24", strict=False))
                        except ValueError:
                            subnet = ""
                        is_new_subnet = subnet and subnet not in known_subnets
                        if is_new_subnet:
                            known_subnets.add(subnet)
                            logger.info("New subnet in stats: %s", subnet)

                        if rare_hit:
                            if len(matched_rare_ips) < keep_cap:
                                matched_rare_ips.add(ip)
                                logger.info("Rare subnet hit: %s in %s", ip, rare_hit)
                                logger.info(
                                    "Rare keep: %d/%d",
                                    len(matched_rare_ips),
                                    keep_cap,
                                )
                            else:
                                logger.info(
                                    "Rare subnet hit (limit %d reached); keep probing.",
                                    keep_cap,
                                )
                        elif is_new_subnet:
                            if len(matched_rare_ips) < keep_cap:
                                matched_rare_ips.add(ip)
                                logger.info("Rare by new subnet: %s", subnet)
                                logger.info(
                                    "Rare keep: %d/%d",
                                    len(matched_rare_ips),
                                    keep_cap,
                                )
                            else:
                                logger.info(
                                    "New subnet (limit %d reached); keep probing.",
                                    keep_cap,
                                )

                        wait_page_ready(page)
                    elif result.status == "pending":
                        logger.info(
                            "Создание в статусе 'Создается'. Слот занят, продолжаю работу."
                        )
                    else:
                        logger.warning("Create failed/timeout.")
                        if has_fatal_error(page, cfg, logger):
                            logger.error("Фатальная ошибка. Выход.")
                            return exit_with_error(
                                cfg,
                                logger,
                                2,
                                "Фатальная ошибка при создании IP (редкая стратегия).",
                            )
                        pause_after_cleanup_s = cfg.failure_pause_s
                        pause_reason = (
                            "Нефатальная ошибка при создании IP (редкая стратегия); "
                            "перезапуск цикла"
                        )
                        restart_cycle = True
                        break

                if pause_after_cleanup_s:
                    logger.info(
                        "Нефатальная ошибка. Пауза на %.1f минут и перезапуск цикла.",
                        cfg.failure_pause_s / 60,
                    )
                    if pause_reason:
                        notify_pause(logger, pause_reason, pause_after_cleanup_s)
                    time.sleep(pause_after_cleanup_s)
                    if restart_cycle:
                        continue

            elif strategy == "single":
                total_created = 0
                round_created = 0
                restart_cycle = False
                goal_created = random.randint(
                    cfg.single_goal_created_min, cfg.single_goal_created_max
                )
                round_size = max(1, cfg.single_round_size)
                pause_after_cleanup_s: Optional[float] = None
                pause_reason: Optional[str] = None

                logger.info(
                    "Single strategy: goal %d created (range %d-%d)",
                    goal_created,
                    cfg.single_goal_created_min,
                    cfg.single_goal_created_max,
                )
                logger.info(
                    "Single strategy: round size=%d, pause %d-%d s",
                    round_size,
                    cfg.single_round_pause_min_s,
                    cfg.single_round_pause_max_s,
                )

                while total_created < goal_created:
                    if URL_FLOATING_IPS not in page.url:
                        page.goto(URL_FLOATING_IPS)
                        wait_page_ready(page)

                    current_ips, pending_slots = read_current_state(page)
                    if should_stop_due_to_target_slot(
                        cfg, current_ips, pending_slots, matched_target_ips
                    ):
                        logger.info(
                            "Target IP occupies last slot. Stopping single strategy."
                        )
                        notify_status(
                            logger,
                            "Целевой IP занял последний слот; завершаю работу.",
                        )
                        notify_cycle_stats(logger, cycle_counts)
                        return 0
                    total_slots = len(current_ips) + pending_slots

                    if total_slots >= cfg.account_limit:
                        pause = random.uniform(
                            cfg.single_round_pause_min_s,
                            cfg.single_round_pause_max_s,
                        )
                        logger.info(
                            "Account limit reached. Waiting %.1fs before retry.", pause
                        )
                        notify_pause(
                            logger,
                            "Лимит аккаунта; ожидание свободного слота",
                            pause,
                        )
                        time.sleep(pause)
                        continue

                    before_ips = set(current_ips)

                    cooldown_between_mutations(cfg, logger)

                    result = create_one_ip_moscow(page, cfg, logger)
                    if result.status == "created" and result.ip:
                        ip = result.ip
                    else:
                        if result.status == "pending":
                            logger.info(
                                "Создание в статусе 'Создается'. Ожидание завершения."
                            )
                        else:
                            logger.warning("Create failed/timeout.")

                        wait_status, ip = wait_for_new_ip_single(
                            page,
                            cfg,
                            logger,
                            before_ips,
                        )
                        if wait_status == "fatal":
                            logger.error("Фатальная ошибка. Выход.")
                            return exit_with_error(
                                cfg,
                                logger,
                                2,
                                "Фатальная ошибка при ожидании создания IP (одиночная стратегия).",
                            )
                        if wait_status == "restart" or not ip:
                            logger.warning(
                                "Создание не завершилось после ожидания. Перезапуск цикла."
                            )
                            pause_after_cleanup_s = cfg.single_restart_pause_s
                            pause_reason = (
                                "Создание не завершилось после ожидания; "
                                "перезапуск цикла"
                            )
                            restart_cycle = True
                            break

                    total_created += 1
                    round_created += 1
                    logger.info("Created: %s", ip)
                    update_daily_stats(ip, cfg, logger)
                    update_cycle_stats(ip, cycle_counts)

                    skip_delete = False
                    if target_networks:
                        hit_net = match_target_network(ip, target_networks)
                    else:
                        hit_net = None

                    if hit_net:
                        matched_target_ips.add(ip)
                        matched_target_subnets.add(str(hit_net))
                        logger.info("Target CIDR hit: %s in %s", ip, hit_net)
                        notify_target_hit(
                            logger,
                            ip,
                            hit_net,
                            len(matched_target_ips),
                            len(matched_target_subnets),
                        )
                        skip_delete = True

                    wait_page_ready(page)

                    if skip_delete:
                        current_ips, pending_slots = read_current_state(page)
                        if should_stop_due_to_target_slot(
                            cfg, current_ips, pending_slots, matched_target_ips
                        ):
                            logger.info(
                                "Target IP occupies last slot. Stopping single strategy."
                            )
                            notify_status(
                                logger,
                                "Целевой IP занял последний слот; завершаю работу.",
                            )
                            notify_cycle_stats(logger, cycle_counts)
                            return 0
                    else:
                        cooldown_between_mutations(cfg, logger)

                        delete_result = delete_ip(page, cfg, logger, ip)
                        if delete_result.status != "deleted":
                            if delete_result.status == "pending":
                                logger.info("Удаление в процессе. Ожидание завершения.")
                            else:
                                logger.warning("Delete failed. Ожидание завершения.")

                            wait_status = wait_for_ip_removal_single(
                                page,
                                cfg,
                                logger,
                                ip,
                            )
                            if wait_status == "fatal":
                                logger.error("Фатальная ошибка. Выход.")
                                return exit_with_error(
                                    cfg,
                                    logger,
                                    3,
                                    "Фатальная ошибка при ожидании удаления IP (одиночная стратегия).",
                                )
                            if wait_status == "restart":
                                logger.warning(
                                    "Удаление не завершилось после ожидания. Перезапуск цикла."
                                )
                                pause_after_cleanup_s = cfg.single_restart_pause_s
                                pause_reason = (
                                    "Удаление не завершилось после ожидания; "
                                    "перезапуск цикла"
                                )
                                restart_cycle = True
                                break

                        wait_page_ready(page)

                    if round_created >= round_size and total_created < goal_created:
                        pause = random.uniform(
                            cfg.single_round_pause_min_s,
                            cfg.single_round_pause_max_s,
                        )
                        logger.info("Inter-round pause (single): %.1fs", pause)
                        notify_pause(
                            logger,
                            "Пауза между раундами (одиночная стратегия)",
                            pause,
                        )
                        time.sleep(pause)
                        round_created = 0

                if pause_after_cleanup_s:
                    logger.info(
                        "Пауза на %.1f минут и перезапуск цикла.",
                        pause_after_cleanup_s / 60,
                    )
                    if pause_reason:
                        notify_pause(logger, pause_reason, pause_after_cleanup_s)
                    time.sleep(pause_after_cleanup_s)
                    if restart_cycle:
                        continue

            else:
                total_created = 0
                restart_cycle = False

                while total_created < cfg.goal_total_created:
                    if URL_FLOATING_IPS not in page.url:
                        page.goto(URL_FLOATING_IPS)
                        wait_page_ready(page)

                    round_cap = random.randint(cfg.round_cap_min, cfg.round_cap_max)
                    round_cap = min(round_cap, cfg.account_limit)

                    logger.info(
                        "=== Round start: cap=%d, total=%d/%d ===",
                        round_cap,
                        total_created,
                        cfg.goal_total_created,
                    )

                    round_created = []
                    pause_after_cleanup_s = None
                    pause_reason = None
                    stop_after_cleanup = False

                    while True:
                        current_ips, pending_slots = read_current_state(page)
                        new_ips = [ip for ip in current_ips if ip not in last_ips]
                        if new_ips:
                            for ip in new_ips:
                                total_created += 1
                                logger.info("Detected new IP: %s", ip)
                                if ip not in round_created:
                                    round_created.append(ip)
                                update_daily_stats(ip, cfg, logger)
                                update_cycle_stats(ip, cycle_counts)

                                if target_networks:
                                    hit_net = match_target_network(ip, target_networks)
                                else:
                                    hit_net = None

                                if hit_net:
                                    matched_target_ips.add(ip)
                                    matched_target_subnets.add(str(hit_net))
                                    logger.info("Target CIDR hit: %s in %s", ip, hit_net)
                                    notify_target_hit(
                                        logger,
                                        ip,
                                        hit_net,
                                        len(matched_target_ips),
                                        len(matched_target_subnets),
                                    )

                                    if (
                                        len(matched_target_subnets)
                                        >= cfg.target_goal_distinct_subnets
                                    ):
                                        stop_after_cleanup = True
                                    elif len(matched_target_ips) >= cfg.target_goal_ips:
                                        stop_after_cleanup = True
                                        logger.info(
                                            "Цель по количеству IP достигнута, но подсетей: %d",
                                            len(matched_target_subnets),
                                        )
                                    elif not paused_after_first_target:
                                        pause_after_cleanup_s = cfg.target_pause_s
                                        pause_reason = (
                                            "Получен IP из целевых подсетей; пауза перед продолжением"
                                        )
                                        paused_after_first_target = True

                                    if stop_after_cleanup or pause_after_cleanup_s:
                                        break
                            last_ips = set(current_ips)
                            if stop_after_cleanup or pause_after_cleanup_s:
                                break
                        else:
                            last_ips = set(current_ips)

                        total_slots = len(current_ips) + pending_slots
                        if total_slots >= round_cap:
                            logger.info("Round cap reached.")
                            break
                        if total_slots >= cfg.account_limit:
                            logger.info("Account limit reached.")
                            break
                        if total_created >= cfg.goal_total_created:
                            break

                        cooldown_between_mutations(cfg, logger)

                        result = create_one_ip_moscow(page, cfg, logger)
                        if result.status == "created" and result.ip:
                            ip = result.ip
                            last_ips.add(ip)
                            round_created.append(ip)
                            total_created += 1
                            logger.info("Created: %s", ip)
                            update_daily_stats(ip, cfg, logger)
                            update_cycle_stats(ip, cycle_counts)

                            if target_networks:
                                hit_net = match_target_network(ip, target_networks)
                            else:
                                hit_net = None

                            if hit_net:
                                matched_target_ips.add(ip)
                                matched_target_subnets.add(str(hit_net))
                                logger.info("Target CIDR hit: %s in %s", ip, hit_net)
                                notify_target_hit(
                                    logger,
                                    ip,
                                    hit_net,
                                    len(matched_target_ips),
                                    len(matched_target_subnets),
                                )

                                if (
                                    len(matched_target_subnets)
                                    >= cfg.target_goal_distinct_subnets
                                ):
                                    stop_after_cleanup = True
                                elif len(matched_target_ips) >= cfg.target_goal_ips:
                                    stop_after_cleanup = True
                                    logger.info(
                                        "Цель по количеству IP достигнута, но подсетей: %d",
                                        len(matched_target_subnets),
                                    )
                                elif not paused_after_first_target:
                                    pause_after_cleanup_s = cfg.target_pause_s
                                    pause_reason = (
                                        "Получен IP из целевых подсетей; пауза перед продолжением"
                                    )
                                    paused_after_first_target = True

                                if stop_after_cleanup or pause_after_cleanup_s:
                                    break
                        elif result.status == "pending":
                            logger.info(
                                "Создание в статусе 'Создается'. Слот занят, продолжаю работу."
                            )
                        else:
                            logger.warning("Create failed/timeout.")
                            if has_fatal_error(page, cfg, logger):
                                logger.error("Фатальная ошибка. Выход.")
                                return exit_with_error(
                                    cfg,
                                    logger,
                                    2,
                                    "Фатальная ошибка при создании IP (основная стратегия).",
                                )
                            pause_after_cleanup_s = cfg.failure_pause_s
                            pause_reason = (
                                "Нефатальная ошибка при создании IP (основная стратегия); "
                                "перезапуск цикла"
                            )
                            restart_cycle = True
                            break

                        wait_page_ready(page)

                    if round_created:
                        logger.info("Cleanup: deleting %d IPs...", len(round_created))

                    for ip in reversed(round_created):
                        if ip in base_ips or ip in matched_target_ips:
                            continue

                        cooldown_between_mutations(cfg, logger)

                        delete_result = delete_ip(page, cfg, logger, ip)
                        if delete_result.status == "pending":
                            logger.info(
                                "Удаление в процессе, слот занят. Продолжаю работу."
                            )
                            wait_page_ready(page)
                            continue
                        if delete_result.status != "deleted":
                            logger.warning("Delete failed.")
                            if has_fatal_error(page, cfg, logger):
                                logger.error("Фатальная ошибка. Выход.")
                                return exit_with_error(
                                    cfg,
                                    logger,
                                    3,
                                    "Фатальная ошибка при удалении IP (основная стратегия).",
                                )
                            pause_after_cleanup_s = cfg.failure_pause_s
                            pause_reason = (
                                "Нефатальная ошибка при удалении IP (основная стратегия); "
                                "перезапуск цикла"
                            )
                            restart_cycle = True
                            break

                        wait_page_ready(page)

                    if stop_after_cleanup:
                        logger.info("Достигнута цель по IP. Завершаю работу.")
                        if not cleanup_non_target_ips(
                            page, cfg, logger, target_networks
                        ):
                            if has_fatal_error(page, cfg, logger):
                                logger.error("Фатальная ошибка. Выход.")
                                return exit_with_error(
                                    cfg,
                                    logger,
                                    3,
                                    "Фатальная ошибка при финальной очистке.",
                                )
                            notify_status(
                                logger,
                                "Нефатальная ошибка при финальной очистке; завершаю работу.",
                            )
                        notify_cycle_stats(logger, cycle_counts)
                        return 0

                    if pause_after_cleanup_s:
                        if pause_after_cleanup_s == cfg.target_pause_s:
                            logger.info(
                                "Получен IP из целевых подсетей. Пауза на %.1f часов.",
                                cfg.target_pause_s / 3600,
                            )
                        else:
                            logger.info(
                                "Нефатальная ошибка. Пауза на %.1f минут и перезапуск цикла.",
                                cfg.failure_pause_s / 60,
                            )
                        if pause_reason:
                            notify_pause(logger, pause_reason, pause_after_cleanup_s)
                        time.sleep(pause_after_cleanup_s)
                        if restart_cycle:
                            break

                    if total_created < cfg.goal_total_created:
                        pause = random.uniform(cfg.round_pause_min_s, cfg.round_pause_max_s)
                        logger.info("Inter-round pause: %.1fs", pause)
                        notify_pause(logger, "Пауза между раундами", pause)
                        time.sleep(pause)

                if restart_cycle:
                    continue

            if strategy == "single":
                logger.info("Single strategy: skip cleanup of non-target IPs.")
            else:
                if not cleanup_non_target_ips(page, cfg, logger, target_networks):
                    if has_fatal_error(page, cfg, logger):
                        logger.error("Фатальная ошибка. Выход.")
                        return exit_with_error(
                            cfg,
                            logger,
                            3,
                            "Фатальная ошибка при финальной очистке.",
                        )
                    logger.info(
                        "Нефатальная ошибка. Пауза на %.1f минут и перезапуск цикла.",
                        cfg.failure_pause_s / 60,
                    )
                    notify_pause(
                        logger,
                        "Нефатальная ошибка при финальной очистке; перезапуск цикла",
                        cfg.failure_pause_s,
                    )
                    time.sleep(cfg.failure_pause_s)
                    continue

            notify_cycle_stats(logger, cycle_counts)
            logger.info("Запуск завершен. Начинаю финальную паузу...")
            pause = random.uniform(cfg.final_pause_min_s, cfg.final_pause_max_s)
            logger.info("Final pause before new run: %.1fs", pause)
            notify_pause(logger, "Финальная пауза перед новым циклом", pause)
            time.sleep(pause)

    except KeyboardInterrupt:
        logger.info("User stopped script.")
        return 0
    except Exception as e:
        logger.exception(f"Critical error: {e}")
        notify_error(logger, str(e))
        return 1
    finally:
        browser.close()
        p.stop()
        release_lock(setup_logging())
