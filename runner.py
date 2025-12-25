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
from ui import list_ips_from_table, wait_page_ready


def choose_strategy(cfg: Config, logger) -> Tuple[str, List[ipaddress.IPv4Network]]:
    mode = cfg.strategy_mode
    if mode not in ("auto", "main", "rare"):
        logger.warning("Unknown strategy_mode=%s; treating as main.", mode)
        return "main", []

    if mode == "main":
        return "main", []

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
    table = format_stats_table(cycle_counts).rstrip("\n")
    send_telegram_message(logger, format_pre(table), parse_mode="HTML")


def notify_error(logger, message: str) -> None:
    send_telegram_message(logger, f"Ошибка: {message}")


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
        if not delete_ip(page, cfg, logger, ip):
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

        while True:
            ensure_logged_in(page, cfg, logger)

            if URL_FLOATING_IPS not in page.url:
                page.goto(URL_FLOATING_IPS)
                wait_page_ready(page)

            base_ips = set(list_ips_from_table(page))
            logger.info("Base IPs (protected): %d", len(base_ips))

            strategy, rare_networks = choose_strategy(cfg, logger)
            logger.info("Strategy for this run: %s", strategy)

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

                    current_ips = list_ips_from_table(page)
                    protected_ips = set(base_ips) | matched_target_ips | matched_rare_ips
                    probe_ips = [ip for ip in current_ips if ip not in protected_ips]

                    if len(current_ips) >= cfg.account_limit:
                        if not probe_ips:
                            logger.info(
                                "Нет свободных слотов для перебора. Завершаю редкую стратегию."
                            )
                            break
                        cooldown_between_mutations(cfg, logger)
                        if not delete_ip(page, cfg, logger, probe_ips[0]):
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
                            restart_cycle = True
                            break
                        wait_page_ready(page)
                        continue

                    if len(probe_ips) >= probe_slots:
                        cooldown_between_mutations(cfg, logger)
                        if not delete_ip(page, cfg, logger, probe_ips[0]):
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
                            restart_cycle = True
                            break
                        wait_page_ready(page)
                        continue

                    cooldown_between_mutations(cfg, logger)

                    ip = create_one_ip_moscow(page, cfg, logger)
                    if ip:
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
                        restart_cycle = True
                        break

                if pause_after_cleanup_s:
                    logger.info(
                        "Нефатальная ошибка. Пауза на %.1f минут и перезапуск цикла.",
                        cfg.failure_pause_s / 60,
                    )
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
                    stop_after_cleanup = False

                    while True:
                        current_ips = list_ips_from_table(page)
                        if len(current_ips) >= round_cap:
                            logger.info("Round cap reached.")
                            break
                        if len(current_ips) >= cfg.account_limit:
                            logger.info("Account limit reached.")
                            break
                        if total_created >= cfg.goal_total_created:
                            break

                        cooldown_between_mutations(cfg, logger)

                        ip = create_one_ip_moscow(page, cfg, logger)
                        if ip:
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
                                    paused_after_first_target = True

                                if stop_after_cleanup or pause_after_cleanup_s:
                                    break
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
                            restart_cycle = True
                            break

                        wait_page_ready(page)

                    if round_created:
                        logger.info("Cleanup: deleting %d IPs...", len(round_created))

                    for ip in reversed(round_created):
                        if ip in base_ips or ip in matched_target_ips:
                            continue

                        cooldown_between_mutations(cfg, logger)

                        if not delete_ip(page, cfg, logger, ip):
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
                        time.sleep(pause_after_cleanup_s)
                        if restart_cycle:
                            break

                    if total_created < cfg.goal_total_created:
                        pause = random.uniform(cfg.round_pause_min_s, cfg.round_pause_max_s)
                        logger.info("Inter-round pause: %.1fs", pause)
                        time.sleep(pause)

                if restart_cycle:
                    continue

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
                time.sleep(cfg.failure_pause_s)
                continue

            notify_cycle_stats(logger, cycle_counts)
            logger.info("Запуск завершен. Начинаю финальную паузу...")
            pause = random.uniform(cfg.final_pause_min_s, cfg.final_pause_max_s)
            logger.info("Final pause before new run: %.1fs", pause)
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
