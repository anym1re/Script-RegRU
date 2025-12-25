import ipaddress
import logging
import time
from typing import List, Optional, Set

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError

from config import Config, URL_FLOATING_IPS, URL_ORDER_FLOATING_IP
from timing_utils import human_sleep
from ui import (
    click_create_button_with_retries,
    list_ips_from_table,
    wait_for_order_page_ready,
    wait_page_ready,
)


def has_fatal_error(page: Page, cfg: Config, logger: logging.Logger) -> bool:
    try:
        body = page.text_content("body") or ""
    except Exception as e:
        logger.warning("Не удалось прочитать страницу для диагностики: %s", e)
        return False
    lowered = body.lower()
    for marker in cfg.fatal_error_markers:
        if marker.lower() in lowered:
            logger.error("Обнаружен фатальный маркер ошибки: %s", marker)
            return True
    return False


def match_target_network(
    ip: str, networks: List[ipaddress.IPv4Network]
) -> Optional[ipaddress.IPv4Network]:
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return None
    for net in networks:
        if addr in net:
            return net
    return None


def create_one_ip_moscow(page: Page, cfg: Config, logger: logging.Logger) -> Optional[str]:
    if URL_FLOATING_IPS not in page.url:
        page.goto(URL_FLOATING_IPS)
        wait_page_ready(page)

    before_ips: Set[str] = set()
    try:
        before_ips = set(list_ips_from_table(page))
    except Exception as e:
        logger.warning("Не удалось прочитать список IP перед созданием: %s", e)
        try:
            page.reload()
            wait_page_ready(page)
            before_ips = set(list_ips_from_table(page))
        except Exception as e2:
            logger.warning("Повторное чтение списка IP не удалось: %s", e2)

    logger.info("Открытие прямого URL заказа IP...")
    page.goto(URL_ORDER_FLOATING_IP)
    try:
        page.wait_for_load_state("domcontentloaded", timeout=cfg.page_load_timeout_ms)
    except PlaywrightTimeoutError:
        pass
    try:
        page.wait_for_load_state("networkidle", timeout=5000)
    except PlaywrightTimeoutError:
        pass
    human_sleep(cfg)
    wait_for_order_page_ready(page, cfg, logger)

    try:
        page.wait_for_selector("text=Новый плавающий IP", timeout=15000)
    except Exception:
        logger.warning("Заголовок 'Новый плавающий IP' не найден, но пробуем кликать.")

    logger.info(f"Выбор региона: {cfg.region}")
    region_sel = f"div:has-text('{cfg.region}'), button:has-text('{cfg.region}')"
    try:
        page.click(
            f"//div[contains(@class, 'cv-tile')][contains(., '{cfg.region}')]",
            timeout=5000,
        )
    except Exception:
        try:
            page.click(f"text={cfg.region}")
        except Exception:
            logger.info("Не удалось кликнуть по региону (возможно, уже выбран).")

    human_sleep(cfg)

    logger.info("Нажатие 'Добавить плавающий IP'...")
    if not click_create_button_with_retries(page, cfg, logger):
        logger.error("Кнопка создания не найдена или неактивна после ретраев!")
        return None

    logger.info("Запрос отправлен. Ожидание результата...")

    try:
        page.wait_for_url(lambda u: "order" not in u, timeout=10000)
    except Exception:
        pass

    if "order" in page.url:
        page.goto(URL_FLOATING_IPS)
        wait_page_ready(page)

    human_sleep(cfg, kind="poll")

    deadline = time.time() + cfg.create_result_timeout_s

    while time.time() < deadline:
        if URL_FLOATING_IPS not in page.url:
            page.goto(URL_FLOATING_IPS)
            wait_page_ready(page)

        try:
            current_ips = set(list_ips_from_table(page))
        except Exception as e:
            logger.warning("Ошибка чтения таблицы IP, пробуем перезагрузку: %s", e)
            page.reload()
            wait_page_ready(page)
            human_sleep(cfg, kind="poll")
            continue

        new_diff = current_ips - before_ips

        if new_diff:
            new_ip = list(new_diff)[0]
            logger.info(f"Успех! Новый IP: {new_ip}")
            return new_ip

        if page.get_by_text("Создается").count() > 0:
            if int(time.time()) % 10 == 0:
                logger.info("Вижу статус 'Создается'...")

        human_sleep(cfg, kind="poll")

    logger.warning(
        "Таймаут ожидания создания (%ss). Делаем перезагрузку...",
        cfg.create_result_timeout_s,
    )
    page.reload()
    wait_page_ready(page)
    try:
        current_ips = set(list_ips_from_table(page))
    except Exception as e:
        logger.warning("Не удалось перечитать список после перезагрузки: %s", e)
        return None

    new_diff = current_ips - before_ips
    if new_diff:
        new_ip = list(new_diff)[0]
        logger.info(f"Успех после перезагрузки! Новый IP: {new_ip}")
        return new_ip

    return None


def delete_ip(page: Page, cfg: Config, logger: logging.Logger, ip: str) -> bool:
    logger.info(f"Удаление IP: {ip}")

    if URL_FLOATING_IPS not in page.url:
        page.goto(URL_FLOATING_IPS)
        wait_page_ready(page)

    try:
        ip_el = page.get_by_text(ip).first
        if not ip_el.is_visible():
            logger.warning("IP не найден на странице.")
            return False

        row = page.locator("div.fip-table__row").filter(has=ip_el).first
        if not row.count():
            row = page.locator("tr").filter(has=ip_el).first

        if not row.count():
            logger.warning("Не удалось определить строку таблицы для IP.")
            return False

        menu_btn = row.locator("button").last
        menu_btn.click()

        human_sleep(cfg)

        page.get_by_text("Удалить IP").click()
        human_sleep(cfg)

        confirm_btn = page.locator(
            "button:has-text('Удалить IP-адрес'), button:has-text('Удалить')"
        ).last
        confirm_btn.wait_for(state="visible", timeout=5000)
        confirm_btn.click()

        logger.info("Подтверждено. Ждем удаления...")

        try:
            row.wait_for(state="detached", timeout=cfg.delete_result_timeout_s * 1000)
            logger.info("IP удален (строка исчезла).")
            return True
        except PlaywrightTimeoutError:
            logger.warning(
                "Таймаут ожидания удаления (%ss). Делаем перезагрузку...",
                cfg.delete_result_timeout_s,
            )

        page.reload()
        wait_page_ready(page)
        try:
            current = list_ips_from_table(page)
        except Exception as e:
            logger.warning("Не удалось перечитать список после перезагрузки: %s", e)
            return False

        if ip not in current:
            logger.info("IP удален (после перезагрузки).")
            return True

        return False

    except Exception as e:
        logger.error(f"Ошибка удаления: {e}")
        return False
