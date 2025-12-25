import time
import logging
from dataclasses import dataclass
from typing import List, Set

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError

from config import Config, IP_REGEX
from timing_utils import human_sleep


@dataclass(frozen=True)
class RowInfo:
    ip: str
    region: str
    status: str


def wait_page_ready(page: Page) -> None:
    try:
        page.wait_for_selector("//*[contains(., 'Плавающие IP')]", timeout=30000)
    except PlaywrightTimeoutError:
        pass


def wait_for_any_selector(
    page: Page, selectors: List[str], timeout_s: int, logger: logging.Logger
) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        for sel in selectors:
            try:
                if page.locator(sel).count() > 0:
                    return True
            except Exception:
                continue
        time.sleep(0.5)
    logger.warning("Не дождались ожидаемых элементов страницы заказа.")
    return False


def wait_for_order_page_ready(page: Page, cfg: Config, logger: logging.Logger) -> None:
    selectors = [
        "text=Новый плавающий IP",
        "button:has-text('Добавить плавающий IP')",
        "button:has-text('Создать')",
        "//div[contains(@class, 'cv-tile')]",
    ]
    wait_for_any_selector(page, selectors, cfg.order_page_ready_timeout_s, logger)


def click_create_button_with_retries(page: Page, cfg: Config, logger: logging.Logger) -> bool:
    selectors = [
        "button:has-text('Добавить плавающий IP')",
        "button:has-text('Создать')",
    ]
    timeout_ms = int(cfg.create_button_timeout_s * 1000)
    for attempt in range(1, cfg.create_button_retries + 1):
        for sel in selectors:
            loc = page.locator(sel).first
            try:
                if loc.count() == 0:
                    continue
                loc.scroll_into_view_if_needed()
                loc.wait_for(state="visible", timeout=timeout_ms)
                if not loc.is_enabled():
                    logger.info(
                        "Кнопка создания неактивна, ждем (attempt %d/%d).",
                        attempt,
                        cfg.create_button_retries,
                    )
                    break
                loc.click(timeout=timeout_ms)
                return True
            except PlaywrightTimeoutError:
                continue
            except Exception as e:
                logger.info(
                    "Не удалось нажать кнопку создания (attempt %d/%d): %s",
                    attempt,
                    cfg.create_button_retries,
                    e,
                )
                continue
        human_sleep(cfg)
        try:
            page.wait_for_load_state("networkidle", timeout=5000)
        except PlaywrightTimeoutError:
            pass
    return False


def list_rows_from_table(page: Page) -> List[RowInfo]:
    out: List[RowInfo] = []

    locators = [
        "//div[contains(@class, 'fip-table__row')]",
        "//table//tbody//tr",
        "//*[@role='row']",
    ]

    rows = []
    for loc in locators:
        elements = page.locator(loc).all()
        if elements:
            rows = elements
            break

    seen_keys: Set[str] = set()

    for r in rows:
        text = r.inner_text().strip()
        if not text:
            continue

        ip = ""
        m = IP_REGEX.search(text)
        if m:
            ip = m.group(0)

        key = ip or text
        if key in seen_keys:
            continue

        status = ""
        lowered = text.lower()
        if "созда" in lowered:
            status = "Создается"
        elif "актив" in lowered:
            status = "Активен"

        out.append(RowInfo(ip=ip, region="", status=status))
        seen_keys.add(key)

    return out


def list_ips_from_table(page: Page) -> List[str]:
    return [r.ip for r in list_rows_from_table(page) if r.ip]
