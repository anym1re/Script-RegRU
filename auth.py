import os
import time
import logging

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError

from config import Config, URL_AUTH, URL_FLOATING_IPS
from timing_utils import human_sleep


def ensure_logged_in(page: Page, cfg: Config, logger: logging.Logger) -> None:
    logger.info("Проверка сессии...")

    # 1. Go to auth page; already logged in sessions should redirect.
    try:
        page.goto(URL_AUTH, timeout=cfg.page_load_timeout_ms)
        page.wait_for_load_state("domcontentloaded")
    except Exception as e:
        logger.warning(f"Ошибка перехода на {URL_AUTH}: {e}")

    human_sleep(cfg)

    # 2. Detect login page.
    is_login_page = False
    try:
        if page.locator(".qa-auth-form-field-login, input[name='login'], input[name='username']").count() > 0:
            is_login_page = True
        elif "auth" in page.url or "login" in page.url:
            is_login_page = True
    except Exception:
        pass

    if is_login_page:
        logger.info(f"Обнаружена страница входа. URL: {page.url}")

        email = os.getenv("REGRU_EMAIL")
        password = os.getenv("REGRU_PASSWORD")

        if not email or not password:
            logger.warning("!!! REGRU_EMAIL или REGRU_PASSWORD не заданы в .env !!!")
            logger.warning("Пожалуйста, войдите вручную в браузере.")
            try:
                page.wait_for_url(lambda u: "panel" in u, timeout=300000)
            except Exception:
                pass
            return

        logger.info("Ввод учетных данных...")
        try:
            page.fill(
                ".qa-auth-form-field-login, input[name='login'], input[name='username'], input[type='email']",
                email,
            )
            human_sleep(cfg)

            page.fill(
                ".qa-auth-form-field-pass, input[name='password'], input[type='password']",
                password,
            )
            human_sleep(cfg)

            btn = page.locator(
                ".qa-auth-form-login-btn, button[type='submit'], button:has-text('Войти')"
            ).first
            if btn.count() > 0:
                btn.click()
            else:
                page.keyboard.press("Enter")

            logger.info("Форма отправлена, ждем входа...")
            logger.info("Ждем редиректа в панель...")
            try:
                def log_url(u):
                    logger.info(f"Navigated to: {u}")
                    return "/panel" in u and "auth" not in u

                page.wait_for_url(log_url, timeout=60000)
                logger.info(f"Успешный вход. Текущий URL: {page.url}")
            except PlaywrightTimeoutError:
                logger.error(f"Таймаут входа. Текущий URL: {page.url}")
                page.screenshot(path="login_failed.png")
                with open("login_failed.html", "w", encoding="utf-8") as f:
                    f.write(page.content())
                if not cfg.headless:
                    logger.info("Ждем 30 сек на ручное исправление...")
                    time.sleep(30)

        except Exception as e:
            logger.error(f"Ошибка в процессе входа: {e}")
            raise
    else:
        logger.info("Похоже, мы уже авторизованы (нет полей входа).")

    # 3. Final check by opening target page.
    page.goto(URL_FLOATING_IPS)
    try:
        page.wait_for_selector(
            "//div[contains(@class, 'fip-table__row')] | //*[contains(., 'Плавающие IP')] | //button[contains(., 'Добавить')]",
            timeout=20000,
        )
        logger.info("Сессия подтверждена, мы в разделе IP.")
    except PlaywrightTimeoutError:
        logger.error("Не удалось загрузить раздел IP после входа. Возможно, вход не выполнен.")
        page.screenshot(path="session_check_failed.png")
        raise SystemExit("Fatal: Authentication failed or UI changed")
