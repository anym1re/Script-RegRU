import logging
import sys
import time
from logging.handlers import RotatingFileHandler

from config import LOCK_FILE, LOG_FILE


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("regcloud_floating_ips")
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)

    fh = RotatingFileHandler(LOG_FILE, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
    fh.setFormatter(fmt)

    logger.handlers.clear()
    logger.addHandler(sh)
    logger.addHandler(fh)
    return logger


def acquire_lock(logger: logging.Logger) -> None:
    if LOCK_FILE.exists():
        raise SystemExit(f"Lock exists: {LOCK_FILE} (возможно, уже запущено).")
    LOCK_FILE.write_text(str(int(time.time())), encoding="utf-8")
    logger.info("Lock acquired: %s", LOCK_FILE)


def release_lock(logger: logging.Logger) -> None:
    try:
        LOCK_FILE.unlink(missing_ok=True)
        logger.info("Lock released: %s", LOCK_FILE)
    except Exception:
        pass
