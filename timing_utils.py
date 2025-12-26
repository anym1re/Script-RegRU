import random
import time
import logging
from collections import deque

from config import Config

_mutation_timestamps = deque()


def human_sleep(cfg: Config, kind: str = "action") -> None:
    if kind == "action":
        time.sleep(random.uniform(cfg.action_sleep_min, cfg.action_sleep_max))
    else:
        time.sleep(random.uniform(cfg.poll_sleep_min, cfg.poll_sleep_max))


def throttle_mutation_rpm(cfg: Config, logger: logging.Logger) -> None:
    max_rpm = cfg.mutation_max_rpm
    if max_rpm <= 0:
        return

    now = time.monotonic()
    window_start = now - 60.0
    while _mutation_timestamps and _mutation_timestamps[0] < window_start:
        _mutation_timestamps.popleft()

    if len(_mutation_timestamps) >= max_rpm:
        oldest = _mutation_timestamps[0]
        wait_s = max(0.0, 60.0 - (now - oldest))
        if wait_s > 0:
            jitter = random.uniform(0.3, 1.1)
            sleep_s = wait_s + jitter
            logger.info("RPM cap: waiting %.1fs to stay <= %d rpm", sleep_s, max_rpm)
            time.sleep(sleep_s)

            now = time.monotonic()
            window_start = now - 60.0
            while _mutation_timestamps and _mutation_timestamps[0] < window_start:
                _mutation_timestamps.popleft()

    _mutation_timestamps.append(time.monotonic())


def cooldown_between_mutations(cfg: Config, logger: logging.Logger) -> None:
    throttle_mutation_rpm(cfg, logger)
    s = random.uniform(cfg.mutation_cooldown_min_s, cfg.mutation_cooldown_max_s)
    logger.info("Cooldown before mutation: %.1fs", s)
    time.sleep(s)
