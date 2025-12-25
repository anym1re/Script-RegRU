import random
import time
import logging

from config import Config


def human_sleep(cfg: Config, kind: str = "action") -> None:
    if kind == "action":
        time.sleep(random.uniform(cfg.action_sleep_min, cfg.action_sleep_max))
    else:
        time.sleep(random.uniform(cfg.poll_sleep_min, cfg.poll_sleep_max))


def cooldown_between_mutations(cfg: Config, logger: logging.Logger) -> None:
    s = random.uniform(cfg.mutation_cooldown_min_s, cfg.mutation_cooldown_max_s)
    logger.info("Cooldown before mutation: %.1fs", s)
    time.sleep(s)
