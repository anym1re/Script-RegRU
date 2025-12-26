import re
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

URL_FLOATING_IPS = "https://cloud.reg.ru/panel/floating-ips"
URL_BASE = "https://cloud.reg.ru"
URL_AUTH = "https://cloud.reg.ru/panel/auth"
URL_ORDER_FLOATING_IP = "https://cloud.reg.ru/panel/floating-ips/order-floating-ip"

LOG_FILE = Path("regcloud_floating_ips.log")
LOCK_FILE = Path("regcloud_floating_ips.lock")

IP_REGEX = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")


@dataclass(frozen=True)
class Config:
    region: str = "Москва"

    account_limit: int = 5
    goal_total_created: int = 50

    round_cap_min: int = 2
    round_cap_max: int = 5

    # "human" delays
    action_sleep_min: float = 0.8
    action_sleep_max: float = 2.2
    poll_sleep_min: float = 1.2
    poll_sleep_max: float = 2.8

    mutation_cooldown_min_s: int = 5
    mutation_cooldown_max_s: int = 16

    round_pause_min_s: int = 20
    round_pause_max_s: int = 120

    final_pause_min_s: int = 10 * 60
    final_pause_max_s: int = 45 * 60
    failure_pause_s: int = 30 * 60

    create_result_timeout_s: int = 300
    delete_result_timeout_s: int = 300
    order_page_ready_timeout_s: int = 25
    create_button_timeout_s: int = 8
    create_button_retries: int = 4

    headless: bool = True
    page_load_timeout_ms: int = 30000

    target_cidrs: Tuple[str, ...] = (
        "79.174.91.0/24",
        "79.174.92.0/24",
        "79.174.93.0/24",
        "79.174.94.0/24",
        "79.174.95.0/24",
    )
    target_goal_ips: int = 2
    target_goal_distinct_subnets: int = 2
    target_pause_s: int = 6 * 60 * 60
    stats_file: str = "daily_stats.txt"
    strategy_mode: str = "single"  # auto, main, rare, single
    strategy_auto_probability: float = 0.4
    single_goal_created_min: int = 120
    single_goal_created_max: int = 180
    single_round_size: int = 10
    single_round_pause_min_s: int = 30
    single_round_pause_max_s: int = 180
    single_reload_every_s: int = 5 * 60
    single_max_reload_attempts: int = 3
    single_restart_pause_s: int = 15 * 60
    rare_rotation_slots: int = 1
    rare_keep_max: int = 4
    rare_goal_created_min: int = 60
    rare_goal_created_max: int = 80
    rare_subnet_max_count: int = 1
    rare_subnet_top_n: int = 0
    fatal_error_markers: Tuple[str, ...] = (
        "429",
        "too many requests",
        "слишком много запросов",
        "service unavailable",
        "bad gateway",
        "gateway timeout",
        "internal server error",
        "ошибка сервера",
        "произошла ошибка",
        "что-то пошло не так",
    )
