import ipaddress
import time
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set

from config import Config


def stats_file_path(cfg: Config) -> Path:
    return Path(cfg.stats_file)


def parse_stats_table(text: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("|"):
            continue
        parts = [p.strip() for p in line.strip("|").split("|")]
        if len(parts) < 2:
            continue
        if parts[0].lower() == "subnet" or parts[1].lower() == "total_count":
            continue
        try:
            counts[parts[0]] = int(parts[1])
        except ValueError:
            continue
    return counts


def parse_stats_sections(text: str, default_date: str) -> Dict[str, Dict[str, int]]:
    sections: Dict[str, Dict[str, int]] = {}
    current_date: Optional[str] = None
    buffer: List[str] = []
    has_header = False

    for line in text.splitlines():
        if line.startswith("# "):
            if current_date and buffer:
                sections[current_date] = parse_stats_table("\n".join(buffer))
            current_date = line[2:].strip()
            buffer = []
            has_header = True
            continue
        if current_date:
            buffer.append(line)

    if current_date and buffer:
        sections[current_date] = parse_stats_table("\n".join(buffer))

    if not has_header:
        counts = parse_stats_table(text)
        if counts:
            sections[default_date] = counts

    return sections


def format_stats_table(counts: Dict[str, int]) -> str:
    header_subnet = "subnet"
    header_count = "total_count"
    rows = sorted(counts.items(), key=lambda x: (-x[1], x[0]))
    subnet_width = max([len(header_subnet)] + [len(k) for k, _ in rows])
    count_width = max([len(header_count)] + [len(str(v)) for _, v in rows])
    line = f"+{'-' * subnet_width}+{'-' * count_width}+"
    out = [
        line,
        f"|{header_subnet.ljust(subnet_width)}|{header_count.ljust(count_width)}|",
        line,
    ]
    for subnet, total in rows:
        out.append(f"|{subnet.ljust(subnet_width)}|{str(total).ljust(count_width)}|")
    out.append(line)
    return "\n".join(out) + "\n"


def format_stats_sections(sections: Dict[str, Dict[str, int]]) -> str:
    out: List[str] = []
    for date_str in sorted(sections.keys()):
        out.append(f"# {date_str}")
        out.append(format_stats_table(sections[date_str]).rstrip("\n"))
        out.append("")
    if out:
        out.pop()
    return "\n".join(out) + "\n"


def update_daily_stats(ip: str, cfg: Config, logger: logging.Logger) -> None:
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return
    if addr.version != 4:
        return
    subnet = str(ipaddress.ip_network(f"{ip}/24", strict=False))
    date_str = time.strftime("%Y-%m-%d")
    path = stats_file_path(cfg)
    sections: Dict[str, Dict[str, int]] = {}
    if path.exists():
        try:
            sections = parse_stats_sections(path.read_text(encoding="utf-8"), date_str)
        except Exception as e:
            logger.warning("Не удалось прочитать статистику %s: %s", path, e)
    counts = sections.get(date_str, {})
    counts[subnet] = counts.get(subnet, 0) + 1
    sections[date_str] = counts
    try:
        path.write_text(format_stats_sections(sections), encoding="utf-8")
    except Exception as e:
        logger.warning("Не удалось записать статистику %s: %s", path, e)


def aggregate_stats_sections(sections: Dict[str, Dict[str, int]]) -> Dict[str, int]:
    totals: Dict[str, int] = {}
    for counts in sections.values():
        for subnet, count in counts.items():
            totals[subnet] = totals.get(subnet, 0) + count
    return totals


def get_known_subnets(cfg: Config, logger: logging.Logger) -> Set[str]:
    path = stats_file_path(cfg)
    if not path.exists():
        return set()
    try:
        sections = parse_stats_sections(
            path.read_text(encoding="utf-8"), time.strftime("%Y-%m-%d")
        )
    except Exception as e:
        logger.warning("Не удалось прочитать статистику %s: %s", path, e)
        return set()

    totals = aggregate_stats_sections(sections)
    return set(totals.keys())


def select_rare_subnets(cfg: Config, logger: logging.Logger) -> List[ipaddress.IPv4Network]:
    path = stats_file_path(cfg)
    if not path.exists():
        return []
    try:
        sections = parse_stats_sections(
            path.read_text(encoding="utf-8"), time.strftime("%Y-%m-%d")
        )
    except Exception as e:
        logger.warning("Не удалось прочитать статистику %s: %s", path, e)
        return []

    totals = aggregate_stats_sections(sections)
    if not totals:
        return []

    if cfg.rare_subnet_top_n > 0:
        items = sorted(totals.items(), key=lambda x: (x[1], x[0]))
        candidates = [subnet for subnet, _ in items[: cfg.rare_subnet_top_n]]
    else:
        candidates = [
            subnet for subnet, count in totals.items() if count <= cfg.rare_subnet_max_count
        ]

    networks: List[ipaddress.IPv4Network] = []
    for subnet in candidates:
        try:
            networks.append(ipaddress.ip_network(subnet))
        except ValueError:
            logger.warning("Некорректный subnet в статистике: %s", subnet)

    return networks
