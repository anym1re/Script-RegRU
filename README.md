# Script-RegRU

Автоматизация управления плавающими IP в панели REG.RU через Playwright.
Скрипт создаёт и удаляет IP, ведёт статистику подсетей, может искать целевые CIDR и отправлять уведомления в Telegram.

## Возможности
- Автоматическое создание/удаление плавающих IP
- Стратегии перебора: основная, редкая, одиночная и auto
- Поиск IP в целевых подсетях (`target_cidrs`)
- Статистика по подсетям в `daily_stats.txt`
- Уведомления в Telegram (опционально)
- Защита от параллельных запусков через lock-файл

## Требования
- Python 3
- Playwright
- Аккаунт в REG.RU с доступом к панели облака

## Установка
```bash
python -m pip install -r requirements.txt
python -m playwright install chromium
```

## Настройка окружения
Скопируйте пример и заполните переменные:
```bash
cp .env.example .env
```

`.env`:
```
REGRU_EMAIL=your_email@example.com
REGRU_PASSWORD=your_password
TELEGRAM_BOT_TOKEN=your_token
TELEGRAM_CHAT_ID=your_chat_id
```

`TELEGRAM_*` необязательны — без них уведомления отключены.

## Запуск
```bash
python regru-wl.py
```

Параметры запуска задаются в `regru-wl.py` (см. `Config(...)`).
Если нужен headless-режим для сервера/Docker, установите `headless=True`.

## Конфигурация
Основные параметры находятся в `config.py` (dataclass `Config`).
Ключевые поля:
- `region` — регион для заказа IP
- `account_limit` — общий лимит IP в аккаунте
- `goal_total_created` — цель по созданным IP в цикле
- `strategy_mode` — `auto` / `main` / `rare` / `single`
- `target_cidrs` — список целевых подсетей
- `target_goal_ips`, `target_goal_distinct_subnets` — критерии остановки по целям
- `headless` — режим запуска браузера

## Логи и служебные файлы
- `regcloud_floating_ips.log` — лог (ротация до 1 МБ, 3 бэкапа)
- `regcloud_floating_ips.lock` — lock-файл с timestamp (если скрипт упал, удалите вручную)
- `daily_stats.txt` — статистика по подсетям
- `login_failed.png` / `login_failed.html` — артефакты при проблемах входа

## Примечания
- Не запускайте несколько копий одновременно — будет блокировка через lock-файл.
- Скрипт автоматизирует веб-интерфейс; при изменении UI может потребоваться адаптация селекторов.
