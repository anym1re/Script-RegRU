import sys

from dotenv import load_dotenv

from config import Config
from runner import run

load_dotenv()


if __name__ == "__main__":
    cfg = Config(
        headless=False, # Ставим False для локальной отладки, в Docker можно True
        account_limit=15,
        goal_total_created=50,
    )
    sys.exit(run(cfg))
