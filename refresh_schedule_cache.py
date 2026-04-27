import os
import sys
from typing import Tuple

from bot import load_env_file
from schedule_scraper import ScheduleRepository


def load_settings() -> Tuple[str, int, int]:
    load_env_file()
    cache_dir = os.getenv("SCHEDULE_CACHE_DIR", "data").strip() or "data"
    scraper_timeout = int(os.getenv("SCRAPER_TIMEOUT", "30"))
    scraper_workers = int(os.getenv("SCRAPER_WORKERS", "4"))
    return cache_dir, scraper_timeout, max(1, scraper_workers)


def main() -> int:
    cache_dir, scraper_timeout, scraper_workers = load_settings()
    repository = ScheduleRepository(
        cache_dir=cache_dir,
        timeout=scraper_timeout,
        max_workers=scraper_workers,
    )

    try:
        snapshot = repository.refresh_all_schedules(force_catalog_refresh=True)
    except Exception as exc:
        print(f"Refresh failed: {exc}", file=sys.stderr)
        return 1

    groups_count = len(snapshot.get("groups", []))
    errors = snapshot.get("errors", [])
    refreshed_at = snapshot.get("fetched_at", "—")

    print(f"Refresh completed at {refreshed_at}")
    print(f"Groups refreshed: {groups_count}")
    print(f"Errors: {len(errors)}")
    for error in errors[:10]:
        print(f"- {error}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
