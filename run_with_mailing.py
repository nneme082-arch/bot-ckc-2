import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from threading import Event, Thread
from typing import Any, Dict, List, Optional

import bot as base_bot
from schedule_scraper import ScheduleRepository


DAILY_MAIL_HOUR = 6
DAILY_MAIL_MINUTE = 0


class DailyMailingScheduler:
    def __init__(
        self,
        schedule_repository: ScheduleRepository,
        send_updates_callback,
    ) -> None:
        self.schedule_repository = schedule_repository
        self.send_updates_callback = send_updates_callback
        self.state_path = self.schedule_repository.cache_dir / "daily_mailing_state.json"
        self.stop_event = Event()
        self.thread = Thread(
            target=self.run_forever,
            name="daily-mailing-scheduler",
            daemon=True,
        )

    def start(self) -> None:
        if not self.thread.is_alive():
            self.thread.start()

    def next_run_at(self, now: Optional[datetime] = None) -> datetime:
        current = now.astimezone(base_bot.MOSCOW_TZ) if now is not None else datetime.now(base_bot.MOSCOW_TZ)
        next_run = current.replace(
            hour=DAILY_MAIL_HOUR,
            minute=DAILY_MAIL_MINUTE,
            second=0,
            microsecond=0,
        )
        if next_run <= current:
            next_run = next_run + timedelta(days=1)
        return next_run

    def load_state(self) -> Dict[str, str]:
        if not self.state_path.exists():
            return {"last_sent_schedule": ""}
        try:
            state = json.loads(self.state_path.read_text(encoding="utf-8"))
            if not isinstance(state, dict):
                return {"last_sent_schedule": ""}
            return state
        except json.JSONDecodeError:
            return {"last_sent_schedule": ""}

    def save_state(self, payload: Dict[str, str]) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def date_key(self, current: datetime) -> str:
        return current.astimezone(base_bot.MOSCOW_TZ).strftime("%Y-%m-%d")

    def schedule_key(self, current: datetime) -> str:
        return f"{self.date_key(current)} {DAILY_MAIL_HOUR:02d}:{DAILY_MAIL_MINUTE:02d}"

    def should_send_now(self, current: datetime) -> bool:
        state = self.load_state()
        current_schedule_key = self.schedule_key(current)
        last_sent_schedule = str(state.get("last_sent_schedule", "")).strip()
        scheduled_time_reached = (current.hour, current.minute) >= (DAILY_MAIL_HOUR, DAILY_MAIL_MINUTE)
        if not scheduled_time_reached:
            return False

        if last_sent_schedule == current_schedule_key:
            return False

        today_prefix = f"{self.date_key(current)} "
        if last_sent_schedule.startswith(today_prefix):
            return current_schedule_key > last_sent_schedule

        return True

    def run_job(self, current: datetime) -> None:
        snapshot = self.schedule_repository.refresh_all_schedules(force_catalog_refresh=True)
        mailing_result = self.send_updates_callback(snapshot)
        self.save_state({"last_sent_schedule": self.schedule_key(current)})
        print(
            "Daily mailing complete: "
            f"groups={len(snapshot.get('groups', []))}, "
            f"errors={len(snapshot.get('errors', []))}, "
            f"sent={mailing_result.get('sent_count', 0)}/{mailing_result.get('recipient_count', 0)}.",
            flush=True,
        )

    def run_forever(self) -> None:
        while not self.stop_event.is_set():
            now = datetime.now(base_bot.MOSCOW_TZ)
            if self.should_send_now(now):
                try:
                    self.run_job(now)
                except Exception as exc:
                    print(f"Daily mailing failed: {exc}", file=sys.stderr, flush=True)
                if self.stop_event.wait(timeout=60):
                    return
                continue

            next_run = self.next_run_at(now)
            seconds_until_run = max(1, int((next_run - now).total_seconds()))
            print(
                f"Daily refresh and mailing scheduled for {next_run.isoformat(timespec='minutes')} MSK.",
                flush=True,
            )
            if self.stop_event.wait(timeout=seconds_until_run):
                return

            try:
                self.run_job(datetime.now(base_bot.MOSCOW_TZ))
            except Exception as exc:
                print(f"Daily mailing failed: {exc}", file=sys.stderr, flush=True)


class MailingTelegramBot(base_bot.TelegramBot):
    def __init__(self, token: str, schedule_repository: ScheduleRepository) -> None:
        super().__init__(token, schedule_repository)
        self.daily_refresh_scheduler = DailyMailingScheduler(
            self.schedule_repository,
            self.send_daily_schedule_updates,
        )

    def get_mailing_recipients(self) -> List[Dict[str, Any]]:
        recipients: List[Dict[str, Any]] = []
        for chat_id, state in self.user_state_store.load_all().items():
            selected_group = str(state.get("selected_group", "")).strip()
            if not selected_group:
                continue

            try:
                parsed_chat_id = int(chat_id)
            except (TypeError, ValueError):
                continue

            recipients.append(
                {
                    "chat_id": parsed_chat_id,
                    "selected_group": selected_group,
                }
            )
        return recipients

    def build_daily_schedule_message(self, schedule: Dict[str, Any]) -> str:
        return "Обновленное расписание:\n\n" + self.format_schedule(schedule)

    def send_daily_schedule_updates(
        self,
        snapshot: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, int]:
        recipients = self.get_mailing_recipients()
        sent_count = 0
        failed_count = 0

        for recipient in recipients:
            chat_id = recipient["chat_id"]
            group_name = recipient["selected_group"]
            try:
                schedule = self.schedule_repository.get_schedule_for_group(group_name)
                self.send_text_blocks(chat_id, self.build_daily_schedule_message(schedule))
                sent_count += 1
            except Exception as exc:
                failed_count += 1
                print(
                    f"Could not send daily mailing to chat {chat_id} for group {group_name}: {exc}",
                    file=sys.stderr,
                    flush=True,
                )

        return {
            "recipient_count": len(recipients),
            "sent_count": sent_count,
            "failed_count": failed_count,
        }

    def handle_status(self) -> str:
        return super().handle_status().replace("21:20", "06:00")


def main() -> int:
    try:
        config = base_bot.Config.from_env()
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 1

    repository = ScheduleRepository(
        cache_dir=config.cache_dir,
        timeout=config.scraper_timeout,
        max_workers=config.scraper_workers,
    )
    bot = MailingTelegramBot(config.token, repository)
    bot.run(timeout=config.poll_timeout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
