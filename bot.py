import json
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event, Thread
from typing import Any, Callable, Dict, List, Optional

from schedule_scraper import ScheduleRepository


API_ROOT = "https://api.telegram.org"
MESSAGE_LIMIT = 3800
MOSCOW_TZ = timezone(timedelta(hours=3), name="MSK")
SCHEDULE_BUTTON_TEXT = "Расписание"
TODAY_BUTTON_TEXT = "Сегодня"
TOMORROW_BUTTON_TEXT = "Завтра"
WEEK_BUTTON_TEXT = "Вся неделя"
MY_GROUP_BUTTON_TEXT = "Моя группа"
SET_GROUP_BUTTON_TEXT = "Выбрать группу"
CHANGE_GROUP_BUTTON_TEXT = "Сменить группу"
GROUPS_BUTTON_TEXT = "Список групп"
BOT_COMMANDS = [
    {"command": "schedule", "description": "Показать расписание"},
    {"command": "setgroup", "description": "Выбрать группу"},
    {"command": "mygroup", "description": "Моя группа"},
    {"command": "groups", "description": "Все группы"},
    {"command": "find", "description": "Поиск группы"},
    {"command": "refresh", "description": "Обновить расписание"},
    {"command": "status", "description": "Состояние кеша"},
    {"command": "help", "description": "Помощь"},
]


DAILY_MAIL_HOUR = 22
DAILY_MAIL_MINUTE = 20


def load_env_file(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


@dataclass
class Config:
    token: str
    poll_timeout: int = 30
    cache_dir: str = "data"
    scraper_timeout: int = 30
    scraper_workers: int = 4

    @classmethod
    def from_env(cls) -> "Config":
        load_env_file()
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        if not token:
            raise ValueError(
                "Переменная окружения TELEGRAM_BOT_TOKEN не задана. "
                "Создайте файл .env по примеру .env.example."
            )

        is_railway = any(key.startswith("RAILWAY_") for key in os.environ)
        default_cache_dir = "/app/data" if is_railway else "data"
        cache_dir = os.getenv("SCHEDULE_CACHE_DIR", default_cache_dir).strip() or default_cache_dir
        scraper_timeout = int(os.getenv("SCRAPER_TIMEOUT", "30"))
        scraper_workers = int(os.getenv("SCRAPER_WORKERS", "4"))
        poll_timeout = int(os.getenv("BOT_POLL_TIMEOUT", "30"))
        return cls(
            token=token,
            poll_timeout=poll_timeout,
            cache_dir=cache_dir,
            scraper_timeout=scraper_timeout,
            scraper_workers=max(1, scraper_workers),
        )


class UserStateStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load_all(self) -> Dict[str, Dict[str, Any]]:
        if not self.path.exists():
            return {}
        return json.loads(self.path.read_text(encoding="utf-8"))

    def save_all(self, payload: Dict[str, Dict[str, Any]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def get(self, chat_id: int) -> Dict[str, Any]:
        state = self.load_all().get(str(chat_id), {})
        return {
            "selected_group": str(state.get("selected_group", "")).strip(),
            "awaiting_group": bool(state.get("awaiting_group", False)),
        }

    def update(self, chat_id: int, **changes: Any) -> Dict[str, Any]:
        payload = self.load_all()
        key = str(chat_id)
        current = payload.get(key, {})
        current.update(changes)
        payload[key] = current
        self.save_all(payload)
        return self.get(chat_id)


class DailyRefreshScheduler:
    def __init__(self, schedule_repository: ScheduleRepository) -> None:
        self.schedule_repository = schedule_repository
        self.stop_event = Event()
        self.thread = Thread(
            target=self.run_forever,
            name="daily-refresh-scheduler",
            daemon=True,
        )

    def start(self) -> None:
        if not self.thread.is_alive():
            self.thread.start()

    def next_run_at(self, now: Optional[datetime] = None) -> datetime:
        current = now.astimezone(MOSCOW_TZ) if now is not None else datetime.now(MOSCOW_TZ)
        next_run = current.replace(hour=21, minute=20, second=0, microsecond=0)
        if next_run <= current:
            next_run = next_run + timedelta(days=1)
        return next_run

    def run_forever(self) -> None:
        while not self.stop_event.is_set():
            now = datetime.now(MOSCOW_TZ)
            next_run = self.next_run_at(now)
            seconds_until_run = max(1, int((next_run - now).total_seconds()))
            print(
                f"Автообновление расписания запланировано на {next_run.isoformat(timespec='minutes')} по МСК.",
                flush=True,
            )
            if self.stop_event.wait(timeout=seconds_until_run):
                return

            try:
                snapshot = self.schedule_repository.refresh_all_schedules(force_catalog_refresh=True)
                print(
                    "Автообновление завершено: "
                    f"{len(snapshot.get('groups', []))} групп, "
                    f"ошибок {len(snapshot.get('errors', []))}.",
                    flush=True,
                )
            except Exception as exc:
                print(f"Ошибка автообновления расписания: {exc}", file=sys.stderr, flush=True)


class TelegramBot:
    def __init__(self, token: str, schedule_repository: ScheduleRepository) -> None:
        self.base_url = f"{API_ROOT}/bot{token}"
        self.me: Optional[Dict[str, Any]] = None
        self.schedule_repository = schedule_repository
        self.user_state_store = UserStateStore(self.schedule_repository.cache_dir / "user_states.json")
        self.daily_refresh_scheduler = DailyRefreshScheduler(self.schedule_repository)

    def api_call(self, method: str, payload: Optional[Dict[str, Any]] = None) -> Any:
        data: Optional[bytes] = None
        headers: Dict[str, str] = {}
        request_method = "GET"

        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
            request_method = "POST"

        request = urllib.request.Request(
            url=f"{self.base_url}/{method}",
            data=data,
            headers=headers,
            method=request_method,
        )

        try:
            with urllib.request.urlopen(request, timeout=70) as response:
                body = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Telegram API HTTP error for {method}: {details}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Network error while calling {method}: {exc}") from exc

        parsed = json.loads(body)
        if not parsed.get("ok"):
            description = parsed.get("description", "Unknown Telegram API error")
            raise RuntimeError(f"Telegram API error for {method}: {description}")

        return parsed["result"]

    def get_me(self) -> Dict[str, Any]:
        result = self.api_call("getMe")
        if not isinstance(result, dict):
            raise RuntimeError("Unexpected getMe response")
        self.me = result
        return result

    def get_updates(self, offset: Optional[int], timeout: int) -> List[Dict[str, Any]]:
        payload: Dict[str, Any] = {
            "timeout": timeout,
            "allowed_updates": ["message"],
        }
        if offset is not None:
            payload["offset"] = offset

        result = self.api_call("getUpdates", payload)
        if not isinstance(result, list):
            raise RuntimeError("Unexpected getUpdates response")
        return [item for item in result if isinstance(item, dict)]

    def send_chat_action(self, chat_id: int, action: str = "typing") -> None:
        self.api_call(
            "sendChatAction",
            {
                "chat_id": chat_id,
                "action": action,
            },
        )

    def sync_commands(self) -> None:
        self.api_call("setMyCommands", {"commands": BOT_COMMANDS})

    def build_keyboard(self, chat_id: int) -> Dict[str, Any]:
        state = self.user_state_store.get(chat_id)
        if state["selected_group"]:
            keyboard = [
                [{"text": SCHEDULE_BUTTON_TEXT}],
                [{"text": MY_GROUP_BUTTON_TEXT}, {"text": CHANGE_GROUP_BUTTON_TEXT}],
                [{"text": GROUPS_BUTTON_TEXT}],
            ]
            placeholder = f"Текущая группа: {state['selected_group']}"
        else:
            keyboard = [
                [{"text": SCHEDULE_BUTTON_TEXT}],
                [{"text": SET_GROUP_BUTTON_TEXT}, {"text": GROUPS_BUTTON_TEXT}],
            ]
            placeholder = "Напишите группу, например П-21"

        return {
            "keyboard": keyboard,
            "resize_keyboard": True,
            "is_persistent": True,
            "input_field_placeholder": placeholder,
        }

    def send_message(
        self,
        chat_id: int,
        text: str,
        include_keyboard: bool = True,
        reply_markup: Optional[Dict[str, Any]] = None,
    ) -> None:
        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
        }
        if include_keyboard:
            payload["reply_markup"] = reply_markup or self.build_keyboard(chat_id)
        self.api_call("sendMessage", payload)

    def send_text_blocks(self, chat_id: int, text: str) -> None:
        blocks = self.split_text(text)
        for index, block in enumerate(blocks):
            self.send_message(chat_id, block, include_keyboard=index == 0)

    def split_text(self, text: str) -> List[str]:
        if len(text) <= MESSAGE_LIMIT:
            return [text]

        chunks: List[str] = []
        current_lines: List[str] = []
        current_size = 0

        for line in text.splitlines():
            line_size = len(line) + 1
            if current_lines and current_size + line_size > MESSAGE_LIMIT:
                chunks.append("\n".join(current_lines))
                current_lines = [line]
                current_size = line_size
                continue

            current_lines.append(line)
            current_size += line_size

        if current_lines:
            chunks.append("\n".join(current_lines))
        return chunks

    def get_selected_group(self, chat_id: int) -> str:
        return self.user_state_store.get(chat_id)["selected_group"]

    def set_selected_group(self, chat_id: int, group_name: str) -> None:
        self.user_state_store.update(
            chat_id,
            selected_group=group_name,
            awaiting_group=False,
        )

    def set_awaiting_group(self, chat_id: int, value: bool) -> None:
        self.user_state_store.update(chat_id, awaiting_group=value)

    def resolve_single_group(self, query: str) -> Any:
        matches = self.schedule_repository.find_groups(query)
        if not matches:
            raise LookupError(f"Группа '{query}' не найдена.")
        if len(matches) > 1:
            suggestions = ", ".join(group.name for group in matches[:12])
            raise LookupError(
                f"Найдено несколько групп: {suggestions}. Напишите точное название."
            )
        return matches[0]

    def select_group(self, chat_id: int, query: str) -> str:
        group = self.resolve_single_group(query)
        self.set_selected_group(chat_id, group.name)
        schedule = self.schedule_repository.get_schedule_for_group(group.name)
        intro = (
            f"Группа {group.name} сохранена.\n"
            "Ниже показываю полное расписание."
        )
        return f"{intro}\n\n{self.format_schedule(schedule)}"

    def get_saved_schedule(self, chat_id: int) -> Dict[str, Any]:
        selected_group = self.get_selected_group(chat_id)
        if not selected_group:
            self.set_awaiting_group(chat_id, True)
            raise LookupError(
                "Группа еще не выбрана. Нажмите Выбрать группу и отправьте, например, П-21."
            )
        return self.schedule_repository.get_schedule_for_group(selected_group)

    def handle_message(self, message: Dict[str, Any]) -> None:
        chat = message.get("chat")
        if not isinstance(chat, dict):
            return

        chat_id = chat.get("id")
        if not isinstance(chat_id, int):
            return

        text = message.get("text")
        if not isinstance(text, str):
            self.send_message(chat_id, "Пока я умею работать только с текстовыми сообщениями.")
            return

        normalized = text.strip()
        command = normalized.split()[0].split("@", 1)[0].lower() if normalized.startswith("/") else ""

        try:
            if command in {"/refresh", "/update"}:
                self.send_message(chat_id, "Начинаю обновление расписания для всех групп. Это может занять некоторое время.")
                self.send_chat_action(chat_id)
                reply = self.handle_refresh()
            elif command in {"/groups", "/find", "/schedule", "/status"} or not command:
                self.send_chat_action(chat_id)
                reply = self.dispatch_text(chat_id, normalized, chat, message)
            else:
                reply = self.dispatch_text(chat_id, normalized, chat, message)
        except LookupError as exc:
            reply = str(exc)
        except Exception as exc:
            reply = f"Не удалось выполнить запрос: {exc}"

        self.send_text_blocks(chat_id, reply)

    def dispatch_text(
        self,
        chat_id: int,
        text: str,
        chat: Dict[str, Any],
        message: Dict[str, Any],
    ) -> str:
        normalized = text.strip()
        command = normalized.split()[0].split("@", 1)[0].lower() if normalized.startswith("/") else ""
        state = self.user_state_store.get(chat_id)

        if command == "/start":
            first_name = ""
            from_user = message.get("from")
            if isinstance(from_user, dict):
                candidate = from_user.get("first_name")
                if isinstance(candidate, str):
                    first_name = candidate.strip()

            name_part = f", {first_name}" if first_name else ""
            if state["selected_group"]:
                return (
                    f"Привет{name_part}! Текущая группа: {state['selected_group']}.\n\n"
                    "Кнопки внизу:\n"
                    "Расписание\n"
                    "Моя группа\n"
                    "Сменить группу"
                )
            self.set_awaiting_group(chat_id, True)
            return (
                f"Привет{name_part}! Я показываю расписание групп RMK.\n\n"
                "Сначала выберите группу. Просто отправьте ее название, например: П-21"
            )

        if command == "/help":
            return (
                "Основные команды:\n"
                "/schedule - показать все расписание выбранной группы\n"
                "/setgroup П-21 - выбрать группу\n"
                "/mygroup - показать текущую группу\n"
                "/find П-2 - поиск по группам\n"
                "/groups - список всех групп\n"
                "/refresh - обновить весь кеш\n"
                "/status - состояние кеша\n\n"
                "Можно и без команд: кнопками внизу или просто сообщением с названием группы."
            )

        if command == "/id":
            chat_title = chat.get("title")
            title_part = f" ({chat_title})" if isinstance(chat_title, str) and chat_title else ""
            return f"ID текущего чата: {chat['id']}{title_part}"

        if command in {"/today", "/tomorrow"}:
            return self.handle_saved_group_schedule(chat_id)

        if command == "/mygroup":
            return self.handle_my_group(chat_id)

        if command == "/setgroup":
            query = self.extract_argument(normalized)
            if not query:
                self.set_awaiting_group(chat_id, True)
                return "Напишите название группы, например П-21."
            return self.select_group(chat_id, query)

        if command == "/groups":
            return self.handle_groups()

        if command == "/find":
            query = self.extract_argument(normalized)
            if not query:
                return "Укажите часть названия группы. Пример: /find П-2"
            return self.handle_find(query)

        if command == "/schedule":
            query = self.extract_argument(normalized)
            if query:
                return self.handle_schedule(query, chat_id=chat_id, save_selection=True)
            return self.handle_saved_group_schedule(chat_id)

        if command == "/status":
            return self.handle_status()

        if command in {"/refresh", "/update"}:
            return self.handle_refresh()

        if command:
            return "Неизвестная команда. Используйте /help."

        if not normalized:
            return "Напишите название группы или используйте /help."

        if normalized.casefold() == SCHEDULE_BUTTON_TEXT.casefold():
            if state["selected_group"]:
                return self.handle_saved_group_schedule(chat_id)
            self.set_awaiting_group(chat_id, True)
            return "Напишите название группы, например П-21."

        if normalized.casefold() in {
            TODAY_BUTTON_TEXT.casefold(),
            TOMORROW_BUTTON_TEXT.casefold(),
            WEEK_BUTTON_TEXT.casefold(),
        }:
            return self.handle_saved_group_schedule(chat_id)

        if normalized.casefold() == MY_GROUP_BUTTON_TEXT.casefold():
            return self.handle_my_group(chat_id)

        if normalized.casefold() == GROUPS_BUTTON_TEXT.casefold():
            return self.handle_groups()

        if normalized.casefold() in {
            SET_GROUP_BUTTON_TEXT.casefold(),
            CHANGE_GROUP_BUTTON_TEXT.casefold(),
        }:
            self.set_awaiting_group(chat_id, True)
            return "Напишите название группы, например П-21."

        if state["awaiting_group"]:
            return self.select_group(chat_id, normalized)

        return self.handle_schedule(normalized, chat_id=chat_id, save_selection=True)

    def extract_argument(self, text: str) -> str:
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            return ""
        return parts[1].strip()

    def handle_groups(self) -> str:
        groups = self.schedule_repository.get_groups()
        grouped: Dict[str, List[str]] = {}
        for group in groups:
            grouped.setdefault(group.course, []).append(group.name)

        lines = [f"Групп найдено: {len(groups)}", ""]
        for course in sorted(grouped.keys()):
            names = ", ".join(sorted(grouped[course]))
            lines.append(f"{course}:")
            lines.append(names)
            lines.append("")

        return "\n".join(lines).strip()

    def handle_find(self, query: str) -> str:
        matches = self.schedule_repository.find_groups(query)
        if not matches:
            return f"По запросу '{query}' ничего не найдено."

        lines = [f"Найдено групп: {len(matches)}", ""]
        for group in matches[:50]:
            lines.append(f"{group.name} ({group.course}, id {group.group_id})")

        if len(matches) > 50:
            lines.append("")
            lines.append("Показаны первые 50 совпадений.")

        return "\n".join(lines)

    def handle_my_group(self, chat_id: int) -> str:
        selected_group = self.get_selected_group(chat_id)
        if not selected_group:
            self.set_awaiting_group(chat_id, True)
            return "Группа еще не выбрана. Напишите ее название, например П-21."
        return f"Текущая группа: {selected_group}"

    def handle_schedule(
        self,
        query: str,
        chat_id: Optional[int] = None,
        save_selection: bool = False,
    ) -> str:
        schedule = self.schedule_repository.get_schedule_for_group(query)
        if chat_id is not None and save_selection:
            self.set_selected_group(chat_id, str(schedule.get("group_name", query)))
        return self.format_schedule(schedule)

    def handle_saved_group_schedule(self, chat_id: int) -> str:
        schedule = self.get_saved_schedule(chat_id)
        return self.format_schedule(schedule)

    def handle_refresh(self) -> str:
        snapshot = self.schedule_repository.refresh_all_schedules(force_catalog_refresh=True)
        groups_count = len(snapshot.get("groups", []))
        errors = snapshot.get("errors", [])
        lines = [
            "Обновление завершено.",
            f"Групп в кеше: {groups_count}",
            f"Ошибок: {len(errors)}",
            f"Время обновления: {snapshot.get('fetched_at', '—')}",
        ]
        if errors:
            lines.append("")
            lines.append("Первые ошибки:")
            lines.extend(errors[:10])
        return "\n".join(lines)

    def handle_status(self) -> str:
        catalog = self.schedule_repository.load_json(self.schedule_repository.catalog_path)
        snapshot = self.schedule_repository.load_snapshot()

        lines = []
        if catalog:
            lines.append(
                f"Каталог групп: {len(catalog.get('groups', []))} | обновлен {catalog.get('fetched_at', '—')}"
            )
        else:
            lines.append("Каталог групп: еще не загружен")

        if snapshot:
            lines.append(
                f"Кеш расписания: {len(snapshot.get('groups', []))} | обновлен {snapshot.get('fetched_at', '—')}"
            )
            if snapshot.get("errors"):
                lines.append(f"Ошибок последнего обновления: {len(snapshot.get('errors', []))}")
        else:
            lines.append("Кеш расписания: еще не создан")

        lines.append("")
        lines.append("Автообновление кеша: каждый день в 21:20 по МСК.")
        lines.append("")
        lines.append(
            "Важно: страница watchstudent.php сейчас отдает актуальные ближайшие дни и, похоже, не использует year/month как архив месяца."
        )
        return "\n".join(lines)

    def format_schedule(self, schedule: Dict[str, Any]) -> str:
        return self.format_schedule_filtered(schedule, selected_labels=None, header_override=None)

    def format_schedule_filtered(
        self,
        schedule: Dict[str, Any],
        selected_labels: Optional[set] = None,
        header_override: Optional[str] = None,
    ) -> str:
        lines = [
            header_override
            or f"{schedule.get('group_name', 'Группа')} ({schedule.get('course', 'курс не указан')})",
            f"Обновлено: {schedule.get('fetched_at', '—')}",
            "",
        ]

        days = schedule.get("days", [])
        if selected_labels is not None:
            days = [day for day in days if str(day.get("label", "")) in selected_labels]
        if not days:
            lines.append("На странице группы не найдено расписание.")
            return "\n".join(lines)

        for day in days:
            label = str(day.get("label", "День"))
            lines.append(label)

            lessons = day.get("lessons", [])
            visible_lessons = [lesson for lesson in lessons if lesson.get("entries")]
            if not visible_lessons:
                lines.append("Занятий нет.")
                lines.append("")
                continue

            for lesson in visible_lessons:
                lesson_number = lesson.get("number", "?")
                entries = lesson.get("entries", [])
                for index, entry in enumerate(entries):
                    prefix = f"{lesson_number}." if index == 0 else "  "
                    note = str(entry.get("note", "")).strip()
                    note_part = f" [{note}]" if note and note not in {"—", "-"} else ""
                    teacher = str(entry.get("teacher", "")).strip()
                    teacher_part = f" | {teacher}" if teacher else ""
                    room = str(entry.get("room", "")).strip()
                    room_part = f" | {room}" if room else ""
                    lines.append(
                        f"{prefix} {entry.get('subject', '—')}{note_part}{teacher_part}{room_part}"
                    )

            lines.append("")

        return "\n".join(lines).strip()

    def wait_until_ready(self) -> Dict[str, Any]:
        while True:
            try:
                me = self.get_me()
                self.sync_commands()
                return me
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                print(f"Ошибка запуска бота: {exc}", file=sys.stderr, flush=True)
                time.sleep(5)

    def run(self, timeout: int) -> None:
        me = self.wait_until_ready()
        self.daily_refresh_scheduler.start()
        username = me.get("username", "unknown")
        print(f"Бот @{username} запущен. Нажмите Ctrl+C для остановки.")

        offset: Optional[int] = None
        while True:
            try:
                updates = self.get_updates(offset=offset, timeout=timeout)
                for update in updates:
                    update_id = update.get("update_id")
                    if isinstance(update_id, int):
                        offset = update_id + 1

                    message = update.get("message")
                    if isinstance(message, dict):
                        self.handle_message(message)
            except KeyboardInterrupt:
                print("\nБот остановлен.")
                break
            except Exception as exc:
                print(f"Error: {exc}", file=sys.stderr)
                time.sleep(3)


def main() -> int:
    try:
        config = Config.from_env()
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 1

    repository = ScheduleRepository(
        cache_dir=config.cache_dir,
        timeout=config.scraper_timeout,
        max_workers=config.scraper_workers,
    )
    bot = TelegramBot(config.token, repository)
    bot.run(timeout=config.poll_timeout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
