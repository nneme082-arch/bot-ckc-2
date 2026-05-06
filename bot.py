#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import sys
import time
import urllib.error
import urllib.request
import re
from dataclasses import dataclass
from datetime import datetime, date, timezone, timedelta
from pathlib import Path
from threading import Event, Thread
from typing import Any, Dict, List, Optional, Tuple

from schedule_scraper import ScheduleRepository

# ----------------------------------------------------------------------
# Конфигурация (тот же Config, что уже был)
# ----------------------------------------------------------------------
from config import ADMIN_IDS, BOT_POLL_TIMEOUT, SCRAPER_TIMEOUT, SCRAPER_WORKERS, SCHEDULE_CACHE_DIR, SCHEDULE_CACHE_DIR as DEFAULT_CACHE_DIR

# ----------------------------------------------------------------------
# КОНСТАНТЫ И УТИЛИТЫ
# ----------------------------------------------------------------------
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

# ----------------------------------------------------------------------
# Помощники
# ----------------------------------------------------------------------
def load_env_file(path: str = ".env") -> None:
    """Подгружает переменные из .env (не обязателен, но удобен)."""
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
        # Если бот запущен в Railway – используем /app/data, иначе – обычный каталог
        is_railway = any(k.startswith("RAILWAY_") for k in os.environ)
        default_cache = "/app/data" if is_railway else "data"
        cache_dir = os.getenv("SCHEDULE_CACHE_DIR", default_cache).strip() or default_cache
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


def is_admin(user_id: int) -> bool:
    """Проверка, является ли пользователь администратором."""
    return user_id in ADMIN_IDS


# ----------------------------------------------------------------------
# Дневные планировщики (как и в оригинальном коде)
# ----------------------------------------------------------------------
class DailyRefreshScheduler:
    """Запускает обновление кеша каждый день в 21:20 МСК."""

    def  __init__ ( self, schedule_repository: ScheduleRepository ) -> None :
        self.schedule_repository = schedule_repository
        self.stop_event = Event ( )​
        self.thread = Thread (​
            target=self.run_forever,
            name="daily-refresh-scheduler",
            daemon=True,
        )

    def start(self) -> None:
        if not self.thread.is_alive():
            self.thread.start()

    @staticmethod
    def _next_run_at(now: Optional[datetime] = None) -> datetime:
        """Вычисляем время следующего запуска (21:20 МСК)."""
        cur = now.astimezone(MOSCOW_TZ) if now else datetime.now(MOSCOW_TZ)
        nxt = cur.replace(hour=21, minute=20, second=0, microsecond=0)
        if nxt <= cur:
            nxt += timedelta(days=1)
        return nxt

    def run_forever(self) -> None:
        while not self.stop_event.is_set():
            now = datetime.now(MOSCOW_TZ)
            next_run = self._next_run_at(now)
            seconds = max(1, int((next_run - now).total_seconds()))
            print(
                f"Автообновление расписания запланировано на {next_run.isoformat(timespec='minutes')} МСК.",
                flush=True,
            )
            if self.stop_event.wait(timeout=seconds):
                return
            try:
                snapshot = self.schedule_repository.refresh_all_schedules(force_catalog_refresh=True)
                print(
                    "Автообновление завершено:",
                    f"{len(snapshot.get('groups', []))} групп,",
                    f"{len(snapshot.get('errors', []))} ошибок.",
                    flush=True,
                )
            except Exception as exc:
                print(f"Ошибка автообновления: {exc}", file=sys.stderr, flush=True)


# ----------------------------------------------------------------------
# Основной бот‑класс
# ----------------------------------------------------------------------
class TelegramBot:
    """Класс‑обёртка над Telegram‑API, содержит всё бизнес‑логика."""

    def __init__(self, token: str, schedule_repository: ScheduleRepository) -> None:
        self.base_url = f"{API_ROOT}/bot{token}"
        self.me: Optional[Dict[str, Any]] = None
        self.schedule_repository = schedule_repository

        # ----- Хранилище пользовательских состояний (группы) -----
        self.user_state_store = UserStateStore(
            self.schedule_repository.cache_dir / "user_states.json"
        )

        # ----- Субъекты: авто‑обновление и админ‑данные -----
        self.daily_refresh_scheduler = DailyRefreshScheduler(self.schedule_repository)

        # ----- Файл с предметами (admin‑часть) -----
        self.subjects_path = Path(self.schedule_repository.cache_dir) / "subjects.json"
        self._load_subjects()

    # ------------------------------------------------------------------
    # Вспомогательные методы для API Telegram
    # ------------------------------------------------------------------
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
            with urllib.request.urlopen(request, timeout=70) as resp:
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Telegram API HTTP error for {method}: {details}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Network error while calling {method}: {exc}") from exc

        parsed = json.loads(body)
        if not parsed.get("ok"):
            raise RuntimeError(f"Telegram API error for {method}: {parsed.get('description')}")
        return parsed["result"]

    def get_me(self) -> Dict[str, Any]:
        result = self.api_call("getMe")
        if not isinstance(result, dict):
            raise RuntimeError("Unexpected getMe response")
        self.me = result
        return result

    def get_updates(self, offset: Optional[int], timeout: int) -> List[Dict[str, Any]]:
        payload = {"timeout": timeout, "allowed_updates": ["message"]}
        if offset is not None:
            payload["offset"] = offset
        result = self.api_call("getUpdates", payload)
        if not isinstance(result, list):
            raise RuntimeError("Unexpected getUpdates response")
        return [u for u in result if isinstance(u, dict)]

    def send_chat_action(self, chat_id: int, action: str = "typing") -> None:
        self.api_call("sendChatAction", {"chat_id": chat_id, "action": action})

    def send_message(
        self,
        chat_id: int,
        text: str,
        include_keyboard: bool = True,
        reply_markup: Optional[Dict[str, Any]] = None,
    ) -> None:
        payload: Dict[str, Any] = {"chat_id": chat_id, "text": text}
        if include_keyboard:
            payload["reply_markup"] = reply_markup or self.build_keyboard(chat_id)
        self.api_call("sendMessage", payload)

    def send_text_blocks(self, chat_id: int, text: str) -> None:
        """Разбивает очень большой текст на блоки < MESSAGE_LIMIT."""
        blocks = self._split_text(text)
        for i, block in enumerate(blocks):
            self.send_message(chat_id, block, include_keyboard=i == 0)

    @staticmethod
    def _split_text(text: str) -> List[str]:
        if len(text) <= MESSAGE_LIMIT:
            return [text]
        parts: List[str] = []
        cur: List[str] = []
        cur_len = 0
        for line in text.splitlines():
            line_len = len(line) + 1
            if cur and cur_len + line_len > MESSAGE_LIMIT:
                parts.append("\n".join(cur))
                cur = [line]
                cur_len = line_len
            else:
                cur.append(line)
                cur_len += line_len
        if cur:
            parts.append("\n".join(cur))
        return parts

    # ------------------------------------------------------------------
    # Хранилище пользовательского состояния (выбранная группа)
    # ------------------------------------------------------------------
    def get_selected_group(self, chat_id: int) -> str:
        return self.user_state_store.get(chat_id)["selected_group"]

    def set_selected_group(self, chat_id: int, group_name: str) -> None:
        self.user_state_store.update(chat_id, selected_group=group_name, awaiting_group=False)

    def set_awaiting_group(self, chat_id: int, value: bool) -> None:
        self.user_state_store.update(chat_id, awaiting_group=value)

    # ------------------------------------------------------------------
    # Клавиатура под сообщение
    # ------------------------------------------------------------------
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
            placeholder = "Введите название группы, например П‑21"
        return {
            "keyboard": keyboard,
            "resize_keyboard": True,
            "is_persistent": True,
            "input_field_placeholder": placeholder,
        }

    # ------------------------------------------------------------------
    #   *****   ADMIN PART   *****
    # ------------------------------------------------------------------
    def _load_subjects(self) -> None:
        """Загружает словарь subjects из JSON‑файла (code → total_hours)."""
        if self.subjects_path.is_file():
            try:
                self.subjects: Dict[str, int] = json.loads(
                    self.subjects_path.read_text(encoding="utf-8")
                )
            except Exception:
                self.subjects = {}
        else:
            self.subjects = {}

    def _save_subjects(self) -> None:
        """Сохраняет текущий словарь subjects в файл."""
        self.subjects_path.parent.mkdir(parents=True, exist_ok=True)
        self.subjects_path.write_text(
            json.dumps(self.subjects, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    # ---------- admin commands ----------
    def handle_add_subject(self, chat_id: int, args: List[str]) -> str:
        """/addsubject CODE HOURS – добавить/обновить предмет."""
        if not is_admin(chat_id):
            return "🔒 Доступ только у администраторов."
        if len(args) != 2:
            return "❌ Формат: /addsubject CODE HOURS (пример: /addsubject EH.01 60)"
        code = args[0].upper()
        try:
            hours = int(args[1])
        except ValueError:
            return "❌ Число часов должно быть целым."
        self.subjects[code] = hours
        self._save_subjects()
        return f"✅ Предмет {code} установлен: {hours} пар."

    def handle_list_subjects(self) -> str:
        if not self.subjects:
            return "📭 Нет сохранённых предметов. Администратор может добавить их через /addsubject."
        lines = ["📋 **Список предметов (запланировано пар)**\n"]
        for code, hours in sorted(self.subjects.items()):
            lines.append(f"{code} — {hours} пар")
        return "\n".join(lines)

    def handle_reset_subjects(self) -> str:
        self.subjects.clear()
        self._save_subjects()
        return "✅ Все предметы удалены."

    # ---------- статистика ----------
    @staticmethod
    def _parse_day_label(label: str, year: int, month: int) -> Optional[date]:
        """
        Пробует превратить строку вида «01.05», «1/05/2026», «01‑05‑2026» и т.п.
        в объект datetime.date.
        """
        # Ищем три числа (день, месяц [, год])
        m = re.search(r"(\d{1,2})[.\-/]\s*(\d{1,2})(?:[.\-/]\s*(\d{4}))?", label)
        if not m:
            return None
        day = int(m.group(1))
        month_candidate = int(m.group(2))
        year_candidate = int(m.group(3)) if m.group(3) else year

        # иногда в метке месяц уже указан правильно, иногда – нет.
        # Пытаемся сначала взять найденный месяц, если он валиден.
        try:
            return date(year_candidate, month_candidate, day)
        except ValueError:
            pass

        # Если не удалось – используем месяц, переданный в запросе.
        try:
            return date(year, month, day)
        except ValueError:
            return None

    def _count_passed_lessons(self) -> Dict[str, int]:
        """
        Считает количество уже прошедших пар (по текущей дате) по каждому
        предмету. В основе – кэш‑snapshot (schedule_snapshot.json).
        """
        now = datetime.now(MOSCOW_TZ).date()
        snapshot = self.schedule_repository.load_snapshot()
        if not snapshot:
            return {}

        passed: Dict[str, int] = {}
        for group in snapshot.get("groups", []):
            year = group.get("requested_year")
            month = group.get("requested_month")
            for day_block in group.get("days", []):
                label = day_block.get("label", "")
                day_date = self._parse_day_label(label, year, month)
                if not day_date or day_date > now:
                    continue
                for lesson in day_block.get("lessons", []):
                    for entry in lesson.get("entries", []):
                        sub = entry.get("subject", "").strip()
                        if not sub:
                            continue
                        passed[sub] = passed.get(sub, 0) + 1
        return passed

    def handle_stats(self) -> str:
        """Команда /stats – выводит прогресс по каждому предмету."""
        if not self.subjects:
            return (
                "📭 Нет заданных предметов. Администратор может добавить их через "
                "/addsubject."
            )
        passed = self._count_passed_lessons()
        lines = ["📊 **Статистика по предметам**\n"]
        for code, total in sorted(self.subjects.items()):
            done = passed.get(code, 0)
            left = max(0, total - done)
            lines.append(f"📘 **{code}**")
            lines.append(f"   Всего: {total}")
            lines.append(f"   Прошло: {done}")
            lines.append(f"   Осталось: {left}\n")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    #   *****   БОТ‑ЛОГИКА (обработчики сообщений)   *****
    # ------------------------------------------------------------------
    def resolve_single_group(self, query: str) -> Any:
        matches = self.schedule_repository.find_groups(query)
        if not matches:
            raise LookupError(f"Группа «{query}» не найдена.")
        if len(matches) > 1:
            suggestions = ", ".join(g.name for g in matches[:12])
            raise LookupError(
                f"Найдено несколько групп: {suggestions}. Уточните название."
            )
        return matches[0]

    def select_group(self, chat_id: int, query: str) -> str:
        group = self.resolve_single_group(query)
        self.set_selected_group(chat_id, group.name)
        schedule = self.schedule_repository.get_schedule_for_group(group.name)
        intro = f"Группа {group.name} сохранена.\nНиже показываю полное расписание."
        return f"{intro}\n\n{self.format_schedule(schedule)}"

    def get_saved_schedule(self, chat_id: int) -> Dict[str, Any]:
        selected = self.get_selected_group(chat_id)
        if not selected:
            self.set_awaiting_group(chat_id, True)
            raise LookupError(
                "Группа ещё не выбрана. Нажмите «Выбрать группу» и отправьте её название."
            )
        return self.schedule_repository.get_schedule_for_group(selected)

    # --------------------------------------------------------------
    # Обработка входящего сообщения (только текст)
    # --------------------------------------------------------------
    def handle_message(self, message: Dict[str, Any]) -> None:
        chat = message.get("chat")
        if not isinstance(chat, dict):
            return
        chat_id = chat.get("id")
        if not isinstance(chat_id, int):
            return

        text = message.get("text")
        if not isinstance(text, str):
            self.send_message(
                chat_id, "Пока я умею работать только с простыми текстовыми сообщениями."
            )
            return

        # normalise command /text → /command
        normalized = text.strip()
        command = normalized.split()[0].split("@", 1)[0].lower() if normalized.startswith("/") else ""

        try:
            # --------------------- ADMIN commands --------------------
            if command == "/addsubject":
                reply = self.handle_add_subject(chat_id, normalized.split()[1:])
            elif command == "/listsubjects":
                reply = self.handle_list_subjects()
            elif command == "/resetsubjects":
                reply = self.handle_reset_subjects()
            elif command == "/stats":
                reply = self.handle_stats()
            # --------------------- Основные команды --------------------
            elif command == "/start":
                reply = self.cmd_start(message)
            elif command == "/help":
                reply = self.cmd_help()
            elif command in {"/today", "/tomorrow"}:
                reply = self.handle_saved_group_schedule(chat_id)
            elif command == "/mygroup":
                reply = self.handle_my_group(chat_id)
            elif command == "/setgroup":
                args = self.extract_argument(normalized)
                if not args:
                    self.set_awaiting_group(chat_id, True)
                    reply = "Введите название группы, например П‑21."
                else:
                    reply = self.select_group(chat_id, args)
            elif command == "/groups":
                reply = self.handle_groups()
            elif command == "/find":
                args = self.extract_argument(normalized)
                if not args:
                    reply = "Укажите часть названия группы. Пример: /find П‑2"
                else:
                    reply = self.handle_find(args)
            elif command == "/schedule":
                args = self.extract_argument(normalized)
                if args:
                    reply = self.handle_schedule(args, chat_id=chat_id, save_selection=True)
                else:
                    reply = self.handle_saved_group_schedule(chat_id)
            elif command == "/refresh":
                reply = self.handle_refresh()
            elif command == "/status":
                reply = self.handle_status()
            # --------------------- Кнопки (внутри текста) ----------------
            elif normalized == SCHEDULE_BUTTON_TEXT:
                if self.user_state_store.get(chat_id)["selected_group"]:
                    reply = self.handle_saved_group_schedule(chat_id)
                else:
                    self.set_awaiting_group(chat_id, True)
                    reply = "Введите название группы, например П‑21."
            elif normalized in {TODAY_BUTTON_TEXT, TOMORROW_BUTTON_TEXT, WEEK_BUTTON_TEXT}:
                reply = self.handle_saved_group_schedule(chat_id)
            elif normalized == MY_GROUP_BUTTON_TEXT:
                reply = self.handle_my_group(chat_id)
            elif normalized == GROUPS_BUTTON_TEXT:
                reply = self.handle_groups()
            elif normalized in {SET_GROUP_BUTTON_TEXT, CHANGE_GROUP_BUTTON_TEXT}:
                self.set_awaiting_group(chat_id, True)
                reply = "Введите название группы, например П‑21."
            elif self.user_state_store.get(chat_id)["awaiting_group"]:
                reply = self.select_group(chat_id, normalized)
            else:
                # Если пользователь просто пишет название группы (или любой текст)
                reply = self.handle_schedule(
                    normalized, chat_id=chat_id, save_selection=True
                )
        except LookupError as exc:
            reply = str(exc)
        except Exception as exc:
            reply = f"❌ Не удалось выполнить запрос: {exc}"

        self.send_text_blocks(chat_id, reply)

    # ------------------------------------------------------------------
    # 111% реализации команд (перенесены из оригинального кода)
    # ------------------------------------------------------------------
    def extract_argument(self, text: str) -> str:
        parts = text.split(maxsplit=1)
        return parts[1].strip() if len(parts) > 1 else ""

    def cmd_start(self, message: Dict[str, Any]) -> str:
        from_user = message.get("from", {})
        first_name = from_user.get("first_name", "")
        name_part = f", {first_name}" if first_name else ""
        if self.user_state_store.get(message["chat"]["id"])["selected_group"]:
            return f"Привет{name_part}! Текущая группа: {self.get_selected_group(message['chat']['id'])}."
        self.set_awaiting_group(message["chat"]["id"], True)
        return (
            f"Привет{name_part}! Я показываю расписание групп RMK.\n"
            "Сначала выберите группу – просто отправьте её название, например: П‑21"
        )

    def cmd_help(self) -> str:
        return """
📖 **Помощь**

/schedule — показать всё расписание выбранной группы
/setgroup П‑21 — выбрать группу
/mygroup — показать текущую группу
/groups — список всех групп
/find П‑2 — поиск по частичному названию
/refresh — обновить весь кеш
/status — состояние кеша
/stats — статистика по предметам (только для админа)
/addsubject CODE HOURS — добавить/изменить предмет (только для админа)
/listsubjects — список предметов (только для админа)
/resetsubjects — удалить все предметы (только для админа)
/help — эта справка
"""

    # --------------------------------------------------------------
    # НИЖЕ – почти без изменений (ас‑исходный код из вашего репозитория)
    # --------------------------------------------------------------
    def handle_groups(self) -> str:
        groups = self.schedule_repository.get_groups()
        grouped: Dict[str, List[str]] = {}
        for group in groups:
            grouped.setdefault(group.course, []).append(group.name)

        lines = [f"Групп найдено: {len(groups)}", ""]
        for course in sorted(grouped):
            lines.append(f"{course}:")
            lines.append(", ".join(sorted(grouped[course])))
            lines.append("")
        return "\n".join(lines).strip()

    def handle_find(self, query: str) -> str:
        matches = self.schedule_repository.find_groups(query)
        if not matches:
            return f"По запросу «{query}» ничего не найдено."
        lines = [f"Найдено групп: {len(matches)}", ""]
        for g in matches[:50]:
            lines.append(f"{g.name} ({g.course}, id {g.group_id})")
        if len(matches) > 50:
            lines.append("\nПоказаны первые 50 результатов.")
        return "\n".join(lines)

    def handle_my_group(self, chat_id: int) -> str:
        sel = self.get_selected_group(chat_id)
        if not sel:
            self.set_awaiting_group(chat_id, True)
            return "Группа ещё не выбрана. Нажмите «Выбрать группу» и введите её название."
        return f"Текущая группа: {sel}"

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
        groups_cnt = len(snapshot.get("groups", []))
        errors = snapshot.get("errors", [])
        lines = [
            "✅ Обновление завершено.",
            f"Групп в кеше: {groups_cnt}",
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
                f"Каталог групп: {len(catalog.get('groups', []))} | обновлён {catalog.get('fetched_at', '—')}"
            )
        else:
            lines.append("Каталог групп: ещё не загружен")
        if snapshot:
            lines.append(
                f"Кеш расписания: {len(snapshot.get('groups', []))} | обновлён {snapshot.get('fetched_at', '—')}"
            )
            if snapshot.get("errors"):
                lines.append(f"Ошибок последнего обновления: {len(snapshot['errors'])}")
        else:
            lines.append("Кеш расписания: ещё не создан")
        lines.extend(
            [
                "",
                "Автообновление кеша: каждый день в 21:20 по МСК.",
                "",
                "Важно: страница watchstudent.php сейчас отдает только ближайшие дни.",
            ]
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
    days = [d for d in days if str(d.get("label", "")) in selected_labels]

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


