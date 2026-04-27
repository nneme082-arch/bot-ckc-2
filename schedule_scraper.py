import json
import re
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime
from html import unescape
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List, Optional, Tuple


TIMETABLE_BASE_URL = "https://rmk.stavedu.ru:8010/moodle/eioswork/timetable"
INDEX_URL = f"{TIMETABLE_BASE_URL}/index.php"
WATCH_URL = f"{TIMETABLE_BASE_URL}/watchstudent.php"
USER_AGENT = "Mozilla/5.0 (compatible; ScheduleBot/1.0; +https://api.telegram.org)"


def clean_text(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", value)
    text = unescape(text).replace("\xa0", " ")
    return " ".join(text.split())


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


@dataclass
class GroupInfo:
    group_id: int
    name: str
    course: str
    url: str

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "GroupInfo":
        return cls(
            group_id=int(payload["group_id"]),
            name=str(payload["name"]),
            course=str(payload["course"]),
            url=str(payload["url"]),
        )


class ScheduleScraper:
    def __init__(self, timeout: int = 30) -> None:
        self.timeout = timeout

    def fetch_text(self, url: str) -> str:
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP error while loading {url}: {details}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Network error while loading {url}: {exc}") from exc

    def build_index_url(self, year: int, month: int) -> str:
        return f"{INDEX_URL}?year={year}&month={month}"

    def build_group_url(self, group_id: int, year: int, month: int) -> str:
        return f"{WATCH_URL}?year={year}&month={month}&group={group_id}"

    def fetch_groups(self, year: int, month: int) -> List[GroupInfo]:
        html = self.fetch_text(self.build_index_url(year, month))
        groups: List[GroupInfo] = []
        seen_ids = set()
        current_course = ""

        token_pattern = re.finditer(
            r"<h4 class='links-content__title'>(.*?)</h4[^>]*>|"
            r"<a href='([^']*watchstudent\.php[^']*group=(\d+)[^']*)'>(.*?)</a>",
            html,
            re.S,
        )
        for match in token_pattern:
            heading_html = match.group(1)
            if heading_html is not None:
                current_course = clean_text(heading_html)
                continue

            url = match.group(2)
            group_id_text = match.group(3)
            name_html = match.group(4)
            if not url or not group_id_text or name_html is None:
                continue

            group_id = int(group_id_text)
            if group_id in seen_ids:
                continue
            seen_ids.add(group_id)
            groups.append(
                GroupInfo(
                    group_id=group_id,
                    name=clean_text(name_html),
                    course=current_course or "Без курса",
                    url=url,
                )
            )

        if not groups:
            raise RuntimeError("Could not find any groups on the timetable index page.")
        return groups

    def fetch_group_schedule(self, group: GroupInfo, year: int, month: int) -> Dict[str, Any]:
        html = self.fetch_text(self.build_group_url(group.group_id, year, month))
        return self.parse_group_schedule(group=group, html=html, year=year, month=month)

    def parse_group_schedule(
        self,
        group: GroupInfo,
        html: str,
        year: int,
        month: int,
    ) -> Dict[str, Any]:
        title_match = re.search(r"<title>(.*?)</title>", html, re.S)
        page_title = clean_text(title_match.group(1) if title_match else group.name)
        day_blocks = re.findall(
            r"(<table class='daytable' border=1>.*?</table>)(?=<table class='daytable' border=1>|</div>|<style>)",
            html,
            re.S,
        )
        days: List[Dict[str, Any]] = []

        for block in day_blocks:
            header_match = re.search(r"<td class='thead' colspan=3><b>([^<]+)</b></td>", block)
            label = clean_text(header_match.group(1) if header_match else "")
            lessons: List[Dict[str, Any]] = []

            lesson_matches = re.finditer(
                r"<tr>\s*<td class='thead'[^>]*>(\d+)</td>\s*<td class='td-bold'>\s*<table class='rowtable'>\s*(.*?)\s*</table>\s*</td>\s*</tr>",
                block,
                re.S,
            )
            for lesson_match in lesson_matches:
                lesson_number = int(lesson_match.group(1))
                lesson_html = lesson_match.group(2)
                rows = re.findall(
                    r"<tr>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*</tr>",
                    lesson_html,
                    re.S,
                )
                entries: List[Dict[str, Any]] = []
                for info_html, room_html in rows:
                    subject, teacher, note = self.parse_info_cell(info_html)
                    room = clean_text(room_html) or "—"
                    if self.is_empty_entry(subject, teacher, note, room):
                        continue
                    entries.append(
                        {
                            "subject": subject,
                            "teacher": teacher,
                            "note": note,
                            "room": room,
                        }
                    )

                lessons.append(
                    {
                        "number": lesson_number,
                        "entries": entries,
                    }
                )

            days.append(
                {
                    "label": label or "Без названия",
                    "lessons": lessons,
                }
            )

        return {
            "group_id": group.group_id,
            "group_name": group.name,
            "course": group.course,
            "page_title": page_title,
            "source_url": self.build_group_url(group.group_id, year, month),
            "requested_year": year,
            "requested_month": month,
            "fetched_at": now_iso(),
            "days": days,
        }

    def parse_info_cell(self, info_html: str) -> Tuple[str, str, str]:
        raw = clean_text(info_html)
        parts = [part.strip() for part in raw.split("|", 2)]
        while len(parts) < 3:
            parts.append("")
        return parts[0], parts[1], parts[2]

    def is_empty_entry(self, subject: str, teacher: str, note: str, room: str) -> bool:
        blank = {"", "—", "-"}
        return subject in blank and teacher in blank and note in blank and room in blank


class ScheduleRepository:
    def __init__(
        self,
        cache_dir: str = "data",
        timeout: int = 30,
        max_workers: int = 4,
    ) -> None:
        self.cache_dir = Path(cache_dir)
        self.catalog_path = self.cache_dir / "groups_catalog.json"
        self.snapshot_path = self.cache_dir / "schedule_snapshot.json"
        self.scraper = ScheduleScraper(timeout=timeout)
        self.max_workers = max_workers
        self.lock = RLock()

    def current_period(self) -> Tuple[int, int]:
        current = datetime.now().astimezone()
        return current.year, current.month

    def ensure_cache_dir(self) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def load_json(self, path: Path) -> Optional[Dict[str, Any]]:
        with self.lock:
            if not path.exists():
                return None
            return json.loads(path.read_text(encoding="utf-8"))

    def save_json(self, path: Path, payload: Dict[str, Any]) -> None:
        with self.lock:
            self.ensure_cache_dir()
            path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

    def load_catalog(self) -> Optional[List[GroupInfo]]:
        payload = self.load_json(self.catalog_path)
        if not payload:
            return None
        return [GroupInfo.from_dict(item) for item in payload.get("groups", [])]

    def fetch_and_cache_catalog(self) -> List[GroupInfo]:
        with self.lock:
            year, month = self.current_period()
            groups = self.scraper.fetch_groups(year=year, month=month)
            payload = {
                "fetched_at": now_iso(),
                "requested_year": year,
                "requested_month": month,
                "groups": [asdict(group) for group in groups],
            }
            self.save_json(self.catalog_path, payload)
            return groups

    def get_groups(self, force_refresh: bool = False) -> List[GroupInfo]:
        with self.lock:
            if not force_refresh:
                cached = self.load_catalog()
                if cached:
                    return cached
            return self.fetch_and_cache_catalog()

    def load_snapshot(self) -> Optional[Dict[str, Any]]:
        return self.load_json(self.snapshot_path)

    def save_snapshot(self, payload: Dict[str, Any]) -> None:
        with self.lock:
            self.save_json(self.snapshot_path, payload)

    def find_groups(self, query: str) -> List[GroupInfo]:
        with self.lock:
            normalized = query.strip().upper()
            if not normalized:
                return []

            groups = self.get_groups()
            if normalized.isdigit():
                exact_id_matches = [group for group in groups if str(group.group_id) == normalized]
                if exact_id_matches:
                    return exact_id_matches

            exact_matches = [group for group in groups if group.name.upper() == normalized]
            if exact_matches:
                return exact_matches

            return [group for group in groups if normalized in group.name.upper()]

    def get_schedule_for_group(self, query: str, use_cache: bool = True) -> Dict[str, Any]:
        with self.lock:
            matches = self.find_groups(query)
            if not matches:
                raise LookupError(f"Группа '{query}' не найдена.")
            if len(matches) > 1:
                suggestions = ", ".join(group.name for group in matches[:10])
                raise LookupError(
                    f"Найдено несколько групп: {suggestions}. Уточните название."
                )

            group = matches[0]
            if use_cache:
                cached = self.lookup_cached_schedule(group.group_id)
                if cached:
                    return cached

            year, month = self.current_period()
            schedule = self.scraper.fetch_group_schedule(group=group, year=year, month=month)
            self.upsert_cached_schedule(schedule)
            return schedule

    def lookup_cached_schedule(self, group_id: int) -> Optional[Dict[str, Any]]:
        with self.lock:
            snapshot = self.load_snapshot()
            if not snapshot:
                return None
            groups = snapshot.get("groups", [])
            for item in groups:
                if int(item.get("group_id", -1)) == group_id:
                    return item
            return None

    def upsert_cached_schedule(self, schedule: Dict[str, Any]) -> None:
        with self.lock:
            snapshot = self.load_snapshot() or {
                "fetched_at": now_iso(),
                "requested_year": schedule["requested_year"],
                "requested_month": schedule["requested_month"],
                "groups": [],
                "errors": [],
            }

            groups = snapshot.setdefault("groups", [])
            for index, item in enumerate(groups):
                if int(item.get("group_id", -1)) == int(schedule["group_id"]):
                    groups[index] = schedule
                    break
            else:
                groups.append(schedule)

            snapshot["fetched_at"] = now_iso()
            self.save_snapshot(snapshot)

    def refresh_all_schedules(self, force_catalog_refresh: bool = True) -> Dict[str, Any]:
        with self.lock:
            groups = self.get_groups(force_refresh=force_catalog_refresh)
            year, month = self.current_period()
            schedules: List[Dict[str, Any]] = []
            errors: List[str] = []

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_map = {
                    executor.submit(
                        self.scraper.fetch_group_schedule,
                        group,
                        year,
                        month,
                    ): group
                    for group in groups
                }

                for future in as_completed(future_map):
                    group = future_map[future]
                    try:
                        schedules.append(future.result())
                    except Exception as exc:
                        errors.append(f"{group.name}: {exc}")

            schedules.sort(key=lambda item: str(item["group_name"]).upper())
            payload = {
                "fetched_at": now_iso(),
                "requested_year": year,
                "requested_month": month,
                "groups": schedules,
                "errors": errors,
            }
            self.save_snapshot(payload)
            return payload
