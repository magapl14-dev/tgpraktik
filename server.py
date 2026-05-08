"""
Сервер «Мои практики» — единый процесс:
  • FastAPI    — REST API + раздача статики (фронт + админка)
  • Aiogram    — Telegram-бот: /start, кнопки, напоминания
  • APScheduler— фоновый планировщик напоминаний
  • SQLite     — общая база (один файл, без сетевой БД)

ЗАПУСК ЛОКАЛЬНО (для теста):
  1. pip install -r requirements.txt
  2. cp .env.example .env  →  заполнить BOT_TOKEN, ADMIN_IDS, BASE_URL
  3. python server.py
  4. Открыть туннель: ngrok http 8000  →  скопировать https-URL
  5. У @BotFather: /setdomain → ввести URL без https://
  6. У @BotFather: /newapp → бот → URL = <туннель>/app  → короткое имя 'tracker'

ДЕПЛОЙ:
  • Render.com Web Service: build `pip install -r requirements.txt`, start `python server.py`
  • Переменные окружения: BOT_TOKEN, ADMIN_IDS (через запятую), BASE_URL, PORT (Render даст сам)
  • БД (data.db) лежит рядом — для бесплатного тарифа Render это норм; на серьёзный объём — Postgres
"""
import os
import json
import hmac
import hashlib
import sqlite3
import asyncio
import logging
import secrets
from urllib.parse import parse_qsl, unquote
from datetime import datetime, date, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from contextlib import contextmanager

from fastapi import FastAPI, Header, HTTPException, Request, UploadFile, File, Form
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, Literal

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, InlineKeyboardButton, InlineKeyboardMarkup,
    WebAppInfo, MenuButtonWebApp, BotCommand, CallbackQuery,
)
from aiogram.filters import Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ─── НАСТРОЙКИ ─────────────────────────────────────────────────────────────
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
BASE_URL = os.environ.get("BASE_URL", "http://localhost:8000").rstrip("/")
ADMIN_IDS = {int(x) for x in os.environ.get("ADMIN_IDS", "").replace(" ", "").split(",") if x}
PORT = int(os.environ.get("PORT", "8000"))
DB_PATH = Path(os.environ.get("DB_PATH", "data.db"))
TZ = ZoneInfo(os.environ.get("TZ", "Europe/Moscow"))
PHOTOS_DIR = Path(os.environ.get("PHOTOS_DIR", "photos"))
DEFAULT_TZ_NAME = os.environ.get("TZ", "Europe/Moscow")

if not BOT_TOKEN:
    print("⚠️  BOT_TOKEN не задан — бот работать не будет, но API запустится")
if not ADMIN_IDS:
    print("⚠️  ADMIN_IDS не заданы — некому управлять практиками. Добавь свой Telegram ID в .env")

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", level=logging.INFO)
log = logging.getLogger("practices")


# ─── БАЗА ДАННЫХ ───────────────────────────────────────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
  user_id      INTEGER PRIMARY KEY,
  username     TEXT,
  first_name   TEXT,
  language     TEXT,
  created_at   TEXT NOT NULL,
  last_seen    TEXT,
  tz           TEXT,                   -- IANA-имя зоны, NULL = серверная по умолчанию
  mute_until   INTEGER DEFAULT 0       -- timestamp; пока now < mute_until, напоминания не шлём
);

CREATE TABLE IF NOT EXISTS practices (
  id            TEXT PRIMARY KEY,
  name          TEXT NOT NULL,
  description   TEXT,
  type          TEXT NOT NULL,         -- 'binary' | 'count'
  target        INTEGER,
  unit          TEXT,
  icon          TEXT,
  palette       TEXT,
  media_url     TEXT,
  media_label   TEXT,
  photo         TEXT,                  -- /photos/<id>.jpg или (легаси) data:...base64
  max_reminders INTEGER DEFAULT 3,     -- сколько раз в день напоминать
  reminder_from TEXT DEFAULT '08:00',  -- окно напоминаний начало
  reminder_to   TEXT DEFAULT '21:00',  -- окно напоминаний конец
  active        INTEGER DEFAULT 1,
  created_at    TEXT NOT NULL,
  created_by    INTEGER
);

CREATE TABLE IF NOT EXISTS user_practices (
  user_id              INTEGER NOT NULL,
  practice_id          TEXT NOT NULL,
  period_type          TEXT NOT NULL,         -- 'week' | 'month' | 'forever'
  period_start         TEXT NOT NULL,         -- YYYY-MM-DD
  period_end           TEXT,                  -- YYYY-MM-DD or NULL for forever
  joined_at            TEXT NOT NULL,
  period_end_notified  INTEGER DEFAULT 0,     -- 1 = уже отправили предупреждение о конце
  PRIMARY KEY (user_id, practice_id),
  FOREIGN KEY (practice_id) REFERENCES practices(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS entries (
  user_id     INTEGER NOT NULL,
  practice_id TEXT NOT NULL,
  date        TEXT NOT NULL,           -- YYYY-MM-DD
  completed   INTEGER DEFAULT 0,
  count       INTEGER DEFAULT 0,
  ts          INTEGER NOT NULL,
  PRIMARY KEY (user_id, practice_id, date)
);

CREATE TABLE IF NOT EXISTS reminders_sent (
  user_id     INTEGER NOT NULL,
  practice_id TEXT NOT NULL,
  date        TEXT NOT NULL,
  count       INTEGER DEFAULT 0,
  last_at     INTEGER,
  PRIMARY KEY (user_id, practice_id, date)
);

CREATE TABLE IF NOT EXISTS categories (
  id          TEXT PRIMARY KEY,
  name        TEXT NOT NULL,
  parent_id   TEXT,
  level       INTEGER NOT NULL,         -- 1..4, считается сервером
  icon        TEXT,
  sort_order  INTEGER DEFAULT 0,
  created_at  TEXT NOT NULL,
  FOREIGN KEY (parent_id) REFERENCES categories(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS practice_categories (
  practice_id  TEXT NOT NULL,
  category_id  TEXT NOT NULL,
  PRIMARY KEY (practice_id, category_id),
  FOREIGN KEY (practice_id) REFERENCES practices(id) ON DELETE CASCADE,
  FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS app_meta (
  key    TEXT PRIMARY KEY,
  value  TEXT
);

CREATE INDEX IF NOT EXISTS idx_entries_date ON entries(date);
CREATE INDEX IF NOT EXISTS idx_user_practices_user ON user_practices(user_id);
CREATE INDEX IF NOT EXISTS idx_categories_parent ON categories(parent_id);
CREATE INDEX IF NOT EXISTS idx_practice_categories_cat ON practice_categories(category_id);
"""

MAX_CATEGORY_LEVEL = 4


@contextmanager
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def _alter_safe(c, sql: str):
    """ALTER TABLE, который не падает если колонка уже есть."""
    try:
        c.execute(sql)
    except sqlite3.OperationalError as e:
        if "duplicate column name" not in str(e).lower():
            raise


def _migrate_photos_to_disk(c):
    """Переводит legacy data:image/...;base64,... из БД в файлы на диске.
    Идемпотентна: ищет только записи, где photo начинается с 'data:'."""
    import base64, re
    PHOTOS_DIR.mkdir(parents=True, exist_ok=True)
    rows = c.execute("SELECT id, photo FROM practices WHERE photo LIKE 'data:%'").fetchall()
    if not rows:
        return
    log.info("Миграция фото: переношу %d записей из base64 в файлы", len(rows))
    for r in rows:
        m = re.match(r"data:image/(\w+);base64,(.+)", r["photo"], re.S)
        if not m:
            log.warning("Не разобрать data-url у практики %s, пропускаю", r["id"])
            continue
        ext = m.group(1).lower()
        if ext not in ("jpeg", "jpg", "png", "webp"):
            ext = "jpg"
        if ext == "jpeg":
            ext = "jpg"
        try:
            data = base64.b64decode(m.group(2))
            fname = f"{r['id']}.{ext}"
            (PHOTOS_DIR / fname).write_bytes(data)
            c.execute("UPDATE practices SET photo=? WHERE id=?", (f"/photos/{fname}", r["id"]))
        except Exception as e:
            log.warning("Не сохранить фото для %s: %s", r["id"], e)


def init_db():
    with db() as c:
        c.executescript(SCHEMA)
        # Миграции для существующих БД (новые колонки, добавленные позже первого релиза)
        _alter_safe(c, "ALTER TABLE users ADD COLUMN tz TEXT")
        _alter_safe(c, "ALTER TABLE users ADD COLUMN mute_until INTEGER DEFAULT 0")
        _alter_safe(c, "ALTER TABLE user_practices ADD COLUMN period_end_notified INTEGER DEFAULT 0")
        _migrate_photos_to_disk(c)


# ─── ДЕМО-ДАННЫЕ ───────────────────────────────────────────────────────────
DEMO_CATEGORIES = [
    # (id, name, parent_id, level, icon, sort_order)
    ("cat_demo_health",      "Здоровье",           None,                1, "🪷", 1),
    ("cat_demo_inner",       "Внутренняя работа",  None,                1, "🧘", 2),
    ("cat_demo_male",        "Мужские практики",   None,                1, "⚔️", 3),
    ("cat_demo_body",        "Телесные",           "cat_demo_health",   2, "💪", 1),
    ("cat_demo_food",        "Питание",            "cat_demo_health",   2, "🍃", 2),
    ("cat_demo_active",      "Активные",           "cat_demo_body",     3, "🏃", 1),
    ("cat_demo_meditation",  "Медитация",          "cat_demo_inner",    2, "📿", 1),
]

# (id, name, desc, type, target, unit, icon, palette, max_rem, from, to, [cat_ids])
DEMO_PRACTICES = [
    ("p_demo_meditation", "Утренняя медитация",
     "Сесть на 15 минут после пробуждения. Дыхание, без приложений и музыки.",
     "binary", None, "", "🧘", "mint", 2, "07:00", "11:00",
     ["cat_demo_meditation"]),
    ("p_demo_run", "Бег",
     "3 км в спокойном темпе. Можно с интервалами или пульсовой работой.",
     "count", 3, "км", "🏃", "rust", 2, "06:30", "20:00",
     ["cat_demo_active"]),
    ("p_demo_cold", "Холодный душ",
     "Минимум 60 секунд под холодной водой. Постепенно увеличивай время.",
     "binary", None, "", "💧", "azure", 2, "06:00", "10:00",
     ["cat_demo_body", "cat_demo_male"]),
    ("p_demo_read", "Чтение",
     "30 минут художественной книги или нон-фикшн. Не лента в телефоне.",
     "count", 30, "мин", "📖", "gold", 2, "18:00", "23:00",
     ["cat_demo_inner"]),
    ("p_demo_nosugar", "Без сахара",
     "Никаких сладостей и десертов сегодня. Фрукты можно.",
     "binary", None, "", "🌿", "sage", 1, "10:00", "21:00",
     ["cat_demo_food", "cat_demo_male"]),
]


def _seed_demo(c) -> int:
    """Идемпотентно заливает демо-категории и практики через INSERT OR IGNORE.
    Возвращает число созданных практик (для лога). Не трогает уже существующие записи."""
    now = datetime.now(TZ).isoformat()
    for cid, name, parent, level, icon, sort_order in DEMO_CATEGORIES:
        c.execute(
            """INSERT OR IGNORE INTO categories
               (id, name, parent_id, level, icon, sort_order, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (cid, name, parent, level, icon, sort_order, now),
        )
    created = 0
    for pid, name, desc, typ, target, unit, icon, palette, max_rem, t_from, t_to, cat_ids in DEMO_PRACTICES:
        cur = c.execute(
            """INSERT OR IGNORE INTO practices
               (id, name, description, type, target, unit, icon, palette,
                media_url, media_label, photo,
                max_reminders, reminder_from, reminder_to, active, created_at, created_by)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', '', NULL, ?, ?, ?, 1, ?, NULL)""",
            (pid, name, desc, typ, target, unit, icon, palette,
             max_rem, t_from, t_to, now),
        )
        if cur.rowcount > 0:
            created += 1
        for cat_id in cat_ids:
            c.execute(
                "INSERT OR IGNORE INTO practice_categories (practice_id, category_id) VALUES (?, ?)",
                (pid, cat_id),
            )
    return created


def seed_demo_once():
    """Вызывается при старте. Заливает демо ровно один раз — флаг в app_meta защищает
    от воскрешения удалённых админом практик при следующих рестартах."""
    with db() as c:
        existing = c.execute("SELECT 1 FROM app_meta WHERE key='seeded_demo_v1'").fetchone()
        if existing:
            return
        created = _seed_demo(c)
        c.execute("INSERT OR REPLACE INTO app_meta (key, value) VALUES ('seeded_demo_v1', ?)",
                  (datetime.now(TZ).isoformat(),))
        log.info("Демо-данные: создано %d практик и до %d категорий", created, len(DEMO_CATEGORIES))


# ─── ВАЛИДАЦИЯ TELEGRAM initData ───────────────────────────────────────────
def verify_init_data(init_data: str) -> Optional[dict]:
    """
    Проверяет подпись initData по правилам Telegram:
    https://core.telegram.org/bots/webapps#validating-data-received-via-the-mini-app
    Возвращает данные пользователя или None.
    """
    if not init_data or not BOT_TOKEN:
        return None
    try:
        parsed = dict(parse_qsl(init_data, keep_blank_values=True))
        received_hash = parsed.pop("hash", None)
        if not received_hash:
            return None
        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
        secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        calculated = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(calculated, received_hash):
            return None
        # Проверка свежести (не старше 24ч)
        auth_date = int(parsed.get("auth_date", 0))
        if auth_date and (datetime.now().timestamp() - auth_date) > 86400:
            return None
        user = json.loads(parsed.get("user", "{}"))
        return user if user.get("id") else None
    except Exception as e:
        log.warning("initData verify failed: %s", e)
        return None


def _safe_tz(name: str) -> Optional[ZoneInfo]:
    """Парсит имя зоны; возвращает None при ошибке (защита от мусора в заголовке)."""
    if not name or not isinstance(name, str) or len(name) > 64:
        return None
    try:
        return ZoneInfo(name)
    except Exception:
        return None


def require_user(init_data: str, tz_header: str = "") -> dict:
    user = verify_init_data(init_data)
    if not user:
        # В dev-режиме без BOT_TOKEN допускаем заголовок X-Dev-User-Id
        if not BOT_TOKEN and init_data and init_data.startswith("dev:"):
            return {"id": int(init_data[4:]), "first_name": "Dev", "username": "dev"}
        raise HTTPException(status_code=401, detail="Unauthorized")
    # Touch user record. Если в заголовке валидная IANA-зона и она ещё не задана у юзера,
    # сохраняем её (авто-определение). Юзер потом сможет переопределить вручную.
    now = datetime.now(TZ).isoformat()
    auto_tz = tz_header if _safe_tz(tz_header) else None
    with db() as c:
        c.execute(
            """INSERT INTO users (user_id, username, first_name, language, created_at, last_seen, tz)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(user_id) DO UPDATE SET
                 username=excluded.username,
                 first_name=excluded.first_name,
                 language=excluded.language,
                 last_seen=excluded.last_seen,
                 tz=COALESCE(users.tz, excluded.tz)""",
            (user["id"], user.get("username"), user.get("first_name"),
             user.get("language_code"), now, now, auto_tz),
        )
    return user


def require_admin(init_data: str, tz_header: str = "") -> dict:
    user = require_user(init_data, tz_header)
    if user["id"] not in ADMIN_IDS:
        raise HTTPException(status_code=403, detail="Admins only")
    return user


# ─── ВРЕМЯ И ЧАСОВОЙ ПОЯС ──────────────────────────────────────────────────
def get_user_tz(user_id: int, c=None) -> ZoneInfo:
    """Возвращает ZoneInfo юзера; если не задана — серверный TZ."""
    def _read(conn):
        r = conn.execute("SELECT tz FROM users WHERE user_id=?", (user_id,)).fetchone()
        return r["tz"] if r else None
    name = _read(c) if c is not None else None
    if c is None:
        with db() as conn:
            name = _read(conn)
    z = _safe_tz(name) if name else None
    return z or TZ


def user_now(user_id: int, c=None) -> datetime:
    return datetime.now(get_user_tz(user_id, c))


def user_today_str(user_id: int, c=None) -> str:
    return user_now(user_id, c).strftime("%Y-%m-%d")


def user_today_d(user_id: int, c=None) -> date:
    return user_now(user_id, c).date()


# ─── ФОТО ──────────────────────────────────────────────────────────────────
def save_photo_from_input(value: Optional[str], practice_id: str) -> Optional[str]:
    """Принимает то, что пришло в поле photo от клиента.
    Возможные варианты:
      - None или пустая строка → None (без фото)
      - '/photos/...'           → путь, оставить как есть
      - 'data:image/...;base64' → сохранить файл, вернуть /photos/<id>.<ext>
      - что-то ещё              → None (мусор игнорируем)"""
    if not value:
        return None
    if value.startswith("/photos/"):
        return value
    if not value.startswith("data:"):
        return None
    import base64, re
    m = re.match(r"data:image/(\w+);base64,(.+)", value, re.S)
    if not m:
        return None
    ext = m.group(1).lower()
    if ext == "jpeg":
        ext = "jpg"
    if ext not in ("jpg", "png", "webp"):
        ext = "jpg"
    try:
        data = base64.b64decode(m.group(2))
    except Exception:
        return None
    PHOTOS_DIR.mkdir(parents=True, exist_ok=True)
    fname = f"{practice_id}.{ext}"
    (PHOTOS_DIR / fname).write_bytes(data)
    return f"/photos/{fname}"


# ─── МОДЕЛИ ───────────────────────────────────────────────────────────────
class PracticeIn(BaseModel):
    name: str
    description: Optional[str] = ""
    type: Literal["binary", "count"] = "binary"
    target: Optional[int] = None
    unit: Optional[str] = ""
    icon: Optional[str] = "✨"
    palette: Optional[str] = "amber"
    media_url: Optional[str] = ""
    media_label: Optional[str] = ""
    photo: Optional[str] = None
    max_reminders: int = Field(3, ge=0, le=10)
    reminder_from: str = "08:00"
    reminder_to: str = "21:00"
    active: bool = True
    category_ids: list[str] = Field(default_factory=list)


class CategoryIn(BaseModel):
    name: str
    parent_id: Optional[str] = None
    icon: Optional[str] = ""
    sort_order: int = 0


class JoinIn(BaseModel):
    practice_id: str
    period_type: Literal["week", "month", "forever"] = "month"


class EntryIn(BaseModel):
    practice_id: str
    date: Optional[str] = None      # YYYY-MM-DD, default — сегодня
    completed: Optional[bool] = None
    count: Optional[int] = None     # абсолютное значение, не дельта


class SettingsIn(BaseModel):
    tz: Optional[str] = None             # IANA, например 'Europe/Moscow'
    mute_until: Optional[int] = None     # epoch seconds; 0/None = пауза снята


class ExtendIn(BaseModel):
    practice_id: str
    period_type: Optional[Literal["week", "month", "forever"]] = None  # default — текущий тип


# ─── ВСПОМОГАТЕЛЬНЫЕ ──────────────────────────────────────────────────────
def today_str() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d")


def is_done(practice_row, entry_row) -> bool:
    if not entry_row:
        return False
    if practice_row["type"] == "binary":
        return bool(entry_row["completed"])
    return (entry_row["count"] or 0) >= (practice_row["target"] or 1)


def compute_streaks(done_dates, today: date) -> tuple[int, int]:
    """По набору 'выполненных' дат (YYYY-MM-DD) возвращает (текущая_серия, рекорд).
    Текущая серия не обнуляется, если сегодня ещё не закрыто — отсчёт от вчера."""
    days = set(done_dates)
    if not days:
        return 0, 0
    cursor = today if today.isoformat() in days else today - timedelta(days=1)
    current = 0
    while cursor.isoformat() in days:
        current += 1
        cursor -= timedelta(days=1)
    sorted_days = sorted(date.fromisoformat(d) for d in days)
    best = run = 1
    for i in range(1, len(sorted_days)):
        if (sorted_days[i] - sorted_days[i - 1]).days == 1:
            run += 1
            if run > best:
                best = run
        else:
            run = 1
    return current, best


def compute_full_day_streaks(subs, done_set, today_d: date) -> dict:
    """Стрики 'полного дня'. Считаем активные на каждый день практики и долю выполненных.
    Дни без активных практик — нейтральны (не растят и не рвут серию).
    Сегодня не считаем провалом, если ratio < threshold (день ещё может быть закрыт).

    subs: list of (period_start_d, period_end_d_or_None, practice_id)
    done_set: set of (practice_id, date_iso)
    """
    if not subs:
        return {"full_current": 0, "full_best": 0, "half_current": 0, "half_best": 0}
    sub_l = [(ps, pe or date(9999, 1, 1), pid) for ps, pe, pid in subs]
    earliest = min(s[0] for s in sub_l)
    if (today_d - earliest).days > 365:
        earliest = today_d - timedelta(days=365)

    days_data: list = []  # [(date_d, ratio_or_None)]
    cur = earliest
    while cur <= today_d:
        active = [pid for ps, pe, pid in sub_l if ps <= cur <= pe]
        if not active:
            days_data.append((cur, None))
        else:
            done_in_day = sum(1 for pid in active if (pid, cur.isoformat()) in done_set)
            days_data.append((cur, done_in_day / len(active)))
        cur += timedelta(days=1)

    def streak_for(threshold: float) -> tuple[int, int]:
        cur_streak = 0
        first_active_seen = False
        for _, ratio in reversed(days_data):
            if ratio is None:
                continue
            ok = ratio >= threshold
            if ok:
                cur_streak += 1
                first_active_seen = True
            else:
                if not first_active_seen:
                    # Это сегодня (или ближайший к сегодня активный день) — он ещё может быть закрыт.
                    first_active_seen = True
                    continue
                break
        best = run = 0
        for _, ratio in days_data:
            if ratio is None:
                continue
            if ratio >= threshold:
                run += 1
                if run > best:
                    best = run
            else:
                run = 0
        return cur_streak, best

    f_cur, f_best = streak_for(1.0)
    h_cur, h_best = streak_for(0.5)
    return {"full_current": f_cur, "full_best": f_best,
            "half_current": h_cur, "half_best": h_best}


def practice_to_dict(row, category_ids: Optional[list] = None) -> dict:
    return {
        "id": row["id"],
        "name": row["name"],
        "description": row["description"] or "",
        "type": row["type"],
        "target": row["target"],
        "unit": row["unit"] or "",
        "icon": row["icon"] or "✨",
        "palette": row["palette"] or "amber",
        "media_url": row["media_url"] or "",
        "media_label": row["media_label"] or "",
        "photo": row["photo"],
        "max_reminders": row["max_reminders"],
        "reminder_from": row["reminder_from"],
        "reminder_to": row["reminder_to"],
        "active": bool(row["active"]),
        "category_ids": category_ids or [],
    }


# ─── КАТЕГОРИИ ─────────────────────────────────────────────────────────────
def _load_practice_categories(c, practice_ids) -> dict:
    """{practice_id: [category_id, ...]} одним запросом."""
    ids = list(practice_ids)
    if not ids:
        return {}
    placeholders = ",".join("?" * len(ids))
    rows = c.execute(
        f"SELECT practice_id, category_id FROM practice_categories WHERE practice_id IN ({placeholders})",
        ids,
    ).fetchall()
    out: dict = {}
    for r in rows:
        out.setdefault(r["practice_id"], []).append(r["category_id"])
    return out


def _category_descendants(c, root_id: str) -> list:
    """Все потомки + сам корень. Используется для фильтрации практик по категории."""
    rows = c.execute(
        """WITH RECURSIVE descendants(id) AS (
              SELECT id FROM categories WHERE id = ?
              UNION ALL
              SELECT c.id FROM categories c JOIN descendants d ON c.parent_id = d.id
           )
           SELECT id FROM descendants""",
        (root_id,),
    ).fetchall()
    return [r["id"] for r in rows]


def _save_practice_categories(c, practice_id: str, category_ids):
    """Перезаписывает связи практика→категории. Игнорирует несуществующие category_id."""
    c.execute("DELETE FROM practice_categories WHERE practice_id=?", (practice_id,))
    ids = [cid for cid in (category_ids or []) if cid]
    if not ids:
        return
    placeholders = ",".join("?" * len(ids))
    valid = [r["id"] for r in c.execute(
        f"SELECT id FROM categories WHERE id IN ({placeholders})", ids
    ).fetchall()]
    for cid in valid:
        c.execute(
            "INSERT OR IGNORE INTO practice_categories (practice_id, category_id) VALUES (?, ?)",
            (practice_id, cid),
        )


# ─── FASTAPI ───────────────────────────────────────────────────────────────
app = FastAPI(title="Practices Tracker")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/")
def root():
    return FileResponse("frontend/app.html")


@app.get("/app")
def app_page():
    return FileResponse("frontend/app.html")


@app.get("/admin")
def admin_page():
    return FileResponse("frontend/admin.html")


@app.get("/healthz")
def healthz():
    return {"ok": True, "ts": datetime.now(TZ).isoformat()}


# ─── API: Пользователь ────────────────────────────────────────────────────
@app.get("/api/me")
def me(x_init_data: str = Header(default=""), x_user_tz: str = Header(default="")):
    user = require_user(x_init_data, x_user_tz)
    with db() as c:
        r = c.execute("SELECT tz, mute_until FROM users WHERE user_id=?", (user["id"],)).fetchone()
    tz_name = (r["tz"] if r and r["tz"] else DEFAULT_TZ_NAME)
    mute_until = (r["mute_until"] or 0) if r else 0
    return {
        "id": user["id"],
        "name": user.get("first_name"),
        "is_admin": user["id"] in ADMIN_IDS,
        "tz": tz_name,
        "mute_until": mute_until,
        "muted": mute_until > int(datetime.now(TZ).timestamp()),
    }


@app.get("/api/me/settings")
def get_settings(x_init_data: str = Header(default=""), x_user_tz: str = Header(default="")):
    user = require_user(x_init_data, x_user_tz)
    with db() as c:
        r = c.execute("SELECT tz, mute_until FROM users WHERE user_id=?", (user["id"],)).fetchone()
    return {
        "tz": (r["tz"] if r and r["tz"] else DEFAULT_TZ_NAME),
        "mute_until": (r["mute_until"] or 0) if r else 0,
    }


@app.put("/api/me/settings")
def update_settings(body: SettingsIn,
                    x_init_data: str = Header(default=""),
                    x_user_tz: str = Header(default="")):
    user = require_user(x_init_data, x_user_tz)
    sets, params = [], []
    if body.tz is not None:
        if not _safe_tz(body.tz):
            raise HTTPException(400, "Bad tz")
        sets.append("tz=?")
        params.append(body.tz)
    if body.mute_until is not None:
        sets.append("mute_until=?")
        params.append(max(0, int(body.mute_until)))
    if not sets:
        return {"ok": True}
    params.append(user["id"])
    with db() as c:
        c.execute(f"UPDATE users SET {', '.join(sets)} WHERE user_id=?", params)
    return {"ok": True}


@app.get("/api/practices")
def list_practices(category_id: Optional[str] = None,
                   x_init_data: str = Header(default=""),
                   x_user_tz: str = Header(default="")):
    require_user(x_init_data, x_user_tz)
    with db() as c:
        if category_id:
            cat_ids = _category_descendants(c, category_id)
            if not cat_ids:
                return []
            placeholders = ",".join("?" * len(cat_ids))
            rows = c.execute(
                f"""SELECT DISTINCT p.* FROM practices p
                    JOIN practice_categories pc ON pc.practice_id = p.id
                    WHERE p.active = 1 AND pc.category_id IN ({placeholders})
                    ORDER BY p.created_at DESC""",
                cat_ids,
            ).fetchall()
        else:
            rows = c.execute("SELECT * FROM practices WHERE active=1 ORDER BY created_at DESC").fetchall()
        cat_map = _load_practice_categories(c, [r["id"] for r in rows])
    return [practice_to_dict(r, cat_map.get(r["id"], [])) for r in rows]


@app.get("/api/categories")
def list_categories(x_init_data: str = Header(default=""), x_user_tz: str = Header(default="")):
    """Плоский список — фронт сам строит дерево по parent_id."""
    require_user(x_init_data, x_user_tz)
    with db() as c:
        rows = c.execute(
            "SELECT * FROM categories ORDER BY level, sort_order, name"
        ).fetchall()
    return [dict(r) for r in rows]


@app.get("/api/my")
def my_data(x_init_data: str = Header(default=""), x_user_tz: str = Header(default="")):
    """Полное состояние текущего юзера: активные практики + записи (60 дн.) +
    история подписок (для heatmap) + стрики (full/half/legacy any-done)."""
    user = require_user(x_init_data, x_user_tz)
    with db() as c:
        tz = get_user_tz(user["id"], c)
        today_d = datetime.now(tz).date()
        today = today_d.isoformat()

        ups = c.execute(
            """SELECT up.*, p.* FROM user_practices up
               JOIN practices p ON p.id = up.practice_id
               WHERE up.user_id = ?
                 AND (up.period_end IS NULL OR up.period_end >= ?)""",
            (user["id"], today),
        ).fetchall()
        active_cat_map = _load_practice_categories(c, [r["id"] for r in ups])
        practices = []
        for r in ups:
            d = practice_to_dict(r, active_cat_map.get(r["id"], []))
            d["period_type"] = r["period_type"]
            d["period_start"] = r["period_start"]
            d["period_end"] = r["period_end"]
            practices.append(d)

        entries_rows = c.execute(
            """SELECT * FROM entries WHERE user_id = ?
               AND date >= date(?, '-60 days')""",
            (user["id"], today),
        ).fetchall()
        entries = {}
        for e in entries_rows:
            entries[f"{e['date']}_{e['practice_id']}"] = {
                "completed": bool(e["completed"]),
                "count": e["count"],
                "ts": e["ts"],
            }

        # Все подписки юзера (включая истёкшие) — для расчёта стриков «полного дня» и heatmap.
        subs_rows = c.execute(
            """SELECT up.practice_id, up.period_type, up.period_start, up.period_end,
                      p.name, p.description, p.type, p.target, p.unit,
                      p.icon, p.palette, p.photo, p.media_url, p.media_label
               FROM user_practices up
               JOIN practices p ON p.id = up.practice_id
               WHERE up.user_id = ?""",
            (user["id"],),
        ).fetchall()
        history_cat_map = _load_practice_categories(c, [r["practice_id"] for r in subs_rows])
        history_practices = [{
            "id": r["practice_id"],
            "name": r["name"],
            "description": r["description"] or "",
            "type": r["type"],
            "target": r["target"],
            "unit": r["unit"] or "",
            "icon": r["icon"] or "✨",
            "palette": r["palette"] or "amber",
            "photo": r["photo"],
            "media_url": r["media_url"] or "",
            "media_label": r["media_label"] or "",
            "period_type": r["period_type"],
            "period_start": r["period_start"],
            "period_end": r["period_end"],
            "category_ids": history_cat_map.get(r["practice_id"], []),
        } for r in subs_rows]

        # Полная история «выполненных» дней — для расчёта серий.
        done_rows = c.execute(
            """SELECT e.practice_id, e.date FROM entries e
               JOIN practices p ON p.id = e.practice_id
               WHERE e.user_id = ?
                 AND ( (p.type='binary' AND e.completed=1)
                       OR (p.type='count' AND e.count >= COALESCE(p.target,1)) )""",
            (user["id"],),
        ).fetchall()

    by_practice: dict = {}
    all_done_days: set = set()
    done_set: set = set()
    for r in done_rows:
        by_practice.setdefault(r["practice_id"], set()).add(r["date"])
        all_done_days.add(r["date"])
        done_set.add((r["practice_id"], r["date"]))

    for p in practices:
        cur, best = compute_streaks(by_practice.get(p["id"], set()), today_d)
        p["streak"] = cur
        p["best_streak"] = best

    subs_for_streak = [
        (date.fromisoformat(r["period_start"]),
         date.fromisoformat(r["period_end"]) if r["period_end"] else None,
         r["practice_id"])
        for r in subs_rows
    ]
    full_streaks = compute_full_day_streaks(subs_for_streak, done_set, today_d)
    overall_cur, overall_best = compute_streaks(all_done_days, today_d)

    return {
        "practices": practices,
        "history_practices": history_practices,
        "entries": entries,
        "today": today,
        "overall_streak": overall_cur,        # legacy (любая практика закрыта)
        "overall_best": overall_best,
        "full_day_streak": full_streaks["full_current"],
        "full_day_best": full_streaks["full_best"],
        "half_day_streak": full_streaks["half_current"],
        "half_day_best": full_streaks["half_best"],
    }


def _period_end_for(period_type: str, start_d: date) -> Optional[date]:
    if period_type == "week":
        return start_d + timedelta(days=7)
    if period_type == "month":
        return start_d + timedelta(days=30)
    return None  # forever


@app.post("/api/my/join")
def join_practice(body: JoinIn,
                  x_init_data: str = Header(default=""),
                  x_user_tz: str = Header(default="")):
    user = require_user(x_init_data, x_user_tz)
    with db() as c:
        today = user_today_d(user["id"], c)
        end = _period_end_for(body.period_type, today)
        practice = c.execute("SELECT id FROM practices WHERE id=? AND active=1", (body.practice_id,)).fetchone()
        if not practice:
            raise HTTPException(404, "Practice not found")
        c.execute(
            """INSERT INTO user_practices (user_id, practice_id, period_type, period_start, period_end, joined_at, period_end_notified)
               VALUES (?,?,?,?,?,?,0)
               ON CONFLICT(user_id, practice_id) DO UPDATE SET
                 period_type=excluded.period_type,
                 period_start=excluded.period_start,
                 period_end=excluded.period_end,
                 period_end_notified=0""",
            (user["id"], body.practice_id, body.period_type,
             today.isoformat(), end.isoformat() if end else None,
             datetime.now(TZ).isoformat()),
        )
    return {"ok": True}


@app.post("/api/my/extend")
def extend_practice(body: ExtendIn,
                    x_init_data: str = Header(default=""),
                    x_user_tz: str = Header(default="")):
    """Продлить подписку на практику. По умолчанию — тем же сроком, что был.
    Новая дата конца = max(текущая_period_end, сегодня) + срок."""
    user = require_user(x_init_data, x_user_tz)
    with db() as c:
        sub = c.execute(
            "SELECT period_type, period_end FROM user_practices WHERE user_id=? AND practice_id=?",
            (user["id"], body.practice_id),
        ).fetchone()
        if not sub:
            raise HTTPException(404, "Subscription not found")
        period_type = body.period_type or sub["period_type"]
        today = user_today_d(user["id"], c)
        if period_type == "forever":
            new_end = None
        else:
            base = today
            if sub["period_end"]:
                pe = date.fromisoformat(sub["period_end"])
                if pe > today:
                    base = pe
            delta = 7 if period_type == "week" else 30
            new_end = base + timedelta(days=delta)
        c.execute(
            """UPDATE user_practices
               SET period_type=?, period_end=?, period_end_notified=0
               WHERE user_id=? AND practice_id=?""",
            (period_type, new_end.isoformat() if new_end else None, user["id"], body.practice_id),
        )
    return {"ok": True, "period_end": new_end.isoformat() if new_end else None}


@app.delete("/api/my/leave/{practice_id}")
def leave_practice(practice_id: str,
                   x_init_data: str = Header(default=""),
                   x_user_tz: str = Header(default="")):
    user = require_user(x_init_data, x_user_tz)
    with db() as c:
        c.execute("DELETE FROM user_practices WHERE user_id=? AND practice_id=?", (user["id"], practice_id))
    return {"ok": True}


@app.post("/api/my/entry")
def upsert_entry(body: EntryIn,
                 x_init_data: str = Header(default=""),
                 x_user_tz: str = Header(default="")):
    user = require_user(x_init_data, x_user_tz)
    ts = int(datetime.now(TZ).timestamp())
    with db() as c:
        target_date = body.date or user_today_str(user["id"], c)
        practice = c.execute("SELECT type FROM practices WHERE id=?", (body.practice_id,)).fetchone()
        if not practice:
            raise HTTPException(404, "Practice not found")
        existing = c.execute(
            "SELECT * FROM entries WHERE user_id=? AND practice_id=? AND date=?",
            (user["id"], body.practice_id, target_date),
        ).fetchone()
        completed = int(body.completed) if body.completed is not None else (existing["completed"] if existing else 0)
        count = body.count if body.count is not None else (existing["count"] if existing else 0)
        if completed == 0 and count == 0:
            c.execute("DELETE FROM entries WHERE user_id=? AND practice_id=? AND date=?",
                      (user["id"], body.practice_id, target_date))
        else:
            c.execute(
                """INSERT INTO entries (user_id, practice_id, date, completed, count, ts)
                   VALUES (?,?,?,?,?,?)
                   ON CONFLICT(user_id, practice_id, date) DO UPDATE SET
                     completed=excluded.completed, count=excluded.count, ts=excluded.ts""",
                (user["id"], body.practice_id, target_date, completed, count, ts),
            )
    return {"ok": True}


@app.get("/api/leaderboard")
def leaderboard(period: Literal["week", "month", "all"] = "month",
                x_init_data: str = Header(default="")):
    """Рейтинг по проценту выполнения за период."""
    require_user(x_init_data)
    today = datetime.now(TZ).date()
    if period == "week":
        start = today - timedelta(days=7)
    elif period == "month":
        start = today - timedelta(days=30)
    else:
        start = today - timedelta(days=365)

    with db() as c:
        # для каждого юзера: сколько практико-дней должны были закрыть и сколько закрыли
        rows = c.execute("""
            SELECT u.user_id, u.first_name, u.username,
                   (SELECT COUNT(*) FROM entries e
                      JOIN practices p ON p.id = e.practice_id
                      WHERE e.user_id = u.user_id AND e.date >= ? AND e.date <= ?
                      AND ( (p.type='binary' AND e.completed=1)
                            OR (p.type='count' AND e.count >= COALESCE(p.target,1)) )
                   ) AS done_count,
                   (SELECT COUNT(*) FROM user_practices up
                      WHERE up.user_id = u.user_id
                      AND up.period_start <= ?
                      AND (up.period_end IS NULL OR up.period_end >= ?)
                   ) AS active_practices
            FROM users u
        """, (start.isoformat(), today.isoformat(), today.isoformat(), start.isoformat())).fetchall()

    period_days = (today - start).days or 1
    items = []
    for r in rows:
        active = r["active_practices"] or 0
        if active == 0:
            continue
        plan = active * period_days
        done = r["done_count"] or 0
        pct = round(min(100, done * 100 / plan)) if plan else 0
        items.append({
            "user_id": r["user_id"],
            "name": r["first_name"] or r["username"] or f"user{r['user_id']}",
            "done": done,
            "plan": plan,
            "pct": pct,
        })
    items.sort(key=lambda x: (-x["pct"], -x["done"]))
    for i, item in enumerate(items, 1):
        item["rank"] = i
    return items


# ─── API: Админка ──────────────────────────────────────────────────────────
@app.get("/api/admin/practices")
def admin_list(x_init_data: str = Header(default="")):
    require_admin(x_init_data)
    with db() as c:
        rows = c.execute("SELECT * FROM practices ORDER BY created_at DESC").fetchall()
        cat_map = _load_practice_categories(c, [r["id"] for r in rows])
    return [practice_to_dict(r, cat_map.get(r["id"], [])) for r in rows]


@app.get("/api/admin/categories")
def admin_list_categories(x_init_data: str = Header(default="")):
    require_admin(x_init_data)
    with db() as c:
        rows = c.execute(
            """SELECT cat.*,
                  (SELECT COUNT(*) FROM practice_categories pc WHERE pc.category_id = cat.id) AS practice_count
               FROM categories cat
               ORDER BY level, sort_order, name"""
        ).fetchall()
    return [dict(r) for r in rows]


@app.post("/api/admin/categories")
def admin_create_category(body: CategoryIn, x_init_data: str = Header(default="")):
    require_admin(x_init_data)
    name = (body.name or "").strip()
    if not name:
        raise HTTPException(400, "Имя категории не может быть пустым")
    with db() as c:
        if body.parent_id:
            parent = c.execute("SELECT level FROM categories WHERE id=?", (body.parent_id,)).fetchone()
            if not parent:
                raise HTTPException(404, "Родительская категория не найдена")
            level = parent["level"] + 1
            if level > MAX_CATEGORY_LEVEL:
                raise HTTPException(400, f"Превышена максимальная глубина {MAX_CATEGORY_LEVEL}")
        else:
            level = 1
        cid = "c_" + secrets.token_hex(6)
        c.execute(
            """INSERT INTO categories (id, name, parent_id, level, icon, sort_order, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (cid, name, body.parent_id, level, body.icon or "", body.sort_order,
             datetime.now(TZ).isoformat()),
        )
    return {"id": cid, "level": level}


@app.put("/api/admin/categories/{cid}")
def admin_update_category(cid: str, body: CategoryIn, x_init_data: str = Header(default="")):
    """Можно менять name, icon, sort_order. parent_id фиксирован после создания."""
    require_admin(x_init_data)
    name = (body.name or "").strip()
    if not name:
        raise HTTPException(400, "Имя категории не может быть пустым")
    with db() as c:
        existing = c.execute("SELECT id FROM categories WHERE id=?", (cid,)).fetchone()
        if not existing:
            raise HTTPException(404, "Not found")
        c.execute("UPDATE categories SET name=?, icon=?, sort_order=? WHERE id=?",
                  (name, body.icon or "", body.sort_order, cid))
    return {"ok": True}


@app.delete("/api/admin/categories/{cid}")
def admin_delete_category(cid: str, x_init_data: str = Header(default="")):
    """Каскадно удаляет потомков и связи с практиками (FK ON DELETE CASCADE)."""
    require_admin(x_init_data)
    with db() as c:
        c.execute("DELETE FROM categories WHERE id=?", (cid,))
    return {"ok": True}


@app.post("/api/admin/seed_demo")
def admin_seed_demo(x_init_data: str = Header(default="")):
    """Принудительная заливка демо. Идемпотентно (INSERT OR IGNORE), флаг игнорируется."""
    require_admin(x_init_data)
    with db() as c:
        created = _seed_demo(c)
        c.execute("INSERT OR REPLACE INTO app_meta (key, value) VALUES ('seeded_demo_v1', ?)",
                  (datetime.now(TZ).isoformat(),))
    return {"ok": True, "created_practices": created, "categories": len(DEMO_CATEGORIES)}


@app.get("/api/admin/stats")
def admin_stats(x_init_data: str = Header(default="")):
    require_admin(x_init_data)
    with db() as c:
        users = c.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"]
        practices = c.execute("SELECT COUNT(*) AS n FROM practices WHERE active=1").fetchone()["n"]
        active_today = c.execute(
            "SELECT COUNT(DISTINCT user_id) AS n FROM entries WHERE date=?", (today_str(),)
        ).fetchone()["n"]
        joined = c.execute(
            "SELECT COUNT(*) AS n FROM user_practices WHERE period_end IS NULL OR period_end >= ?",
            (today_str(),),
        ).fetchone()["n"]
    return {"users": users, "practices": practices, "active_today": active_today, "joined": joined}


@app.post("/api/admin/practices")
def admin_create(body: PracticeIn, x_init_data: str = Header(default="")):
    user = require_admin(x_init_data)
    pid = "p_" + secrets.token_hex(6)
    photo_value = save_photo_from_input(body.photo, pid)
    with db() as c:
        c.execute(
            """INSERT INTO practices
               (id, name, description, type, target, unit, icon, palette, media_url, media_label,
                photo, max_reminders, reminder_from, reminder_to, active, created_at, created_by)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (pid, body.name, body.description, body.type, body.target, body.unit, body.icon,
             body.palette, body.media_url, body.media_label, photo_value,
             body.max_reminders, body.reminder_from, body.reminder_to,
             int(body.active), datetime.now(TZ).isoformat(), user["id"]),
        )
        _save_practice_categories(c, pid, body.category_ids)
    return {"id": pid}


@app.put("/api/admin/practices/{pid}")
def admin_update(pid: str, body: PracticeIn, x_init_data: str = Header(default="")):
    require_admin(x_init_data)
    photo_value = save_photo_from_input(body.photo, pid)
    with db() as c:
        existing = c.execute("SELECT id FROM practices WHERE id=?", (pid,)).fetchone()
        if not existing:
            raise HTTPException(404, "Not found")
        c.execute(
            """UPDATE practices SET name=?, description=?, type=?, target=?, unit=?, icon=?,
               palette=?, media_url=?, media_label=?, photo=?, max_reminders=?,
               reminder_from=?, reminder_to=?, active=? WHERE id=?""",
            (body.name, body.description, body.type, body.target, body.unit, body.icon,
             body.palette, body.media_url, body.media_label, photo_value,
             body.max_reminders, body.reminder_from, body.reminder_to,
             int(body.active), pid),
        )
        _save_practice_categories(c, pid, body.category_ids)
    return {"ok": True}


@app.delete("/api/admin/practices/{pid}")
def admin_delete(pid: str, x_init_data: str = Header(default="")):
    require_admin(x_init_data)
    with db() as c:
        c.execute("DELETE FROM practices WHERE id=?", (pid,))
    return {"ok": True}


@app.get("/api/admin/users")
def admin_users(x_init_data: str = Header(default="")):
    require_admin(x_init_data)
    with db() as c:
        rows = c.execute("""
            SELECT u.user_id, u.username, u.first_name, u.created_at, u.last_seen,
                   (SELECT COUNT(*) FROM user_practices up WHERE up.user_id = u.user_id
                    AND (up.period_end IS NULL OR up.period_end >= ?)) AS active_practices,
                   (SELECT COUNT(*) FROM entries e WHERE e.user_id = u.user_id) AS total_entries
            FROM users u ORDER BY u.last_seen DESC
        """, (today_str(),)).fetchall()
    return [dict(r) for r in rows]


# ─── TELEGRAM BOT ──────────────────────────────────────────────────────────
bot = Bot(token=BOT_TOKEN) if BOT_TOKEN else None
dp = Dispatcher()


def webapp_kb(text: str = "✨ Открыть приложение") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=text, web_app=WebAppInfo(url=f"{BASE_URL}/app"))
    ]])


@dp.message(Command("start"))
async def on_start(msg: Message):
    is_admin = msg.from_user.id in ADMIN_IDS
    text = (
        f"Здравствуй, {msg.from_user.first_name}.\n\n"
        "Это трекер регулярных практик. Выбери из общего списка то, что хочешь делать, "
        "и я буду помогать не забывать.\n\n"
    )
    if is_admin:
        text += f"Ты админ. Управление практиками: {BASE_URL}/admin\n\n"
    text += (
        "Команды:\n"
        "/open — открыть приложение\n"
        "/today — что сегодня\n"
        "/stop — поставить напоминания на паузу до завтра\n"
        "/leave_all — выйти из всех практик"
    )
    await msg.answer(text, reply_markup=webapp_kb())


@dp.message(Command("open"))
async def on_open(msg: Message):
    await msg.answer("Открой приложение:", reply_markup=webapp_kb())


@dp.message(Command("today"))
async def on_today(msg: Message):
    uid = msg.from_user.id
    with db() as c:
        today = user_today_str(uid, c)
        rows = c.execute("""
            SELECT p.name, p.type, p.target,
                   (SELECT completed FROM entries WHERE user_id=? AND practice_id=p.id AND date=?) AS done,
                   (SELECT count FROM entries WHERE user_id=? AND practice_id=p.id AND date=?) AS cnt
            FROM user_practices up JOIN practices p ON p.id = up.practice_id
            WHERE up.user_id=? AND (up.period_end IS NULL OR up.period_end >= ?)
            ORDER BY p.name
        """, (uid, today, uid, today, uid, today)).fetchall()
    if not rows:
        await msg.answer("Ты ещё не выбрал практик. Открой приложение и присоединись к чему-нибудь.",
                         reply_markup=webapp_kb())
        return
    lines = [f"<b>{today}</b>", ""]
    done_total = 0
    for r in rows:
        if r["type"] == "binary":
            ok = bool(r["done"])
            mark = "✅" if ok else "⭕"
            lines.append(f"{mark} {r['name']}")
            if ok: done_total += 1
        else:
            cnt = r["cnt"] or 0
            target = r["target"] or 1
            ok = cnt >= target
            mark = "✅" if ok else "⏳"
            lines.append(f"{mark} {r['name']} — {cnt}/{target}")
            if ok: done_total += 1
    lines.append("")
    lines.append(f"Выполнено: {done_total}/{len(rows)}")
    await msg.answer("\n".join(lines), parse_mode="HTML", reply_markup=webapp_kb())


@dp.message(Command("stop"))
async def on_stop(msg: Message):
    """Пауза напоминаний до завтра 00:00 по TZ юзера. Подписки и стрики не трогаем."""
    uid = msg.from_user.id
    with db() as c:
        tz = get_user_tz(uid, c)
        now = datetime.now(tz)
        tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        mute_until = int(tomorrow.timestamp())
        c.execute("UPDATE users SET mute_until=? WHERE user_id=?", (mute_until, uid))
    await msg.answer(
        "Окей, до завтра напоминаний не будет. Стрики и подписки сохраню — "
        "просто завтра утром начну как обычно.\n\n"
        "Если хочешь выйти из конкретной практики — открой её в приложении и нажми «Выйти». "
        "Или /leave_all чтобы выйти из всех сразу.",
        reply_markup=webapp_kb(),
    )


@dp.message(Command("leave_all"))
async def on_leave_all(msg: Message):
    """Полный выход из всех практик (старое поведение /stop)."""
    with db() as c:
        c.execute("DELETE FROM user_practices WHERE user_id=?", (msg.from_user.id,))
    await msg.answer(
        "Снял все подписки. Напоминаний больше не будет, прогресс остался в истории.\n\n"
        "Чтобы вернуться — открой приложение и присоединись заново.",
        reply_markup=webapp_kb(),
    )


@dp.callback_query(F.data.startswith("extend:"))
async def on_extend_cb(cq: CallbackQuery):
    """Кнопка «Продлить» под уведомлением об окончании периода."""
    try:
        _, pid, period_type = cq.data.split(":", 2)
    except ValueError:
        await cq.answer("Не разобрать кнопку"); return
    if period_type not in ("week", "month", "forever"):
        await cq.answer("Неверный срок"); return
    uid = cq.from_user.id
    with db() as c:
        sub = c.execute(
            "SELECT period_end FROM user_practices WHERE user_id=? AND practice_id=?",
            (uid, pid),
        ).fetchone()
        if not sub:
            await cq.answer("Подписка не найдена"); return
        today = user_today_d(uid, c)
        if period_type == "forever":
            new_end = None
        else:
            base = today
            if sub["period_end"]:
                pe = date.fromisoformat(sub["period_end"])
                if pe > today:
                    base = pe
            delta = 7 if period_type == "week" else 30
            new_end = base + timedelta(days=delta)
        c.execute(
            """UPDATE user_practices SET period_type=?, period_end=?, period_end_notified=0
               WHERE user_id=? AND practice_id=?""",
            (period_type, new_end.isoformat() if new_end else None, uid, pid),
        )
    until = "без срока" if not new_end else f"до {new_end.isoformat()}"
    await cq.answer("Продлил")
    try:
        await cq.message.edit_text(f"✅ Продлил подписку, {until}.")
    except Exception:
        await cq.message.answer(f"✅ Продлил подписку, {until}.")


@dp.callback_query(F.data.startswith("leave:"))
async def on_leave_cb(cq: CallbackQuery):
    """Кнопка «Уйти из практики» под уведомлением."""
    try:
        _, pid = cq.data.split(":", 1)
    except ValueError:
        await cq.answer("Не разобрать кнопку"); return
    with db() as c:
        c.execute("DELETE FROM user_practices WHERE user_id=? AND practice_id=?",
                  (cq.from_user.id, pid))
    await cq.answer("Вышел из практики")
    try:
        await cq.message.edit_text("👋 Снял подписку. История отметок осталась.")
    except Exception:
        await cq.message.answer("👋 Снял подписку. История отметок осталась.")


# ─── ПЛАНИРОВЩИК НАПОМИНАНИЙ ──────────────────────────────────────────────
async def reminders_tick():
    """Запускается каждые 30 минут. Для каждой подписки решаем, шлём ли напоминание,
    с учётом TZ юзера, его mute_until и окна reminder_from..reminder_to."""
    if not bot:
        return
    server_now_ts = int(datetime.now(TZ).timestamp())

    with db() as c:
        # Берём все активные подписки + tz/mute_until юзера. Дата «сегодня» считается per-user ниже.
        rows = c.execute("""
            SELECT up.user_id, u.tz, u.mute_until,
                   p.id AS practice_id, p.name, p.type, p.target,
                   p.max_reminders, p.reminder_from, p.reminder_to,
                   up.period_end
            FROM user_practices up
            JOIN practices p ON p.id = up.practice_id
            JOIN users u ON u.user_id = up.user_id
            WHERE p.active=1
        """).fetchall()

    sent_now = 0
    for r in rows:
        uid = r["user_id"]
        # Пауза напоминаний?
        if r["mute_until"] and r["mute_until"] > server_now_ts:
            continue
        # Часовой пояс юзера
        tz = _safe_tz(r["tz"]) or TZ
        u_now = datetime.now(tz)
        u_today = u_now.strftime("%Y-%m-%d")
        u_cur_t = u_now.time()
        # Период подписки: проверка относительно «сегодня по TZ юзера»
        if r["period_end"] and r["period_end"] < u_today:
            continue

        # Проверка факта выполнения и лимитов делаем коротким запросом
        with db() as c:
            done_row = c.execute(
                "SELECT completed, count FROM entries WHERE user_id=? AND practice_id=? AND date=?",
                (uid, r["practice_id"], u_today),
            ).fetchone()
            sent_row = c.execute(
                "SELECT count, last_at FROM reminders_sent WHERE user_id=? AND practice_id=? AND date=?",
                (uid, r["practice_id"], u_today),
            ).fetchone()

        if r["type"] == "binary" and done_row and done_row["completed"]:
            continue
        if r["type"] == "count" and done_row and (done_row["count"] or 0) >= (r["target"] or 1):
            continue
        sent = (sent_row["count"] if sent_row else 0) or 0
        max_r = r["max_reminders"] or 0
        if max_r == 0 or sent >= max_r:
            continue
        try:
            t_from = datetime.strptime(r["reminder_from"] or "08:00", "%H:%M").time()
            t_to = datetime.strptime(r["reminder_to"] or "21:00", "%H:%M").time()
        except Exception:
            t_from, t_to = time(8, 0), time(21, 0)
        if not (t_from <= u_cur_t <= t_to):
            continue
        window_min = (datetime.combine(date.today(), t_to) - datetime.combine(date.today(), t_from)).total_seconds() / 60
        min_gap_min = max(30, int(window_min / max_r))
        last_at = sent_row["last_at"] if sent_row else None
        if last_at and (server_now_ts - last_at) < min_gap_min * 60:
            continue

        try:
            text = f"⏰ Напомню про практику: <b>{r['name']}</b>"
            if r["type"] == "count":
                done_cnt = (done_row["count"] if done_row else 0) or 0
                text += f"\nПрогресс: {done_cnt}/{r['target']}"
            text += f"\n\nНапоминание {sent + 1} из {max_r} на сегодня."
            await bot.send_message(uid, text, parse_mode="HTML", reply_markup=webapp_kb("Открыть"))
            with db() as c:
                c.execute(
                    """INSERT INTO reminders_sent (user_id, practice_id, date, count, last_at)
                       VALUES (?,?,?,1,?)
                       ON CONFLICT(user_id, practice_id, date) DO UPDATE SET
                         count = count + 1, last_at = excluded.last_at""",
                    (uid, r["practice_id"], u_today, server_now_ts),
                )
            sent_now += 1
        except Exception as e:
            log.warning("send to %s failed: %s", uid, e)
            if "blocked" in str(e).lower() or "Forbidden" in str(e):
                with db() as c:
                    c.execute("DELETE FROM user_practices WHERE user_id=?", (uid,))
    if sent_now:
        log.info("Sent %d reminders", sent_now)


async def period_check_tick():
    """Раз в день: ищем подписки, у которых period_end через 3 дня (или раньше, если пропустили),
    и шлём юзеру уведомление с inline-кнопками 'продлить' / 'уйти'."""
    if not bot:
        return
    server_now_ts = int(datetime.now(TZ).timestamp())
    with db() as c:
        rows = c.execute("""
            SELECT up.user_id, up.practice_id, up.period_type, up.period_end,
                   u.tz, u.mute_until, p.name
            FROM user_practices up
            JOIN users u ON u.user_id = up.user_id
            JOIN practices p ON p.id = up.practice_id
            WHERE up.period_end IS NOT NULL
              AND up.period_end_notified = 0
        """).fetchall()
    sent = 0
    for r in rows:
        if r["mute_until"] and r["mute_until"] > server_now_ts:
            continue  # юзер на паузе — попробуем завтра
        tz = _safe_tz(r["tz"]) or TZ
        today_d = datetime.now(tz).date()
        try:
            pe = date.fromisoformat(r["period_end"])
        except Exception:
            continue
        days_left = (pe - today_d).days
        if days_left > 3 or days_left < 0:
            continue  # ещё не время или уже истекло
        period_type = r["period_type"] or "month"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"Продлить ({_period_label(period_type)})",
                                  callback_data=f"extend:{r['practice_id']}:{period_type}")],
            [InlineKeyboardButton(text="Уйти из практики",
                                  callback_data=f"leave:{r['practice_id']}")],
        ])
        days_word = "дня" if 2 <= days_left <= 4 else "день" if days_left == 1 else "дней"
        text = (f"📅 Подписка на «<b>{r['name']}</b>» заканчивается "
                f"{'сегодня' if days_left == 0 else f'через {days_left} {days_word}'} "
                f"({pe.isoformat()}). Продлеваем?")
        try:
            await bot.send_message(r["user_id"], text, parse_mode="HTML", reply_markup=kb)
            with db() as c:
                c.execute(
                    "UPDATE user_practices SET period_end_notified=1 WHERE user_id=? AND practice_id=?",
                    (r["user_id"], r["practice_id"]),
                )
            sent += 1
        except Exception as e:
            log.warning("period notify to %s failed: %s", r["user_id"], e)
    if sent:
        log.info("Sent %d period-end notices", sent)


def _period_label(t: str) -> str:
    return {"week": "ещё на неделю", "month": "ещё на месяц", "forever": "без срока"}.get(t, "тем же сроком")


# ─── ЗАПУСК ────────────────────────────────────────────────────────────────
scheduler: Optional[AsyncIOScheduler] = None


@app.on_event("startup")
async def on_startup():
    init_db()
    seed_demo_once()
    Path("frontend").mkdir(exist_ok=True)
    log.info("DB ready at %s", DB_PATH.absolute())
    log.info("Photos at %s", PHOTOS_DIR.absolute())
    log.info("BASE_URL = %s", BASE_URL)
    log.info("Admins   = %s", ADMIN_IDS or "(none)")

    global scheduler
    if bot:
        # Установим команды и кнопку меню
        try:
            await bot.set_my_commands([
                BotCommand(command="start", description="Начать"),
                BotCommand(command="open", description="Открыть приложение"),
                BotCommand(command="today", description="Что сегодня"),
                BotCommand(command="stop", description="Пауза напоминаний до завтра"),
                BotCommand(command="leave_all", description="Выйти из всех практик"),
            ])
            await bot.set_chat_menu_button(
                menu_button=MenuButtonWebApp(text="Практики", web_app=WebAppInfo(url=f"{BASE_URL}/app"))
            )
        except Exception as e:
            log.warning("Bot setup warning: %s", e)

        # Параллельно крутим polling бота
        asyncio.create_task(dp.start_polling(bot))

        # Планировщик: напоминания каждые 30 мин, ежедневная проверка периодов
        scheduler = AsyncIOScheduler(timezone=TZ)
        scheduler.add_job(reminders_tick, "interval", minutes=30,
                          next_run_time=datetime.now(TZ) + timedelta(seconds=30))
        scheduler.add_job(period_check_tick, "cron", hour=10, minute=0,
                          next_run_time=datetime.now(TZ) + timedelta(seconds=60))
        scheduler.start()
        log.info("Bot + scheduler started")
    else:
        log.info("Bot disabled (no BOT_TOKEN)")


@app.on_event("shutdown")
async def on_shutdown():
    if scheduler: scheduler.shutdown(wait=False)
    if bot: await bot.session.close()


# Раздаём фронт ПОСЛЕ объявления роутов /app, /admin
app.mount("/static", StaticFiles(directory="frontend"), name="static")
# Каталог под фото и его раздача
PHOTOS_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/photos", StaticFiles(directory=str(PHOTOS_DIR)), name="photos")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=PORT, reload=False)
