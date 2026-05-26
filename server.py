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
import random
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

from fastapi import FastAPI, Header, HTTPException, Request, UploadFile, File, Form, Depends, Response
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, Literal

from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

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
  id             TEXT PRIMARY KEY,
  name           TEXT NOT NULL,
  description    TEXT,
  type           TEXT NOT NULL,         -- 'binary' | 'count' | 'text' | 'photo' | 'video' | 'rounds' | 'timer' — главный (определяет «день засчитан»)
  extras         TEXT,                  -- CSV из 'text','photo','video': опциональные доп. поля (юзер может, но не обязан)
  target         INTEGER,
  unit           TEXT,
  rounds_count   INTEGER,               -- для type='rounds': сколько раундов (например, 5)
  work_seconds   INTEGER,               -- для type='rounds': секунд активной фазы (например, 20)
  rest_seconds   INTEGER,               -- для type='rounds': секунд паузы между раундами (например, 20)
  icon           TEXT,
  palette        TEXT,
  media_url      TEXT,
  media_label    TEXT,
  photo          TEXT,                  -- /photos/<id>.jpg или (легаси) data:...base64
  max_reminders  INTEGER DEFAULT 3,     -- сколько раз в день напоминать
  reminder_from  TEXT DEFAULT '08:00',  -- окно напоминаний начало
  reminder_to    TEXT DEFAULT '21:00',  -- окно напоминаний конец
  active         INTEGER DEFAULT 1,
  catalog_hidden INTEGER DEFAULT 0,     -- 1 = не показывать в общем каталоге (только как уровень программы или по индив. назначению)
  created_at     TEXT NOT NULL,
  created_by     INTEGER
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
  user_id            INTEGER NOT NULL,
  practice_id        TEXT NOT NULL,
  date               TEXT NOT NULL,           -- YYYY-MM-DD
  completed          INTEGER DEFAULT 0,
  count              INTEGER DEFAULT 0,
  response_text      TEXT,                    -- для type='text'
  response_photo     TEXT,                    -- /photos/entries/<file>.jpg для type='photo'
  response_video_url TEXT,                    -- ссылка для type='video'
  ts                 INTEGER NOT NULL,
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

CREATE TABLE IF NOT EXISTS identities (
  user_id        INTEGER NOT NULL,
  provider       TEXT NOT NULL,        -- 'telegram' | 'email'
  external_id    TEXT NOT NULL,        -- TG user_id (str) или email (lowercase)
  password_hash  TEXT,                 -- bcrypt hash, только для provider='email'
  email_verified INTEGER DEFAULT 0,    -- 0/1
  created_at     TEXT NOT NULL,
  PRIMARY KEY (provider, external_id),
  FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS programs (
  id           TEXT PRIMARY KEY,
  name         TEXT NOT NULL,
  description  TEXT,
  icon         TEXT,
  palette      TEXT,
  on_miss      TEXT NOT NULL DEFAULT 'reset',    -- 'reset' | 'continue' — что делать при пропущенном дне
  visibility   TEXT NOT NULL DEFAULT 'public',   -- 'public' (в каталоге) | 'individual' (только назначенным)
  active       INTEGER DEFAULT 1,
  created_at   TEXT NOT NULL,
  created_by   INTEGER
);

CREATE TABLE IF NOT EXISTS program_levels (
  program_id     TEXT NOT NULL,
  level_order    INTEGER NOT NULL,    -- 1..N — порядок уровня внутри программы
  practice_id    TEXT NOT NULL,       -- ссылка на существующую практику
  duration_days  INTEGER NOT NULL,    -- сколько дней этот уровень идёт
  PRIMARY KEY (program_id, level_order),
  FOREIGN KEY (program_id) REFERENCES programs(id) ON DELETE CASCADE,
  FOREIGN KEY (practice_id) REFERENCES practices(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS user_programs (
  user_id              INTEGER NOT NULL,
  program_id           TEXT NOT NULL,
  current_level        INTEGER NOT NULL DEFAULT 1,    -- 1..N
  level_started_at     TEXT NOT NULL,                  -- YYYY-MM-DD, дата начала текущего уровня
  level_completed_days INTEGER NOT NULL DEFAULT 0,     -- сколько дней засчитано в текущем уровне (для on_miss='continue')
  status               TEXT NOT NULL DEFAULT 'active', -- 'active' | 'completed'
  joined_at            TEXT NOT NULL,
  completed_at         TEXT,
  PRIMARY KEY (user_id, program_id),
  FOREIGN KEY (program_id) REFERENCES programs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS motivations (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  practice_id TEXT,                  -- XOR с program_id: ровно одно поле должно быть заполнено
  program_id  TEXT,
  kind        TEXT NOT NULL,         -- 'start' | 'streak' | 'miss'
  value       INTEGER NOT NULL DEFAULT 0,  -- для streak/miss — N дней; для start — 0
  text        TEXT NOT NULL,
  created_at  TEXT NOT NULL,
  FOREIGN KEY (practice_id) REFERENCES practices(id) ON DELETE CASCADE,
  FOREIGN KEY (program_id) REFERENCES programs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS motivations_sent (
  user_id     INTEGER NOT NULL,
  scope_kind  TEXT NOT NULL,         -- 'practice' | 'program'
  scope_id    TEXT NOT NULL,
  kind        TEXT NOT NULL,         -- 'start' | 'streak' | 'miss'
  value       INTEGER NOT NULL,
  sent_date   TEXT NOT NULL,         -- YYYY-MM-DD по TZ юзера
  PRIMARY KEY (user_id, scope_kind, scope_id, kind, value, sent_date)
);

CREATE TABLE IF NOT EXISTS user_assignments (
  user_id      INTEGER NOT NULL,
  target_type  TEXT NOT NULL,         -- 'practice' | 'program'
  target_id    TEXT NOT NULL,
  assigned_at  TEXT NOT NULL,
  assigned_by  INTEGER,
  PRIMARY KEY (user_id, target_type, target_id)
);

CREATE INDEX IF NOT EXISTS idx_entries_date ON entries(date);
CREATE INDEX IF NOT EXISTS idx_user_practices_user ON user_practices(user_id);
CREATE INDEX IF NOT EXISTS idx_categories_parent ON categories(parent_id);
CREATE INDEX IF NOT EXISTS idx_practice_categories_cat ON practice_categories(category_id);
CREATE INDEX IF NOT EXISTS idx_identities_user ON identities(user_id);
CREATE INDEX IF NOT EXISTS idx_program_levels_program ON program_levels(program_id);
CREATE INDEX IF NOT EXISTS idx_user_programs_user ON user_programs(user_id);
CREATE INDEX IF NOT EXISTS idx_motivations_practice ON motivations(practice_id, kind, value);
CREATE INDEX IF NOT EXISTS idx_motivations_program ON motivations(program_id, kind, value);
CREATE INDEX IF NOT EXISTS idx_user_assignments_user ON user_assignments(user_id);
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
        _alter_safe(c, "ALTER TABLE user_practices ADD COLUMN priority INTEGER DEFAULT 0")
        _alter_safe(c, "ALTER TABLE practices ADD COLUMN catalog_hidden INTEGER DEFAULT 0")
        _alter_safe(c, "ALTER TABLE entries ADD COLUMN response_text TEXT")
        _alter_safe(c, "ALTER TABLE entries ADD COLUMN response_photo TEXT")
        _alter_safe(c, "ALTER TABLE entries ADD COLUMN response_video_url TEXT")
        _alter_safe(c, "ALTER TABLE practices ADD COLUMN extras TEXT")
        # 4 CSV-колонки для фильтров: ежедневная/утренняя/...; пассивная/активная/...;
        # инвентарь (коврик,скакалка,...); место (дом,улица,...)
        _alter_safe(c, "ALTER TABLE practices ADD COLUMN tag_frequency TEXT")
        _alter_safe(c, "ALTER TABLE practices ADD COLUMN tag_activity TEXT")
        _alter_safe(c, "ALTER TABLE practices ADD COLUMN tag_inventory TEXT")
        _alter_safe(c, "ALTER TABLE practices ADD COLUMN tag_location TEXT")
        # Раунды: интервальная тренировка (N подходов × work_seconds + rest_seconds)
        _alter_safe(c, "ALTER TABLE practices ADD COLUMN rounds_count INTEGER")
        _alter_safe(c, "ALTER TABLE practices ADD COLUMN work_seconds INTEGER")
        _alter_safe(c, "ALTER TABLE practices ADD COLUMN rest_seconds INTEGER")
        # До 3 аудио, проигрываются по очереди при открытии практики.
        # Хранится как JSON: [{"url": "...", "label": "..."}].
        _alter_safe(c, "ALTER TABLE practices ADD COLUMN audios TEXT")
        # Индивидуальный план раундов: JSON-список [{work, rest, sound_url?}].
        # Если задан — переопределяет rounds_count/work_seconds/rest_seconds в RoundsRunner.
        _alter_safe(c, "ALTER TABLE practices ADD COLUMN rounds_plan TEXT")
        # Точные времена напоминаний: CSV "HH:MM,HH:MM,...". Если задано —
        # игнорирует max_reminders и окно reminder_from/reminder_to, шлёт ровно
        # в каждое перечисленное время (с допуском в один тик планировщика).
        _alter_safe(c, "ALTER TABLE practices ADD COLUMN reminder_times TEXT")
        _migrate_photos_to_disk(c)
        _backfill_telegram_identities(c)


def _backfill_telegram_identities(c):
    """Для каждого users без identity-записи добавляет provider='telegram'.
    Идемпотентно через INSERT OR IGNORE. Запускается при каждом старте — дешёво."""
    rows = c.execute(
        """SELECT u.user_id, u.created_at FROM users u
           WHERE NOT EXISTS (
             SELECT 1 FROM identities i
             WHERE i.user_id = u.user_id AND i.provider = 'telegram'
           )"""
    ).fetchall()
    for r in rows:
        c.execute(
            """INSERT OR IGNORE INTO identities
               (user_id, provider, external_id, created_at)
               VALUES (?, 'telegram', ?, ?)""",
            (r["user_id"], str(r["user_id"]), r["created_at"] or datetime.now(TZ).isoformat()),
        )
    if rows:
        log.info("Backfill identities: %d Telegram-юзеров получили identity-запись", len(rows))


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


# ─── ЛИЧНАЯ ПРОГРАММА (СКРЫТЫЕ ПРАКТИКИ) ───────────────────────────────────
# 8-недельная программа контроля + 2 практики из блока «чувствительность».
# Все с catalog_hidden=1 — не светятся в общем каталоге, видны автору в /admin
# и доступны через индивидуальное назначение (user_assignments) или прямую подписку.
# (id, name, desc, type, target, unit, icon, palette, max_rem, t_from, t_to)
PERSONAL_PRACTICES = [
    # Недели 1–2. Фундамент: дыхание + идентификация + расслабление
    ("pc_w12_breath", "[Н1-2] Диафрагмальное дыхание",
     "Лёжа, рука на животе. 5–10 минут. Вдох на счёт 4 — поднимается живот, не грудь. "
     "Выдох на счёт 8. Цель — сделать длинный выдох автоматическим.",
     "binary", None, "", "🌬️", "sage", 1, "07:00", "22:00"),
    ("pc_w12_floor_id", "[Н1-2] Идентификация тазового дна",
     "Найди мышцу тазового дна. Учись отличать три состояния: сжатие, расслабление, полный покой. "
     "Без возбуждения — это диагностика, не тренировка.",
     "binary", None, "", "🧘", "sage", 1, "07:00", "22:00"),
    ("pc_w12_release", "[Н1-2] Тренировка расслабления",
     "10 повторов. Сжал тазовое дно на 3 секунды → отпустил → продолжаешь отпускать ещё глубже, "
     "«роняя» таз вниз. Вторая часть (углублённое отпускание) — ключевой навык.",
     "binary", None, "", "💧", "sage", 1, "07:00", "22:00"),

    # Недели 3–4. Шкала и точка невозврата
    ("pc_w34_scale", "[Н3-4] Калибровка шкалы 1–10",
     "Стимуляция с постоянным вопросом «где я сейчас, 1–10?». Задача не довести до конца, "
     "а точно называть уровень. Особое внимание зоне 7–9.",
     "binary", None, "", "📊", "gold", 1, "07:00", "22:00"),
    ("pc_w34_signals", "[Н3-4] Картирование сигналов",
     "Запоминаешь телесные предвестники точки невозврата: где, как, какой ритм. "
     "Цель — ловить за 3–5 секунд ДО точки невозврата.",
     "binary", None, "", "🎯", "gold", 1, "07:00", "22:00"),
    ("pc_w34_startstop", "[Н3-4] Базовый старт-стоп",
     "3–4 цикла за подход. Доводишь до 8 → полная остановка → ждёшь падения до 4–5 → продолжаешь. "
     "Завершаешь сам, когда решил. Здесь перепрошивается рефлекс.",
     "binary", None, "", "🚦", "gold", 1, "07:00", "22:00"),

    # Недели 5–6. Интеграция — контроль без пауз
    ("pc_w56_no_stop", "[Н5-6] Старт-стоп без остановки движения",
     "На подходе к 8 — не прекращаешь стимуляцию, а: длинный выдох + расслабление таза + замедление. "
     "Уровень падает до 6 без полной паузы. Переход от «аварийного тормоза» к «управлению газом».",
     "binary", None, "", "🌊", "azure", 1, "07:00", "22:00"),
    ("pc_w56_surf", "[Н5-6] Сёрфинг по шкале 6–8",
     "Сознательно удерживаешься в зоне 6–8 минимум 5 минут. Катаешься вверх-вниз, "
     "не сваливаясь в 9–10 и не падая в 4. Управляемое плато — рабочий навык.",
     "binary", None, "", "🏄", "azure", 1, "07:00", "22:00"),
    ("pc_w56_release_peak", "[Н5-6] Расслабление на пике",
     "В момент приближения к точке невозврата — вместо сжатия полное отпускание таза + выдох. "
     "Сначала будет срываться — это нормально. Переучиваешь рефлекс с напряжения на расслабление.",
     "binary", None, "", "🔓", "azure", 1, "07:00", "22:00"),

    # Недели 7–8. Перенос и углубление
    ("pc_w78_sensate", "[Н7-8] Sensate focus",
     "Только при наличии партнёра. Начинаешь не с проникновения, а с прикосновений без цели возбудиться. "
     "Снимает «гонку к финишу», которая обнуляет наработанный контроль.",
     "binary", None, "", "🤲", "amber", 1, "07:00", "22:00"),
    ("pc_w78_transfer", "[Н7-8] Перенос техник в секс",
     "Те же дыхание, шкала, расслабление — но во внешнем контексте, где больше отвлекающих факторов. "
     "Контроль временно просядет — это ожидаемо, навык адаптируется.",
     "binary", None, "", "🔄", "amber", 1, "07:00", "22:00"),
    ("pc_w78_split", "[Н7-8] Разделение оргазма и эякуляции (опц.)",
     "ТОЛЬКО после уверенного владения расслаблением. На точке невозврата — полное отпускание "
     "тазового дна + выдох + удержание внимания. Через расслабление, не через механическую блокировку.",
     "binary", None, "", "✨", "amber", 1, "07:00", "22:00"),

    # Параллельная линия — чувствительность тела
    ("pc_sens_slow", "[Чувств.] Замедление",
     "Сознательно снижаешь интенсивность и скорость стимуляции. Привычка к быстрой/сильной "
     "мастурбации перенастраивает порог — замедление со временем возвращает тонкость восприятия.",
     "binary", None, "", "🐢", "mint", 1, "07:00", "22:00"),
    ("pc_sens_presence", "[Чувств.] Полное присутствие в теле",
     "Внимание на физических ощущениях здесь и сейчас. Не в голове, не в фантазиях, не в тревоге "
     "о результате. Медитативное ядро тантры.",
     "binary", None, "", "👁️", "mint", 1, "07:00", "22:00"),
]


def _seed_personal_practices(c) -> int:
    """Идемпотентно заливает личные практики автора (catalog_hidden=1).
    INSERT OR IGNORE по фиксированным ID. Возвращает число созданных."""
    now = datetime.now(TZ).isoformat()
    created = 0
    for pid, name, desc, typ, target, unit, icon, palette, max_rem, t_from, t_to in PERSONAL_PRACTICES:
        cur = c.execute(
            """INSERT OR IGNORE INTO practices
               (id, name, description, type, target, unit, icon, palette,
                media_url, media_label, photo,
                max_reminders, reminder_from, reminder_to,
                active, catalog_hidden, created_at, created_by)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', '', NULL, ?, ?, ?, 1, 1, ?, NULL)""",
            (pid, name, desc, typ, target, unit, icon, palette,
             max_rem, t_from, t_to, now),
        )
        if cur.rowcount > 0:
            created += 1
    return created


def seed_personal_once():
    """Заливает личные практики ровно один раз. Флаг 'seeded_personal_v1' в app_meta
    защищает от воскрешения, если автор удалит их вручную из админки."""
    with db() as c:
        if c.execute("SELECT 1 FROM app_meta WHERE key='seeded_personal_v1'").fetchone():
            return
        created = _seed_personal_practices(c)
        c.execute("INSERT OR REPLACE INTO app_meta (key, value) VALUES ('seeded_personal_v1', ?)",
                  (datetime.now(TZ).isoformat(),))
        log.info("Личные практики: создано %d скрытых записей", created)


# ─── 90-ДНЕВНЫЙ ПРОТОКОЛ (СКРЫТЫЕ ПРАКТИКИ) ────────────────────────────────
# Полная программа: утро, день, вечер, добавки, движение/голос, фастинг,
# выходы 3-го месяца, шаббат. Все catalog_hidden=1.
# Префикс id: p90_*; префикс в названии помечает зону дня / категорию.
# (id, name, desc, type, target, unit, icon, palette, max_rem, t_from, t_to)
PERSONAL_PRACTICES_90D = [
    # ─── УТРО (с Н1, ежедневно) ────────────────────────────────────────
    ("p90_ground", "[У] Заземление «Сначала я»",
     "2 мин сразу после пробуждения, ДО телефона. Сесть на край кровати, стопы на пол. "
     "5 медленных вдохов-выдохов, ладонь на груди. Мысленно: «Я здесь. Что сейчас в теле — "
     "моё. Чужое со вчера отпускаю». Цель — поставить границу до того, как день «вселится».",
     "binary", None, "", "🌅", "gold", 1, "06:00", "08:30"),
    ("p90_acv", "[У] Яблочный уксус натощак",
     "1–2 ст. ложки нефильтрованного ACV на 0,5–1 л воды комнатной температуры. Пить через "
     "трубочку медленно в течение 8–10 минут (можно во время Вим Хофа). После — прополоскать "
     "рот чистой водой. Сглаживает глюкозный отклик, поддерживает чувствительность к инсулину. "
     "Не делать в фаст-дни.",
     "binary", None, "", "🍎", "rust", 1, "06:00", "08:30"),
    ("p90_wimhof", "[У] Вим Хоф — 3 раунда (15 мин)",
     "Под голос приложения Wim Hof. Раунд = 30–40 глубоких циклов (вдох полный, выдох "
     "пассивный) → задержка на пустых лёгких до позыва вдохнуть (цель ~1,5 мин, без насилия) "
     "→ вдох + задержка на вдохе 15 сек → выдох. 3 раунда. НЕ делать стоя/в воде/за рулём. "
     "В фаст-дни пропускаем.",
     "binary", None, "", "🌬️", "gold", 1, "06:00", "08:30"),
    ("p90_cold", "[У] Холодная ванна (10 мин)",
     "После Вим Хофа. Войти на спокойном ВЫДОХЕ (не на вдохе). НЕ задерживать дыхание — "
     "удлинённый выдох. Сознательно расслаблять плечи, челюсть, лицо. Дыхание ровное. "
     "После — дать телу согреться самому 3–5 мин, не прыгать в горячее. В фаст-дни пропускаем.",
     "binary", None, "", "🧊", "azure", 1, "06:00", "08:30"),
    ("p90_release", "[У] Зажимы 30 мин (ротация по неделям)",
     "Зона ротируется: Н1/5/9 — горло, Н2/6/10 — грудь, Н3/7/11 — таз, Н4/8/12 — слабейшая. "
     "Схема 30 мин: 5 мин разогрев+дыхание в зону → 20 мин упражнения зоны (растяжка, "
     "освобождение зажима, голос/звук из зоны) → 5 мин лёжа, сканирование «стало теплее/"
     "свободнее?». Цель — физически разжать клапан проявления.",
     "binary", None, "", "🔓", "rust", 1, "06:00", "09:00"),
    ("p90_breath427", "[У] Дыхание 4-2-7 (10 мин)",
     "Сидя прямо, рука на животе. Вдох носом 4 сек (живот вперёд) → пауза 2 сек (мягкая, без "
     "сжатия) → выдох 7 сек (живот втянут к концу). Под интервальный таймер 10 мин. Перевод "
     "нервной системы из симпатики в парасимпатику — закрывает разгон от Вим Хофа и холода.",
     "binary", None, "", "🫁", "sage", 1, "06:30", "09:00"),
    ("p90_iam", "[У] Аффирмации «Я есмь» (5 мин)",
     "Сразу после 4-2-7 (НС восприимчива). 3–5 слов-качеств (нежность, игривость, лёгкость, "
     "сексуальность, бытие...). Каждое — вслух, из груди, низкой нотой, в настоящем: «Я есмь "
     "нежность». Пауза 5–10 сек — ловить телесный отклик. Тепло = твоё; сжатие = заходи через "
     "мягкую дверь («Я впускаю...»). 3 повтора на слово. БЫТЬ, не достигать.",
     "binary", None, "", "✨", "gold", 1, "06:30", "09:00"),
    ("p90_breakfast", "[У] Завтрак: яйца + черника",
     "3 яйца (варёные/глазунья) + 7 черник. С едой — утренние добавки. Холин, D, B12, цинк, "
     "холестерин (сырьё для тестостерона) + быстрые углеводы/калий/магний из ягод. Лёгкий "
     "питательный старт, не тяжёлый. В фаст-дни — пропуск (только вода + электролиты).",
     "binary", None, "", "🥚", "gold", 1, "06:30", "09:00"),

    # ─── ДЕНЬ (с Н1, ежедневно) ────────────────────────────────────────
    ("p90_metrics", "[Д] Замеры в трекере ×3",
     "Утро (после практик) / середина дня / вечер. Каждый замер 30 сек, шкала 1–10: "
     "физическая энергия · ясность · сексуальная энергия · «был собой, не солью» · "
     "заряд/перегруз · голос из груди/горла · сон · вода/электролиты · + строка «где это "
     "было в теле». Через 21 день увидишь по цифрам, что наполняет и что сливает.",
     "binary", None, "", "📊", "amber", 3, "07:00", "22:00"),
    ("p90_bodyscan", "[Д] Сканирование тела ×2 (по 7 мин)",
     "Позднее утро + день. Глаза закрыты, веди внимание сверху вниз: макушка → лицо → шея → "
     "плечи → грудь → живот → таз → ноги → стопы. На каждой зоне называй ТОЛЬКО физику: "
     "тепло / холод / сжато / свободно / пусто / пульсирует. НЕ интерпретируй («я тревожусь» "
     "→ нет; «в груди сжатие» → да). Ловит подмену чувства мыслью.",
     "binary", None, "", "🫀", "amber", 2, "07:00", "21:00"),
    ("p90_pause", "[Д] Правило паузы «ощущение → слово»",
     "В моменте — что-то задело: СТОП на 10 сек → «ГДЕ это в теле и КАКОЕ» (давит/горит/"
     "холодеет/комок) → ТОЛЬКО ПОТОМ называешь эмоцию и реагируешь. Выставляет зазор между "
     "триггером и реакцией. Удар по перехвату и по резкости.",
     "binary", None, "", "⏸️", "amber", 1, "08:00", "22:00"),
    ("p90_mine", "[Д] Моё / чужое (1 раз днём)",
     "Вспомни сильнейшую эмоцию дня — спроси: моя или впитал от кого-то (Рыбы на ASC)? "
     "Если чужое — мысленно отпусти. Чтобы не носить и не отыгрывать чужие состояния как свои.",
     "binary", None, "", "🪞", "amber", 1, "12:00", "22:00"),
    ("p90_squats", "[Д] Приседания (Greasing the Groove)",
     "По часовому триггеру каждые 60–90 мин (с 8:00 до 20:00) — один микро-подход. "
     "Прогрессия дневного суммарного объёма: Н1-2 — 40 (6×7), Н3-4 — 60, Н5-6 — 80, "
     "Н7-8 — 100, Н9-10 — 120, Н11-12 — 150. НИКОГДА до отказа: оставляй 3–5 повторов в "
     "запасе. В фаст-дни — половина объёма.",
     "binary", None, "", "🦵", "amber", 1, "08:00", "20:00"),
    ("p90_pushups", "[Д] Отжимания (Greasing the Groove)",
     "Параллельно приседам, по тому же триггеру. Прогрессия: Н1-2 — 18 (можно с колен), "
     "Н3-4 — 30, Н5-6 — 45, Н7-8 — 60, Н9-10 — 75, Н11-12 — 90. Локти ~45° к корпусу, "
     "тело прямая линия. С колен/от опоры пока форма не чистая. В фаст-дни — половина.",
     "binary", None, "", "💪", "amber", 1, "08:00", "20:00"),
    ("p90_water", "[Д] Вода ~3 литра",
     "Распределить равномерно весь день, не залпом. В фаст-дни ОБЯЗАТЕЛЬНО с электролитами "
     "(щепоть соли + магний/калий из еды накануне). 3 л без соли на голоде = головная боль "
     "от вымывания натрия.",
     "binary", None, "", "💧", "azure", 1, "07:00", "22:00"),

    # ─── ВЕЧЕР (с Н1, ежедневно) ───────────────────────────────────────
    ("p90_emowheel", "[В] Колесо эмоций (5 мин)",
     "Сильнейшее состояние дня → ощущение в теле → базовая эмоция → её оттенок. Записать. "
     "Строит словарь СВОИХ чувств (для других уже есть, для себя пусто). В дни видео-"
     "рефлексии — проговорить голосом ВНУТРИ записи, не дублировать письменно.",
     "binary", None, "", "🎡", "sage", 1, "20:00", "23:00"),
    ("p90_writeoff", "[В] Выгрузка тяжёлого (5 мин)",
     "Письменно — три состояния ноют сегодня, отпускаю. «Выжим губки» (Рыбы на ASC): чтобы "
     "чужое и невыраженное не копилось в перегруз. В дни видео-рефлексии — проговорить "
     "голосом внутри записи, не дублировать письменно.",
     "binary", None, "", "📝", "sage", 1, "20:00", "23:00"),
    ("p90_letter", "[В] Письмо без адресата (по факту)",
     "Когда поднимается сильная эмоция — пиши сыро, не редактируя, пока не отпустит. Твой "
     "канал разрядки через слово (Меркурий-Плутон) сильнее, чем через слёзы. Клапан "
     "ослабляется, слёзы придут сами — не гонись за ними.",
     "binary", None, "", "✉️", "sage", 0, "12:00", "23:00"),
    ("p90_magnesium", "[В] Магний глицинат",
     "За 30–60 мин до сна. Мягкий магний для нервной системы и сна (в отличие от утреннего "
     "цитрата — тот бодрит). Закрывает обе задачи НС: цитрат тонизирует утром, глицинат "
     "успокаивает вечером.",
     "binary", None, "", "💊", "rust", 1, "20:00", "23:30"),
    ("p90_ashwagandha", "[В] Ашваганда (KSM-66)",
     "Вечером, рядом с магнием глицинатом. Снижает кортизол и стресс. Под твою перегруженную "
     "НС (выгорание/тревога) — релевантно именно тебе. Курсами ~8 нед, потом пауза. "
     "Внимание: взаимодействия с лекарствами — при медикаментах сверяться с врачом.",
     "binary", None, "", "🌿", "rust", 1, "20:00", "23:30"),
    ("p90_hotshower", "[В] Горячий душ (2–3 мин, последним)",
     "Тёплая-горячая вода на шею и плечи, расслабляешь их под струёй. Сразу после — в "
     "постель. Завершение дня, переход в парасимпатику. Сон = главный канал переработки "
     "эмоций и роста тестостерона — это помогает уснуть глубже.",
     "binary", None, "", "🚿", "sage", 1, "21:00", "23:30"),

    # ─── ВЕЧЕР (с Месяца 2: Н5+) ───────────────────────────────────────
    ("p90_reading", "[В] Чтение вслух (10 мин) — с Н5",
     "Возьми текст (например, «Прознания Аватра»). Читай медленно, дожимая каждое окончание "
     "слова. Опора на диафрагму, голос из груди (как на низкой ноте утром). Интонация к "
     "концу фразы — ВНИЗ (вес и уверенность). Раз в неделю записать минуту на диктофон, "
     "сравнить с прошлой. Лечит «глотание» окончаний (Близнецы гонят).",
     "binary", None, "", "📖", "sage", 1, "20:00", "23:00"),

    # ─── ВЕЧЕР (с Месяца 2: Н7+) ───────────────────────────────────────
    ("p90_camera", "[В] Камера 2 мин — с Н7",
     "Записать 2 минуты на камеру без подготовки. Тема — что хочешь сказать прямо сейчас. "
     "БЕЗ монтажа, БЕЗ пересъёмки. Приватно, для себя. Цель — привыкание быть видимым, "
     "десенсибилизация страха признания.",
     "binary", None, "", "📹", "rust", 1, "20:00", "23:00"),

    # ─── ВЕЧЕР (с Месяца 2: Н8+) ───────────────────────────────────────
    ("p90_videoreflect", "[В] Видео-рефлексия дня — с Н8",
     "8–10 мин на камеру, приватно. Часть А — тень: где сегодня был другим / что осознал / "
     "какую теневую черту заметил и что с ней сделал (интегрировал / подавил / убежал). "
     "Часть Б — день: насколько ценно прожил (1–10 и почему) / где сопротивление / что "
     "перестать делать. Сквозной слой: в моменте говорить «прямо сейчас я чувствую ___, в "
     "теле это ___». Замыкает петлю утренней аффирмации.",
     "binary", None, "", "🎬", "rust", 1, "20:00", "23:00"),
    ("p90_playing_i", "[В] Играющий «я» на камеру (1×/нед) — с Н8",
     "Раз в неделю записать себя на камеру в свободной импровизации — без структуры, без "
     "цели, живо. Можешь говорить, петь, дурачиться. Цель — выпустить подавленную игровую/"
     "спонтанную часть (то самое «Я есмь игра»). Без оценки качества, приватно.",
     "binary", None, "", "🎭", "rust", 0, "12:00", "23:00"),

    # ─── ДОБАВКИ (с Н1) ────────────────────────────────────────────────
    ("p90_morningsupps", "[Доб] Утренние добавки",
     "С завтраком (для жирорастворимых): D3+K2, омега-3, B-комплекс, магний цитрат, цинк "
     "(не с магнием/кальцием — конкуренция). База для тестостерона и энергии. В фаст-дни "
     "жирорастворимые пропускаем (нужна еда).",
     "binary", None, "", "🌞", "rust", 1, "06:30", "10:00"),
    ("p90_no_stack", "[Доб] NO-стек — с Н3-4",
     "L-цитруллин 1,5–3 г и/или пикногенол + L-аргинин. Оксид азота, кровенаполнение, "
     "механика эрекции. Тайминг: утром или за 30–60 мин до тренировки/близости. Еду не "
     "требует — можно в фаст-день.",
     "binary", None, "", "🧪", "rust", 1, "06:30", "18:00"),
    ("p90_herbs", "[Доб] Травы (женьшень + мака) — с Н5-6",
     "Panax ginseng 600–1000 мг 2–3×/день, ПОСЛЕДНИЙ приём до 14:00 (иначе сон). Мака — "
     "утром. Курсами ~8 нед с паузой. Женьшень — на эректильную функцию (доказательно); "
     "мака — на либидо/желание. Взаимодействия с разжижителями крови / щитовидкой / "
     "седативными — при медикаментах сверяться.",
     "binary", None, "", "🌱", "rust", 1, "07:00", "14:00"),

    # ─── ДВИЖЕНИЕ / ГОЛОС (с Н6) ───────────────────────────────────────
    ("p90_singing", "[Дв] Пение (10–15 мин, 2–3×/нед) — с Н6",
     "Разогрев humming 2 мин → открытые гласные «а-о-у» на опоре → петь в ПОЛНЫЙ голос "
     "(из живота и груди, не горла) песни, которые трогают. НЕ оценивай «красиво/нет» — "
     "задача петь полно и свободно. Если слёзы — не зажимай, пропусти. Голос + зажим горла/"
     "груди + разрядка стыда признания одним заходом.",
     "binary", None, "", "🎤", "azure", 1, "12:00", "20:00"),
    ("p90_dance", "[Дв] Танец (10–15 мин, 2–3×/нед) — с Н6",
     "Свободный танец без хореографии. БЕЗ зеркала-судьи. Закрой глаза, выбери музыку под "
     "настроение, дай телу ВЕСТИ. Особенно отпускай таз и бёдра. Разреши себе быть "
     "«некрасивым», странным, нелепым — в этом весь смысл (роняешь крутящуюся витрину "
     "тени). Прямое ПРОЖИВАНИЕ теневых качеств телом.",
     "binary", None, "", "💃", "azure", 1, "12:00", "20:00"),

    # ─── ФАСТИНГ ───────────────────────────────────────────────────────
    ("p90_fast24", "[Фаст] Фаст 24 часа (2×/нед)",
     "Последний приём накануне — плотный (белок+жир), не сахарный. В сутки — вода + "
     "электролиты (соль обязательно), травяной чай, чёрный кофе без добавок. День без "
     "тяжёлой нагрузки: убрать силовую/холод/Вим Хоф; оставить 4-2-7, сканирование, "
     "лёгкие приседания. Выход мягкий — яйца + черника.",
     "binary", None, "", "🌊", "mint", 1, "06:00", "20:00"),
    ("p90_fast48", "[Фаст] Фаст 48 часов (1×/мес, конец месяца)",
     "Подготовка — накануне усиленные электролиты, без сахара/алкоголя. В течение — вода + "
     "электролиты УСИЛЕННО (соль критична), отдых, минимум нагрузки, только дыхание/прогулки. "
     "Выход (важно!) — ломать малым: бульон/яйца/черника, через 1–2 часа лёгкий приём. "
     "Глубокая аутофагия раз в месяц.",
     "binary", None, "", "🌑", "mint", 1, "06:00", "20:00"),

    # ─── МЕСЯЦ 3 (Н7+) ─────────────────────────────────────────────────
    ("p90_sublimation", "[М3] Сублимация энергии в письмо — с Н7",
     "Когда поднимается сексуальная/творческая энергия — направь её в письмо/текст/проект, "
     "а не на разрядку. Твой канал (Меркурий-Плутон) трансформирует через слово. "
     "Регулярная практика конвертации заряда в продукт.",
     "binary", None, "", "🔥", "rust", 0, "08:00", "22:00"),

    # ─── МЕСЯЦ 3 (Н9+) ─────────────────────────────────────────────────
    ("p90_sacral", "[М3] Сакральный отклик — с Н9",
     "Перед решениями и выступлениями: задать вопрос → НЕ думать → поймать первый телесный "
     "звук/импульс из живота («угу» = да; «нет звука» / сжатие = нет). Выбор из тела, "
     "не из головы. Дизайн человека: сакральная стратегия.",
     "binary", None, "", "🔮", "rust", 0, "08:00", "22:00"),
    ("p90_realout1", "[М3] Реальный выход (стих/камера на аудиторию) — Н9",
     "Прочитать стих или записать видео для конкретной (пусть узкой) аудитории — НЕ для "
     "себя в стол. Первая сцена — выйти из приватного в видимое. Один раз за неделю Н9.",
     "binary", None, "", "🎯", "rust", 0, "08:00", "22:00"),

    # ─── МЕСЯЦ 3 (Н10+) ────────────────────────────────────────────────
    ("p90_voiceload", "[М3] Голос под нагрузкой — с Н10",
     "Намеренно использовать голос в ситуации стресса/давления: говорить из груди, не "
     "сжимать, не повышать. Сложный разговор, спор, выступление — практика голоса в "
     "симпатике. Закрепляет навык, наработанный в спокойной среде.",
     "binary", None, "", "🗣️", "rust", 0, "08:00", "22:00"),
    ("p90_realout2", "[М3] Реальный выход №2 (узкая аудитория) — Н10",
     "Расширение круга: выступить/показаться перед группой 3–10 человек. Не самым близким "
     "и не публично — промежуточный шаг между стихом в стол и публичным.",
     "binary", None, "", "🎯", "rust", 0, "08:00", "22:00"),

    # ─── МЕСЯЦ 3 (Н11+) ────────────────────────────────────────────────
    ("p90_audit", "[М3] Аудит «настоящий / соль» — еженедельно с Н11",
     "Раз в неделю просмотр видео-рефлексий: где был собой и где «солью» (натянутым, "
     "играющим роль, говорящим из ума, а не из тела). Внутренняя чистота через ретроспективу. "
     "Видишь дугу: какие качества всплывают чаще, где повторяется сопротивление.",
     "binary", None, "", "🔍", "rust", 0, "12:00", "22:00"),
    ("p90_realout3", "[М3] Реальный выход №3 (публично) — Н11",
     "Публичное выступление/публикация — выход в широкое поле. Финальная точка дуги "
     "приватное → узкое → публичное. Внутренний материал получает реальную сцену.",
     "binary", None, "", "🎯", "rust", 0, "08:00", "22:00"),

    # ─── ШАББАТ ────────────────────────────────────────────────────────
    ("p90_shabbat", "[Шб] Шаббат-день (1×/нед)",
     "Один выделенный день: с подъёма до обеда — основной фокус, глубоко (рефлексия / "
     "письмо / без графика / по самочувствию). Не «отключение», а ОСМЫСЛЕНИЕ и "
     "проговаривание. Уровневый слой через слово (MC в Стрельце, Рыбы ASC).",
     "binary", None, "", "🕊️", "sage", 0, "06:00", "20:00"),
]


def _seed_personal_practices_90d(c) -> int:
    """Идемпотентно заливает 90-дневный протокол (catalog_hidden=1)."""
    now = datetime.now(TZ).isoformat()
    created = 0
    for pid, name, desc, typ, target, unit, icon, palette, max_rem, t_from, t_to in PERSONAL_PRACTICES_90D:
        cur = c.execute(
            """INSERT OR IGNORE INTO practices
               (id, name, description, type, target, unit, icon, palette,
                media_url, media_label, photo,
                max_reminders, reminder_from, reminder_to,
                active, catalog_hidden, created_at, created_by)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', '', NULL, ?, ?, ?, 1, 1, ?, NULL)""",
            (pid, name, desc, typ, target, unit, icon, palette,
             max_rem, t_from, t_to, now),
        )
        if cur.rowcount > 0:
            created += 1
    return created


def seed_personal_90d_once():
    """Заливает 90-дневный протокол ровно один раз. Флаг 'seeded_90day_v1' защищает
    от воскрешения, если автор удалит часть практик из админки."""
    with db() as c:
        if c.execute("SELECT 1 FROM app_meta WHERE key='seeded_90day_v1'").fetchone():
            return
        created = _seed_personal_practices_90d(c)
        c.execute("INSERT OR REPLACE INTO app_meta (key, value) VALUES ('seeded_90day_v1', ?)",
                  (datetime.now(TZ).isoformat(),))
        log.info("90-дневный протокол: создано %d скрытых записей", created)


# ─── МОТИВАШКИ ДЛЯ ЛИЧНЫХ ПРАКТИК ──────────────────────────────────────────
# По 10 на практику: 5 streak (1/3/5/7/10) + 5 miss (1/3/7/14/30).
# Текст = шаблон + ядро смысла практики (PRACTICE_CORES).
# Miss-тексты с интонацией «ты можешь / тело помнит» — поддержка, не упрёк.

# Ядро смысла каждой практики: что она даёт / зачем она нужна.
PRACTICE_CORES = {
    # ─── Программа контроля (pc_*) ────
    "pc_w12_breath":      "длинный выдох — фундамент контроля, без него вся надстройка не держится",
    "pc_w12_floor_id":    "учишься различать сжатие и расслабление таза — диагностика, а не сила",
    "pc_w12_release":     "расслабление таза — ключевой навык, а не упражнение",
    "pc_w34_scale":       "называешь уровень 1–10 в реальном времени — это уже половина контроля",
    "pc_w34_signals":     "ловишь сигналы тела за 3–5 секунд до точки невозврата",
    "pc_w34_startstop":   "старт-стоп перепрошивает рефлекс, не просто отсрочивает финиш",
    "pc_w56_no_stop":     "управляешь газом, а не давишь на аварийный тормоз",
    "pc_w56_surf":        "удерживаешь плато 6–8 — управляемое возбуждение, а не гонка",
    "pc_w56_release_peak": "переучиваешь рефлекс с напряжения на расслабление на пике",
    "pc_w78_sensate":     "прикосновения без цели снимают гонку к финишу",
    "pc_w78_transfer":    "перенос контроля из соло в реальный контекст с партнёром",
    "pc_w78_split":       "разделение оргазма и эякуляции — через расслабление, не через блокировку",
    "pc_sens_slow":       "замедление возвращает тонкость восприятия, которую съела скорость",
    "pc_sens_presence":   "полное присутствие в теле — медитативное ядро всей практики",

    # ─── 90-дневный протокол (p90_*) ────
    "p90_ground":         "ставишь границу в начале дня — чтобы не носить чужое весь день",
    "p90_acv":            "сглаживает глюкозный отклик и поддерживает чувствительность к инсулину",
    "p90_wimhof":         "разрядка застоя, переключение состояния, контроль стресса по команде",
    "p90_cold":           "длинный выдох в стрессе — тренировка спокойствия под нагрузкой",
    "p90_release":        "физически разжимаешь клапан проявления — горло, грудь, таз",
    "p90_breath427":      "длинный выдох — прямой выключатель симпатики, рычаг блуждающего нерва",
    "p90_iam":            "возвращаешь подавленные качества — БЫТЬ, а не доказывать",
    "p90_breakfast":      "холин, цинк, холестерин — сырьё для тестостерона и энергии",
    "p90_metrics":        "цифры покажут, что реально наполняет, а что только кажется полезным",
    "p90_bodyscan":       "читаешь физику тела напрямую — до того, как ум перехватит чувство",
    "p90_pause":          "зазор между триггером и реакцией — твоя страховка под давлением",
    "p90_mine":           "отличаешь своё от чужого — не управляешь чужими состояниями как своими",
    "p90_squats":         "регулярная нагрузка без перегруза — кровоток, тонус, тестостерон",
    "p90_pushups":        "сила + раскрытие груди — работает заодно на грудной зажим",
    "p90_water":          "гидратация, вывод продуктов аутофагии, ровная энергия",
    "p90_emowheel":       "строишь словарь СВОИХ чувств — для других он есть, для себя пуст",
    "p90_writeoff":       "выжим губки — чтобы чужое и невыраженное не копилось в перегруз",
    "p90_letter":         "слово — твой канал разрядки, сильнее чем слёзы",
    "p90_magnesium":      "мягкий магний для нервной системы и глубокого сна",
    "p90_ashwagandha":    "снижает кортизол — под перегруженную нервную систему",
    "p90_hotshower":      "завершение дня, переход в парасимпатику, глубже спишь",
    "p90_reading":        "артикуляция и темп голоса — лечит «глотание» окончаний",
    "p90_camera":         "привыкание быть видимым — десенсибилизация страха признания",
    "p90_videoreflect":   "замыкаешь петлю: утром назвал тень — вечером ищешь её проявления",
    "p90_playing_i":      "подавленная игровая часть наконец выходит наружу",
    "p90_morningsupps":   "базовый набор для тестостерона, энергии, нервной системы",
    "p90_no_stack":       "оксид азота, кровенаполнение — механика эрекции по биохимии",
    "p90_herbs":          "женьшень на эрекцию (доказательно), мака на либидо",
    "p90_singing":        "голос + зажим горла/груди + разрядка стыда — одним заходом",
    "p90_dance":          "прямое ПРОЖИВАНИЕ теневых качеств телом, без интерпретации",
    "p90_fast24":         "регулярная разрядка инсулинорезистентности и лёгкая аутофагия",
    "p90_fast48":         "глубокая аутофагия и метаболическая перезагрузка раз в месяц",
    "p90_sublimation":    "конвертируешь сексуальный/творческий заряд в слово, а не в разрядку",
    "p90_sacral":         "выбор из тела, а не из головы — сакральная стратегия",
    "p90_realout1":       "первая сцена — выход из приватного в видимое",
    "p90_voiceload":      "голос под давлением — навык в реальной среде, не в тишине",
    "p90_realout2":       "узкая аудитория — промежуточный шаг между столом и публикой",
    "p90_audit":          "внутренняя чистота через ретроспективу видео — видишь дугу",
    "p90_realout3":       "публичный выход — финальная точка дуги приватное → узкое → публичное",
    "p90_shabbat":        "уровневый слой через слово — осмысление, не отключение",
}

# Шаблоны для прорыва (streak). Подставляем {short} — ядро смысла практики.
STREAK_TEMPLATES = {
    1:  "День 1 — старт. {short}. Самый сложный шаг уже сделан.",
    3:  "3 дня подряд. {short}. Это уже сдвиг — не пробуешь, делаешь.",
    5:  "5 дней. Это уже не эксперимент. Привычка прорастает — дальше легче, чем не делать.",
    7:  "Неделя. {short} — становится твоим, не задачей в списке.",
    10: "10 дней. Прошёл порог: {short}. Дальше тело само напоминает.",
}

# Шаблоны для пропусков (miss). Интонация «ты можешь / тело помнит / не упрёк».
MISS_TEMPLATES = {
    1:  "Один день — не провал, просто день. Ты можешь зайти сегодня: {short}.",
    3:  "3 дня тишины. Ты ничего не потерял — тело помнит. {short} — ждёт.",
    7:  "Неделя без. Возврат — это не «начать сначала», это один шаг туда, где остался. Ты можешь.",
    14: "Две недели. Ничего не пропало: {short}. Один заход вернёт ритм.",
    30: "Месяц. Тело ничего не забывает. Один день — и ты снова там, где остановился. Ты можешь.",
}


def _seed_motivations(c) -> int:
    """Идемпотентно генерирует 10 мотивашек на каждую практику из PRACTICE_CORES.
    Возвращает число вставленных строк. Безопасно вызывать однократно через флаг
    'seeded_motivations_v1' — таблица motivations не имеет UNIQUE на (practice_id,
    kind, value), поэтому повторный вызов БЕЗ флага создаст дубликаты."""
    now = datetime.now(TZ).isoformat()
    created = 0
    for pid, short in PRACTICE_CORES.items():
        # Проверяем что практика существует (FK с CASCADE) — на случай если её
        # удалили вручную из админки между сидингом практик и мотивашек.
        exists = c.execute("SELECT 1 FROM practices WHERE id=?", (pid,)).fetchone()
        if not exists:
            continue
        for days, tpl in STREAK_TEMPLATES.items():
            text = tpl.format(short=short)
            c.execute(
                """INSERT INTO motivations (practice_id, program_id, kind, value, text, created_at)
                   VALUES (?, NULL, 'streak', ?, ?, ?)""",
                (pid, days, text, now),
            )
            created += 1
        for days, tpl in MISS_TEMPLATES.items():
            text = tpl.format(short=short)
            c.execute(
                """INSERT INTO motivations (practice_id, program_id, kind, value, text, created_at)
                   VALUES (?, NULL, 'miss', ?, ?, ?)""",
                (pid, days, text, now),
            )
            created += 1
    return created


def seed_motivations_once():
    """Заливает мотивашки ровно один раз. Флаг 'seeded_motivations_v1' защищает
    от дублирования (в отличие от практик, где есть PRIMARY KEY)."""
    with db() as c:
        if c.execute("SELECT 1 FROM app_meta WHERE key='seeded_motivations_v1'").fetchone():
            return
        created = _seed_motivations(c)
        c.execute("INSERT OR REPLACE INTO app_meta (key, value) VALUES ('seeded_motivations_v1', ?)",
                  (datetime.now(TZ).isoformat(),))
        log.info("Мотивашки: создано %d записей (10 на практику × %d практик)",
                 created, created // 10 if created else 0)


# ─── PWA-ИКОНКИ ────────────────────────────────────────────────────────────
PWA_ICONS_DIR = Path("frontend")

def _generate_pwa_icons():
    """При старте рисует иконки через Pillow, если их ещё нет.
    Дизайн: амбра-фон + символ ✨ в центре. Maskable-вариант имеет safe-zone 80%.
    Не перезаписывает существующие — админ может заменить вручную."""
    targets = [
        ("icon-192.png", 192, False),
        ("icon-512.png", 512, False),
        ("icon-180.png", 180, False),       # apple-touch-icon
        ("icon-512-maskable.png", 512, True),
    ]
    if all((PWA_ICONS_DIR / name).exists() for name, _, _ in targets):
        return
    try:
        from PIL import Image, ImageDraw, ImageFont
    except Exception as e:
        log.warning("Pillow недоступен, иконки PWA не сгенерированы: %s", e)
        return

    bg = (212, 168, 87)        # #d4a857 amber
    fg = (26, 20, 16)          # #1a1410 тёмный
    symbol = "✨"

    # Подбираем шрифт с поддержкой эмодзи (если есть на системе).
    candidates = [
        "/usr/share/fonts/truetype/noto/NotoColorEmoji.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/Library/Fonts/Apple Color Emoji.ttc",
        "C:/Windows/Fonts/seguiemj.ttf",
        "C:/Windows/Fonts/arialbd.ttf",
    ]
    font_path = next((p for p in candidates if os.path.exists(p)), None)

    for name, size, maskable in targets:
        out = PWA_ICONS_DIR / name
        if out.exists():
            continue
        img = Image.new("RGB", (size, size), bg)
        draw = ImageDraw.Draw(img)
        # Maskable: safe-zone — оставляем 80% от площади (10% padding с каждой стороны).
        fz = int(size * (0.55 if maskable else 0.7))
        font = None
        if font_path:
            try:
                font = ImageFont.truetype(font_path, fz)
            except Exception:
                font = None
        if font is None:
            font = ImageFont.load_default()
        # Если у дефолтного шрифта нет эмодзи — используем «П» как фоллбек
        text = symbol
        try:
            bbox = draw.textbbox((0, 0), text, font=font)
            if bbox[2] - bbox[0] < 4 or bbox[3] - bbox[1] < 4:
                raise ValueError("symbol not rendered")
        except Exception:
            text = "П"
            try:
                font = ImageFont.truetype(font_path, fz) if font_path else ImageFont.load_default()
            except Exception:
                font = ImageFont.load_default()
            bbox = draw.textbbox((0, 0), text, font=font)
        w = bbox[2] - bbox[0]
        h = bbox[3] - bbox[1]
        x = (size - w) / 2 - bbox[0]
        y = (size - h) / 2 - bbox[1]
        draw.text((x, y), text, fill=fg, font=font)
        try:
            img.save(str(out), "PNG", optimize=True)
            log.info("PWA icon: %s (%dx%d)", name, size, size)
        except Exception as e:
            log.warning("Не сохранить иконку %s: %s", name, e)


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


# ─── СЕССИИ И SECRET_KEY ──────────────────────────────────────────────────
SESSION_COOKIE_NAME = "session"
SESSION_MAX_AGE_DAYS = 30
SECRET_KEY: str = ""
_session_serializer: Optional[URLSafeTimedSerializer] = None


def _init_secret():
    """Берёт SECRET_KEY из env, иначе генерит и сохраняет в app_meta.
    Запускается ПОСЛЕ init_db()."""
    global SECRET_KEY, _session_serializer
    env_key = os.environ.get("SECRET_KEY", "").strip()
    if env_key:
        SECRET_KEY = env_key
    else:
        with db() as c:
            r = c.execute("SELECT value FROM app_meta WHERE key='secret_key'").fetchone()
            if r and r["value"]:
                SECRET_KEY = r["value"]
            else:
                SECRET_KEY = secrets.token_hex(32)
                c.execute("INSERT OR REPLACE INTO app_meta (key, value) VALUES ('secret_key', ?)",
                          (SECRET_KEY,))
    _session_serializer = URLSafeTimedSerializer(SECRET_KEY, salt="session")


def _make_session_token(user_id: int) -> str:
    return _session_serializer.dumps({"uid": int(user_id)})


def _read_session_token(token: str) -> Optional[int]:
    if not token or _session_serializer is None:
        return None
    try:
        data = _session_serializer.loads(token, max_age=SESSION_MAX_AGE_DAYS * 86400)
        return int(data.get("uid"))
    except (BadSignature, SignatureExpired, ValueError, TypeError):
        return None


def _set_session_cookie(response: Response, user_id: int):
    token = _make_session_token(user_id)
    response.set_cookie(
        SESSION_COOKIE_NAME, token,
        max_age=SESSION_MAX_AGE_DAYS * 86400,
        httponly=True, samesite="lax",
        secure=BASE_URL.startswith("https://"),
        path="/",
    )


# ─── АУТЕНТИФИКАЦИЯ ───────────────────────────────────────────────────────
def _authenticate(init_data: str, session_token: str) -> Optional[dict]:
    """Возвращает {id, first_name, username, _method} либо None.
    Сначала пробует Telegram initData, потом cookie-сессию."""
    user = verify_init_data(init_data) if init_data else None
    if user:
        return {
            "id": user["id"],
            "first_name": user.get("first_name"),
            "username": user.get("username"),
            "language_code": user.get("language_code"),
            "_method": "telegram",
        }
    if session_token:
        uid = _read_session_token(session_token)
        if uid:
            with db() as c:
                row = c.execute(
                    "SELECT user_id, first_name, username FROM users WHERE user_id=?",
                    (uid,),
                ).fetchone()
            if row:
                return {
                    "id": row["user_id"],
                    "first_name": row["first_name"],
                    "username": row["username"],
                    "_method": "web",
                }
    return None


def require_user(init_data: str = "", session_token: str = "", tz_header: str = "") -> dict:
    user = _authenticate(init_data, session_token)
    if not user:
        # Dev-fallback: без BOT_TOKEN разрешаем 'dev:<id>'
        if not BOT_TOKEN and init_data and init_data.startswith("dev:"):
            return {"id": int(init_data[4:]), "first_name": "Dev", "username": "dev", "_method": "dev"}
        raise HTTPException(status_code=401, detail="Unauthorized")

    now = datetime.now(TZ).isoformat()
    auto_tz = tz_header if _safe_tz(tz_header) else None
    if user["_method"] == "telegram":
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
            c.execute(
                """INSERT OR IGNORE INTO identities
                   (user_id, provider, external_id, created_at)
                   VALUES (?, 'telegram', ?, ?)""",
                (user["id"], str(user["id"]), now),
            )
    elif user["_method"] == "web":
        with db() as c:
            if auto_tz:
                c.execute("UPDATE users SET last_seen=?, tz=COALESCE(tz, ?) WHERE user_id=?",
                          (now, auto_tz, user["id"]))
            else:
                c.execute("UPDATE users SET last_seen=? WHERE user_id=?", (now, user["id"]))
    return user


def require_admin(init_data: str = "", session_token: str = "", tz_header: str = "") -> dict:
    user = require_user(init_data, session_token, tz_header)
    if user["id"] not in ADMIN_IDS:
        raise HTTPException(status_code=403, detail="Admins only")
    return user


# ─── DEPENDS-ХЕЛПЕРЫ ──────────────────────────────────────────────────────
def current_user(
    request: Request,
    x_init_data: str = Header(default=""),
    x_user_tz: str = Header(default=""),
) -> dict:
    return require_user(x_init_data, request.cookies.get(SESSION_COOKIE_NAME, ""), x_user_tz)


def current_admin(
    request: Request,
    x_init_data: str = Header(default=""),
    x_user_tz: str = Header(default=""),
) -> dict:
    return require_admin(x_init_data, request.cookies.get(SESSION_COOKIE_NAME, ""), x_user_tz)


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
    type: Literal["binary", "count", "text", "photo", "video", "rounds", "timer"] = "binary"
    extras: list[Literal["text", "photo", "video"]] = Field(default_factory=list)
    target: Optional[int] = None
    unit: Optional[str] = ""
    rounds_count: Optional[int] = Field(None, ge=1, le=100)
    work_seconds: Optional[int] = Field(None, ge=1, le=3600)
    rest_seconds: Optional[int] = Field(None, ge=0, le=3600)
    # Если задан — каждый раунд со своим work/rest и (опц.) своим звуком.
    # Длина = эффективное число раундов; rounds_count/work_seconds/rest_seconds
    # тогда используются только для отображения краткой сводки.
    rounds_plan: list[dict] = Field(default_factory=list, max_length=100)
    icon: Optional[str] = "✨"
    palette: Optional[str] = "amber"
    media_url: Optional[str] = ""
    media_label: Optional[str] = ""
    # До 3 аудио (url + label), проигрываются по очереди при открытии практики.
    audios: list[dict] = Field(default_factory=list, max_length=3)
    photo: Optional[str] = None
    max_reminders: int = Field(3, ge=0, le=10)
    reminder_from: str = "08:00"
    reminder_to: str = "21:00"
    # Точные времена напоминаний: список "HH:MM". Если непустой — игнорируют
    # max_reminders и окно, шлём ровно в каждое время (с допуском в один тик).
    reminder_times: list[str] = Field(default_factory=list, max_length=20)
    active: bool = True
    catalog_hidden: bool = False
    category_ids: list[str] = Field(default_factory=list)
    tag_frequency: list[str] = Field(default_factory=list)   # ежедневная / утренняя / ...
    tag_activity:  list[str] = Field(default_factory=list)   # пассивная / активная / ...
    tag_inventory: list[str] = Field(default_factory=list)   # коврик / скакалка / ...
    tag_location:  list[str] = Field(default_factory=list)   # дом / улица / ...


class ProgramLevelIn(BaseModel):
    practice_id: str
    duration_days: int = Field(..., ge=1, le=365)


class ProgramIn(BaseModel):
    name: str
    description: Optional[str] = ""
    icon: Optional[str] = "🎯"
    palette: Optional[str] = "amber"
    on_miss: Literal["reset", "continue"] = "reset"
    visibility: Literal["public", "individual"] = "public"
    active: bool = True
    levels: list[ProgramLevelIn] = Field(default_factory=list)


class ProgramJoinIn(BaseModel):
    program_id: str


class MotivationIn(BaseModel):
    practice_id: Optional[str] = None
    program_id: Optional[str] = None
    kind: Literal["start", "streak", "miss"]
    value: int = Field(0, ge=0, le=10000)
    text: str = Field(..., min_length=1)


class MotivationBulkIn(BaseModel):
    practice_id: Optional[str] = None
    program_id: Optional[str] = None
    kind: Literal["start", "streak", "miss"]
    value: int = Field(0, ge=0, le=10000)
    texts: list[str] = Field(..., min_length=1)


class AssignmentIn(BaseModel):
    target_type: Literal["practice", "program"]
    target_id: str


class CategoryIn(BaseModel):
    name: str
    parent_id: Optional[str] = None
    icon: Optional[str] = ""
    sort_order: int = 0


class TelegramLoginIn(BaseModel):
    """Данные от Telegram Login Widget. Подпись проверяется HMAC-SHA256
    с ключом sha256(BOT_TOKEN). См. https://core.telegram.org/widgets/login#checking-authorization"""
    id: int
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    username: Optional[str] = None
    photo_url: Optional[str] = None
    auth_date: int
    hash: str


class JoinIn(BaseModel):
    practice_id: str
    period_type: Literal["week", "month", "forever"] = "month"


class EntryIn(BaseModel):
    practice_id: str
    date: Optional[str] = None      # YYYY-MM-DD, default — сегодня
    completed: Optional[bool] = None
    count: Optional[int] = None     # абсолютное значение, не дельта
    response_text: Optional[str] = None      # для type='text'
    response_photo: Optional[str] = None     # base64 data: URL или '/photos/...' для type='photo'
    response_video_url: Optional[str] = None # для type='video'


class SettingsIn(BaseModel):
    tz: Optional[str] = None             # IANA, например 'Europe/Moscow'
    mute_until: Optional[int] = None     # epoch seconds; 0/None = пауза снята


class ExtendIn(BaseModel):
    practice_id: str
    period_type: Optional[Literal["week", "month", "forever"]] = None  # default — текущий тип


class ReorderIn(BaseModel):
    # Список practice_id в нужном порядке (первый — самый приоритетный).
    order: list[str]


# ─── ВСПОМОГАТЕЛЬНЫЕ ──────────────────────────────────────────────────────
def today_str() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d")


def is_done(practice_row, entry_row) -> bool:
    if not entry_row:
        return False
    t = practice_row["type"]
    if t == "binary":
        return bool(entry_row["completed"])
    if t == "count":
        return (entry_row["count"] or 0) >= (practice_row["target"] or 1)
    if t == "text":
        return bool((entry_row["response_text"] or "").strip())
    if t == "photo":
        return bool(entry_row["response_photo"])
    if t == "video":
        return bool((entry_row["response_video_url"] or "").strip())
    if t == "rounds":
        return bool(entry_row["completed"])
    if t == "timer":
        return (entry_row["count"] or 0) >= (practice_row["target"] or 1)
    return False


# SQL-фрагмент для условия «день засчитан». Подставляется в WHERE.
# Требует, чтобы entries был под алиасом e, а practices — под p.
DONE_SQL = (
    "((p.type='binary' AND e.completed=1)"
    " OR (p.type='count'  AND e.count >= COALESCE(p.target,1))"
    " OR (p.type='text'   AND e.response_text IS NOT NULL AND TRIM(e.response_text) != '')"
    " OR (p.type='photo'  AND e.response_photo IS NOT NULL AND e.response_photo != '')"
    " OR (p.type='video'  AND e.response_video_url IS NOT NULL AND TRIM(e.response_video_url) != '')"
    " OR (p.type='rounds' AND e.completed=1)"
    " OR (p.type='timer'  AND e.count >= COALESCE(p.target,1)))"
)


def save_response_photo(value: Optional[str], user_id: int, practice_id: str,
                        date_iso: str) -> Optional[str]:
    """Принимает base64 data:image или существующий путь /photos/entries/...
    Возвращает финальный путь под /photos/entries/. Пустые значения → None."""
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
    entries_dir = PHOTOS_DIR / "entries"
    entries_dir.mkdir(parents=True, exist_ok=True)
    fname = f"{user_id}_{practice_id}_{date_iso}.{ext}"
    (entries_dir / fname).write_bytes(data)
    return f"/photos/entries/{fname}"


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


def _parse_extras(value) -> list:
    """Парсит CSV из БД в список. Фильтрует допустимые значения."""
    if not value:
        return []
    allowed = {"text", "photo", "video"}
    return [x.strip() for x in str(value).split(",") if x.strip() in allowed]


def _parse_tags(value) -> list:
    """Парсит CSV тегов из БД в список. Пустые элементы фильтруются."""
    if not value:
        return []
    return [x.strip() for x in str(value).split(",") if x.strip()]


def _parse_reminder_times(value) -> list:
    """CSV точных времён напоминаний из БД → отсортированный список ['HH:MM',...].
    Невалидные элементы отбрасываются."""
    if not value:
        return []
    out = []
    for raw in str(value).split(","):
        s = raw.strip()
        if not s:
            continue
        try:
            datetime.strptime(s, "%H:%M")
        except ValueError:
            continue
        out.append(s)
    return sorted(set(out))


def _reminder_times_to_csv(times: list) -> str:
    """Список ['HH:MM',...] → CSV. Невалидные/дубли отбрасываются, итог сортируется."""
    if not times:
        return ""
    out = []
    seen = set()
    for t in times:
        s = (str(t) or "").strip()
        if not s or s in seen:
            continue
        try:
            datetime.strptime(s, "%H:%M")
        except ValueError:
            continue
        seen.add(s)
        out.append(s)
    return ",".join(sorted(out))


def _parse_audios(value) -> list:
    """JSON-строка → список {url,label}, максимум 3 валидных элемента."""
    if not value:
        return []
    try:
        import json
        raw = json.loads(value)
    except Exception:
        return []
    if not isinstance(raw, list):
        return []
    out = []
    for item in raw[:3]:
        if not isinstance(item, dict):
            continue
        url = (item.get("url") or "").strip()
        if not url:
            continue
        out.append({"url": url[:1000], "label": (item.get("label") or "").strip()[:200]})
    return out


def _parse_rounds_plan(value) -> list:
    """JSON-строка → список раундов [{work, rest, sound_url}]. Максимум 100."""
    if not value:
        return []
    try:
        import json
        raw = json.loads(value)
    except Exception:
        return []
    if not isinstance(raw, list):
        return []
    out = []
    for item in raw[:100]:
        if not isinstance(item, dict):
            continue
        try:
            work = int(item.get("work", 0))
            rest = int(item.get("rest", 0))
        except Exception:
            continue
        if work < 1 or work > 3600 or rest < 0 or rest > 3600:
            continue
        out.append({
            "work": work,
            "rest": rest,
            "sound_url": (item.get("sound_url") or "").strip()[:1000],
            "note": (item.get("note") or "").strip()[:500],
        })
    return out


def _rounds_plan_to_json(plan: list) -> str:
    import json
    cleaned = []
    for item in (plan or [])[:100]:
        if not isinstance(item, dict):
            continue
        try:
            work = int(item.get("work", 0))
            rest = int(item.get("rest", 0))
        except Exception:
            continue
        if work < 1 or work > 3600 or rest < 0 or rest > 3600:
            continue
        cleaned.append({
            "work": work,
            "rest": rest,
            "sound_url": (item.get("sound_url") or "").strip()[:1000],
            "note": (item.get("note") or "").strip()[:500],
        })
    return json.dumps(cleaned, ensure_ascii=False) if cleaned else ""


def _audios_to_json(audios: list) -> str:
    import json
    cleaned = []
    for item in (audios or [])[:3]:
        if not isinstance(item, dict):
            continue
        url = (item.get("url") or "").strip()
        if not url:
            continue
        cleaned.append({"url": url[:1000], "label": (item.get("label") or "").strip()[:200]})
    return json.dumps(cleaned, ensure_ascii=False) if cleaned else ""


def _tags_to_csv(tags: list) -> str:
    """Список тегов → CSV. Чистит пустые и обрезает по 64 символа."""
    if not tags:
        return ""
    out = []
    seen = set()
    for t in tags:
        s = (str(t) or "").strip()[:64]
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return ",".join(out)


def practice_to_dict(row, category_ids: Optional[list] = None) -> dict:
    return {
        "id": row["id"],
        "name": row["name"],
        "description": row["description"] or "",
        "type": row["type"],
        "extras": _parse_extras(row["extras"]) if "extras" in row.keys() else [],
        "target": row["target"],
        "unit": row["unit"] or "",
        "rounds_count": row["rounds_count"] if "rounds_count" in row.keys() else None,
        "work_seconds": row["work_seconds"] if "work_seconds" in row.keys() else None,
        "rest_seconds": row["rest_seconds"] if "rest_seconds" in row.keys() else None,
        "rounds_plan": _parse_rounds_plan(row["rounds_plan"]) if "rounds_plan" in row.keys() else [],
        "icon": row["icon"] or "✨",
        "palette": row["palette"] or "amber",
        "media_url": row["media_url"] or "",
        "media_label": row["media_label"] or "",
        "audios": _parse_audios(row["audios"]) if "audios" in row.keys() else [],
        "photo": row["photo"],
        "max_reminders": row["max_reminders"],
        "reminder_from": row["reminder_from"],
        "reminder_to": row["reminder_to"],
        "reminder_times": _parse_reminder_times(row["reminder_times"]) if "reminder_times" in row.keys() else [],
        "active": bool(row["active"]),
        "catalog_hidden": bool(row["catalog_hidden"]) if "catalog_hidden" in row.keys() else False,
        "category_ids": category_ids or [],
        "tag_frequency": _parse_tags(row["tag_frequency"]) if "tag_frequency" in row.keys() else [],
        "tag_activity":  _parse_tags(row["tag_activity"])  if "tag_activity"  in row.keys() else [],
        "tag_inventory": _parse_tags(row["tag_inventory"]) if "tag_inventory" in row.keys() else [],
        "tag_location":  _parse_tags(row["tag_location"])  if "tag_location"  in row.keys() else [],
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
    return FileResponse("frontend/landing.html")


@app.get("/landing")
def landing_page():
    return FileResponse("frontend/landing.html")


@app.get("/app")
def app_page():
    return FileResponse("frontend/app.html")


@app.get("/admin")
def admin_page():
    return FileResponse("frontend/admin.html")


@app.get("/healthz")
def healthz():
    return {"ok": True, "ts": datetime.now(TZ).isoformat()}


# ─── PWA: manifest и service worker должны жить в корне (для scope=/) ────
@app.get("/manifest.webmanifest")
def pwa_manifest():
    return FileResponse("frontend/manifest.webmanifest", media_type="application/manifest+json")


@app.get("/sw.js")
def pwa_service_worker():
    """Отдаём sw.js из корня — иначе scope ограничится /static/ и SW
    не сможет перехватывать /app, /login и т. п."""
    return FileResponse("frontend/sw.js", media_type="application/javascript",
                        headers={"Service-Worker-Allowed": "/"})


# ─── API: Пользователь ────────────────────────────────────────────────────
@app.get("/api/me")
def me(user: dict = Depends(current_user)):
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
        "auth_method": user.get("_method", "telegram"),
    }


@app.get("/api/me/settings")
def get_settings(user: dict = Depends(current_user)):
    with db() as c:
        r = c.execute("SELECT tz, mute_until FROM users WHERE user_id=?", (user["id"],)).fetchone()
    return {
        "tz": (r["tz"] if r and r["tz"] else DEFAULT_TZ_NAME),
        "mute_until": (r["mute_until"] or 0) if r else 0,
    }


@app.put("/api/me/settings")
def update_settings(body: SettingsIn, user: dict = Depends(current_user)):
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
                   user: dict = Depends(current_user)):
    """Публичный каталог: только active=1.
    Скрытые (catalog_hidden=1) — только если назначены юзеру через user_assignments."""
    with db() as c:
        assigned_ids = {
            r["target_id"] for r in c.execute(
                "SELECT target_id FROM user_assignments WHERE user_id=? AND target_type='practice'",
                (user["id"],),
            ).fetchall()
        }
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
        rows = [r for r in rows if not r["catalog_hidden"] or r["id"] in assigned_ids]
        cat_map = _load_practice_categories(c, [r["id"] for r in rows])
    return [practice_to_dict(r, cat_map.get(r["id"], [])) for r in rows]


@app.get("/api/categories")
def list_categories(user: dict = Depends(current_user)):
    """Плоский список — фронт сам строит дерево по parent_id."""
    with db() as c:
        rows = c.execute(
            "SELECT * FROM categories ORDER BY level, sort_order, name"
        ).fetchall()
    return [dict(r) for r in rows]


@app.get("/api/my")
def my_data(user: dict = Depends(current_user)):
    """Полное состояние текущего юзера: активные практики + записи (60 дн.) +
    история подписок (для heatmap) + стрики (full/half/legacy any-done)."""
    with db() as c:
        tz = get_user_tz(user["id"], c)
        today_d = datetime.now(tz).date()
        today = today_d.isoformat()

        # Подвинуть прогресс программ юзера (переходы уровней, сбросы) — lazily, на каждом /api/my.
        _advance_user_programs(c, user["id"], today_d)

        # Бэкфилл: для каждой назначенной админом практики, на которую юзер ещё не подписан,
        # создаём подписку «без срока». Идемпотентно. Чинит старые назначения, сделанные до
        # авто-подписки в admin_create_assignment.
        c.execute(
            """INSERT INTO user_practices (user_id, practice_id, period_type, period_start,
                                           period_end, joined_at, period_end_notified)
               SELECT ua.user_id, ua.target_id, 'forever', ?, NULL, ?, 0
               FROM user_assignments ua
               JOIN practices p ON p.id = ua.target_id
               WHERE ua.user_id = ? AND ua.target_type = 'practice'
                 AND NOT EXISTS (
                   SELECT 1 FROM user_practices up
                   WHERE up.user_id = ua.user_id AND up.practice_id = ua.target_id
                 )""",
            (today, datetime.now(TZ).isoformat(), user["id"]),
        )

        ups = c.execute(
            """SELECT up.*, p.* FROM user_practices up
               JOIN practices p ON p.id = up.practice_id
               WHERE up.user_id = ?
                 AND (up.period_end IS NULL OR up.period_end >= ?)
               ORDER BY up.priority ASC, up.joined_at ASC""",
            (user["id"], today),
        ).fetchall()
        active_cat_map = _load_practice_categories(c, [r["id"] for r in ups])
        practices = []
        for r in ups:
            d = practice_to_dict(r, active_cat_map.get(r["id"], []))
            d["period_type"] = r["period_type"]
            d["period_start"] = r["period_start"]
            d["period_end"] = r["period_end"]
            d["priority"] = r["priority"] if "priority" in r.keys() else 0
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
                "response_text": e["response_text"],
                "response_photo": e["response_photo"],
                "response_video_url": e["response_video_url"],
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
            f"""SELECT e.practice_id, e.date FROM entries e
                JOIN practices p ON p.id = e.practice_id
                WHERE e.user_id = ? AND {DONE_SQL}""",
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

    # Подписки на программы (активные и завершённые) — для отображения на фронте.
    with db() as c:
        user_progs = c.execute(
            "SELECT program_id FROM user_programs WHERE user_id=? ORDER BY joined_at DESC",
            (user["id"],),
        ).fetchall()
        programs_state = [
            s for s in (_user_program_state(c, user["id"], r["program_id"]) for r in user_progs)
            if s is not None
        ]

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
        "programs": programs_state,
    }


def _period_end_for(period_type: str, start_d: date) -> Optional[date]:
    if period_type == "week":
        return start_d + timedelta(days=7)
    if period_type == "month":
        return start_d + timedelta(days=30)
    return None  # forever


# ─── ПРОГРАММЫ (многоуровневые практики) ───────────────────────────────────

def _program_levels(c, program_id: str) -> list[dict]:
    """Уровни программы по порядку. Возвращает список словарей с полями уровня + практики."""
    rows = c.execute(
        """SELECT pl.program_id, pl.level_order, pl.duration_days, pl.practice_id,
                  p.name AS practice_name, p.type AS practice_type, p.target AS practice_target,
                  p.icon AS practice_icon, p.palette AS practice_palette,
                  p.description AS practice_description, p.unit AS practice_unit
           FROM program_levels pl
           JOIN practices p ON p.id = pl.practice_id
           WHERE pl.program_id = ?
           ORDER BY pl.level_order""",
        (program_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def _practice_done_dates(c, user_id: int, practice_id: str,
                         from_d: date, to_d: date) -> set:
    """Вернёт set ISO-дат, когда юзер выполнил практику в диапазоне [from_d, to_d]."""
    if from_d > to_d:
        return set()
    p = c.execute("SELECT type, target FROM practices WHERE id=?", (practice_id,)).fetchone()
    if not p:
        return set()
    rows = c.execute(
        """SELECT date, completed, count FROM entries
           WHERE user_id=? AND practice_id=? AND date>=? AND date<=?""",
        (user_id, practice_id, from_d.isoformat(), to_d.isoformat()),
    ).fetchall()
    done = set()
    target = p["target"] or 1
    for r in rows:
        if p["type"] == "binary":
            if r["completed"]:
                done.add(r["date"])
        else:
            if (r["count"] or 0) >= target:
                done.add(r["date"])
    return done


def _sync_program_user_practice(c, user_id: int, practice_id: str, start_d: date):
    """При переходе на новый уровень — гарантируем активную подписку user_practices
    на практику этого уровня (period_type='forever', period_start=сегодня, без конца)."""
    c.execute(
        """INSERT INTO user_practices (user_id, practice_id, period_type, period_start,
                                       period_end, joined_at, period_end_notified)
           VALUES (?, ?, 'forever', ?, NULL, ?, 0)
           ON CONFLICT(user_id, practice_id) DO UPDATE SET
             period_type='forever',
             period_start=excluded.period_start,
             period_end=NULL,
             period_end_notified=0""",
        (user_id, practice_id, start_d.isoformat(), datetime.now(TZ).isoformat()),
    )


def _advance_user_programs(c, user_id: int, today_d: date) -> list[dict]:
    """Пересчитывает прогресс по всем активным программам юзера и применяет переходы уровней.
    Возвращает список событий: [{program_id, kind, ...}], которые могут стрельнуть мотивашки
    (пока используется только под этап 4 — на этапе 2 события не обрабатываются).

    Логика:
      - on_miss='reset': если в [level_started_at .. вчера] есть день без done — сброс уровня.
        Иначе level_completed_days = подряд_done_дней_включая_сегодня.
      - on_miss='continue': level_completed_days = общее_число_done_дней_в_уровне.
      - Если completed_days >= duration_days → переход на следующий уровень (level_started_at=сегодня).
      - Если уровней больше нет → status='completed'.
    """
    events: list[dict] = []
    ups = c.execute(
        """SELECT up.user_id, up.program_id, up.current_level, up.level_started_at,
                  up.level_completed_days, p.on_miss, p.name AS program_name
           FROM user_programs up
           JOIN programs p ON p.id = up.program_id
           WHERE up.user_id = ? AND up.status = 'active'""",
        (user_id,),
    ).fetchall()
    for r in ups:
        program_id = r["program_id"]
        on_miss = r["on_miss"] or "reset"
        current_level = r["current_level"]
        level_started = date.fromisoformat(r["level_started_at"])
        levels = _program_levels(c, program_id)
        if not levels:
            continue
        max_level = max(l["level_order"] for l in levels)
        # Может потребоваться несколько итераций (цепочка переходов в один день).
        guard = 0
        while guard < max_level + 2:
            guard += 1
            lvl = next((l for l in levels if l["level_order"] == current_level), None)
            if lvl is None:
                # уровня нет — программа завершена
                c.execute(
                    """UPDATE user_programs SET status='completed', completed_at=?
                       WHERE user_id=? AND program_id=?""",
                    (today_d.isoformat(), user_id, program_id),
                )
                events.append({"program_id": program_id, "kind": "program_completed"})
                break
            duration = lvl["duration_days"]
            practice_id = lvl["practice_id"]
            done = _practice_done_dates(c, user_id, practice_id, level_started, today_d)
            yesterday = today_d - timedelta(days=1)

            if on_miss == "reset":
                # Считаем подряд от level_started до вчера (закрытые дни)
                cur = level_started
                streak = 0
                broken = False
                while cur <= yesterday:
                    if cur.isoformat() in done:
                        streak += 1
                    else:
                        broken = True
                        break
                    cur += timedelta(days=1)
                if broken:
                    # Сброс: новый старт = сегодня
                    new_completed = 1 if today_d.isoformat() in done else 0
                    c.execute(
                        """UPDATE user_programs
                           SET level_started_at=?, level_completed_days=?
                           WHERE user_id=? AND program_id=?""",
                        (today_d.isoformat(), new_completed, user_id, program_id),
                    )
                    level_started = today_d
                    events.append({"program_id": program_id, "kind": "level_reset",
                                   "level": current_level})
                    break
                completed = streak + (1 if today_d.isoformat() in done else 0)
            else:  # continue: считаем все done-дни уровня (не обязательно подряд)
                completed = len(done)

            if completed >= duration:
                # Переход на следующий уровень
                current_level += 1
                level_started = today_d
                next_lvl = next((l for l in levels if l["level_order"] == current_level), None)
                if next_lvl is None:
                    c.execute(
                        """UPDATE user_programs
                           SET current_level=?, level_started_at=?, level_completed_days=0,
                               status='completed', completed_at=?
                           WHERE user_id=? AND program_id=?""",
                        (current_level, level_started.isoformat(),
                         today_d.isoformat(), user_id, program_id),
                    )
                    events.append({"program_id": program_id, "kind": "program_completed"})
                    break
                # Подписать юзера на новую практику-уровень
                _sync_program_user_practice(c, user_id, next_lvl["practice_id"], today_d)
                c.execute(
                    """UPDATE user_programs
                       SET current_level=?, level_started_at=?, level_completed_days=0
                       WHERE user_id=? AND program_id=?""",
                    (current_level, level_started.isoformat(), user_id, program_id),
                )
                events.append({"program_id": program_id, "kind": "level_up",
                               "level": current_level})
                # Повторим цикл — возможно сегодня уже отмечена новая практика
                continue

            # Без перехода — просто обновим level_completed_days
            c.execute(
                """UPDATE user_programs SET level_completed_days=?
                   WHERE user_id=? AND program_id=?""",
                (completed, user_id, program_id),
            )
            # Стрик-мотивашка программы: если есть мотивашка на текущее число completed
            if completed > 0:
                has = c.execute(
                    """SELECT 1 FROM motivations
                       WHERE program_id=? AND kind='streak' AND value=? LIMIT 1""",
                    (program_id, completed),
                ).fetchone()
                if has:
                    _send_motivation(c, user_id, "program", program_id,
                                     "streak", completed, today_d)
            break
    return events


def _user_program_state(c, user_id: int, program_id: str) -> Optional[dict]:
    """Снимок состояния юзерской программы для отдачи на фронт (после _advance)."""
    up = c.execute(
        """SELECT up.*, p.name, p.description, p.icon, p.palette, p.on_miss, p.visibility
           FROM user_programs up
           JOIN programs p ON p.id = up.program_id
           WHERE up.user_id=? AND up.program_id=?""",
        (user_id, program_id),
    ).fetchone()
    if not up:
        return None
    levels = _program_levels(c, program_id)
    cur_lvl = next((l for l in levels if l["level_order"] == up["current_level"]), None)
    return {
        "program_id": program_id,
        "name": up["name"],
        "description": up["description"] or "",
        "icon": up["icon"] or "🎯",
        "palette": up["palette"] or "amber",
        "on_miss": up["on_miss"],
        "status": up["status"],
        "current_level": up["current_level"],
        "level_started_at": up["level_started_at"],
        "level_completed_days": up["level_completed_days"],
        "completed_at": up["completed_at"],
        "joined_at": up["joined_at"],
        "total_levels": len(levels),
        "current_practice_id": cur_lvl["practice_id"] if cur_lvl else None,
        "current_practice_name": cur_lvl["practice_name"] if cur_lvl else None,
        "current_duration_days": cur_lvl["duration_days"] if cur_lvl else None,
        "levels": [{
            "level_order": l["level_order"],
            "practice_id": l["practice_id"],
            "practice_name": l["practice_name"],
            "duration_days": l["duration_days"],
        } for l in levels],
    }


def _program_to_dict(c, program_row, include_levels: bool = True) -> dict:
    d = {
        "id": program_row["id"],
        "name": program_row["name"],
        "description": program_row["description"] or "",
        "icon": program_row["icon"] or "🎯",
        "palette": program_row["palette"] or "amber",
        "on_miss": program_row["on_miss"],
        "visibility": program_row["visibility"],
        "active": bool(program_row["active"]),
        "created_at": program_row["created_at"],
    }
    if include_levels:
        levels = _program_levels(c, program_row["id"])
        d["levels"] = [{
            "level_order": l["level_order"],
            "practice_id": l["practice_id"],
            "practice_name": l["practice_name"],
            "practice_icon": l["practice_icon"] or "✨",
            "duration_days": l["duration_days"],
        } for l in levels]
        d["total_levels"] = len(levels)
    return d


def _send_motivation(c, user_id: int, scope_kind: str, scope_id: str,
                     kind: str, value: int, today_d: date) -> bool:
    """Подбирает случайную мотивашку и отправляет юзеру в Telegram.
    Дедуп — одна запись в motivations_sent на (user, scope, kind, value, дата).
    Возвращает True, если отправили; False — если нечего слать или уже слали.

    Вызывается из sync-роутов и фоновых задач. Bot.send_message планируется на
    главный event loop через run_coroutine_threadsafe."""
    # Дедуп: пробуем вставить — если конфликт, значит уже слали сегодня.
    today_iso = today_d.isoformat()
    try:
        c.execute(
            """INSERT INTO motivations_sent (user_id, scope_kind, scope_id, kind, value, sent_date)
               VALUES (?,?,?,?,?,?)""",
            (user_id, scope_kind, scope_id, kind, value, today_iso),
        )
    except sqlite3.IntegrityError:
        return False  # уже отправляли

    # Выбираем случайную мотивашку
    target_col = "practice_id" if scope_kind == "practice" else "program_id"
    rows = c.execute(
        f"""SELECT text FROM motivations
            WHERE {target_col}=? AND kind=? AND value=?""",
        (scope_id, kind, value),
    ).fetchall()
    if not rows:
        return False
    text = random.choice([r["text"] for r in rows])

    # Проверим, не на паузе ли юзер
    u = c.execute("SELECT mute_until FROM users WHERE user_id=?", (user_id,)).fetchone()
    if u and u["mute_until"] and u["mute_until"] > int(datetime.now(TZ).timestamp()):
        return False

    # Отправка — планируем на главный event loop
    if not bot or not MAIN_LOOP:
        return False
    try:
        asyncio.run_coroutine_threadsafe(
            bot.send_message(user_id, text, reply_markup=webapp_kb("Открыть")),
            MAIN_LOOP,
        )
    except Exception as e:
        log.warning("send motivation to %s failed: %s", user_id, e)
        return False
    return True


@app.post("/api/my/join")
def join_practice(body: JoinIn, user: dict = Depends(current_user)):
    with db() as c:
        today = user_today_d(user["id"], c)
        end = _period_end_for(body.period_type, today)
        practice = c.execute("SELECT id FROM practices WHERE id=? AND active=1", (body.practice_id,)).fetchone()
        if not practice:
            raise HTTPException(404, "Practice not found")
        existing = c.execute(
            "SELECT 1 FROM user_practices WHERE user_id=? AND practice_id=?",
            (user["id"], body.practice_id),
        ).fetchone()
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
        if not existing:
            _send_motivation(c, user["id"], "practice", body.practice_id, "start", 0, today)
    return {"ok": True}


@app.post("/api/my/extend")
def extend_practice(body: ExtendIn, user: dict = Depends(current_user)):
    """Продлить подписку на практику. По умолчанию — тем же сроком, что был.
    Новая дата конца = max(текущая_period_end, сегодня) + срок."""
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


@app.post("/api/my/practices/reorder")
def reorder_practices(body: ReorderIn, user: dict = Depends(current_user)):
    """Задаёт приоритет практик юзера — порядок отображения в «Сегодня».
    Первый id в списке = priority 0 (выше всех)."""
    with db() as c:
        for idx, pid in enumerate(body.order):
            c.execute(
                "UPDATE user_practices SET priority=? WHERE user_id=? AND practice_id=?",
                (idx, user["id"], pid),
            )
    return {"ok": True}


@app.delete("/api/my/leave/{practice_id}")
def leave_practice(practice_id: str, user: dict = Depends(current_user)):
    with db() as c:
        c.execute("DELETE FROM user_practices WHERE user_id=? AND practice_id=?", (user["id"], practice_id))
    return {"ok": True}


@app.post("/api/my/entry")
def upsert_entry(body: EntryIn, user: dict = Depends(current_user)):
    ts = int(datetime.now(TZ).timestamp())
    with db() as c:
        today_d = user_today_d(user["id"], c)
        target_date = body.date or today_d.isoformat()
        practice = c.execute("SELECT * FROM practices WHERE id=?", (body.practice_id,)).fetchone()
        if not practice:
            raise HTTPException(404, "Practice not found")

        # Для text/photo/video — прошлые дни заморожены, изменять можно только сегодня
        is_response_type = practice["type"] in ("text", "photo", "video")
        if is_response_type and target_date != today_d.isoformat():
            raise HTTPException(403, "Ответ можно дать только за сегодня")

        existing = c.execute(
            "SELECT * FROM entries WHERE user_id=? AND practice_id=? AND date=?",
            (user["id"], body.practice_id, target_date),
        ).fetchone()
        completed = int(body.completed) if body.completed is not None else (existing["completed"] if existing else 0)
        count = body.count if body.count is not None else (existing["count"] if existing else 0)
        # Для type='timer' храним лучший результат за день: новая попытка не должна
        # ухудшать рекорд (если только юзер не выставляет count=0 явно, чтобы сбросить).
        if practice["type"] == "timer" and body.count is not None and body.count > 0 and existing:
            count = max(int(body.count), int(existing["count"] or 0))

        # Поля ответа
        resp_text = body.response_text if body.response_text is not None \
                    else (existing["response_text"] if existing else None)
        resp_video = body.response_video_url if body.response_video_url is not None \
                     else (existing["response_video_url"] if existing else None)
        if body.response_photo is not None:
            # Пустая строка / None — снять фото
            resp_photo = save_response_photo(body.response_photo, user["id"],
                                              body.practice_id, target_date) \
                         if body.response_photo else None
        else:
            resp_photo = existing["response_photo"] if existing else None

        # Для response-типов завершённость определяется наличием ответа
        if is_response_type:
            if practice["type"] == "text":
                completed = 1 if (resp_text or "").strip() else 0
            elif practice["type"] == "photo":
                completed = 1 if resp_photo else 0
            elif practice["type"] == "video":
                completed = 1 if (resp_video or "").strip() else 0

        # Пустое всё — удаляем запись
        all_empty = (completed == 0 and count == 0
                     and not (resp_text or "").strip()
                     and not resp_photo
                     and not (resp_video or "").strip())
        if all_empty:
            c.execute("DELETE FROM entries WHERE user_id=? AND practice_id=? AND date=?",
                      (user["id"], body.practice_id, target_date))
        else:
            c.execute(
                """INSERT INTO entries (user_id, practice_id, date, completed, count,
                                        response_text, response_photo, response_video_url, ts)
                   VALUES (?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(user_id, practice_id, date) DO UPDATE SET
                     completed=excluded.completed, count=excluded.count,
                     response_text=excluded.response_text,
                     response_photo=excluded.response_photo,
                     response_video_url=excluded.response_video_url,
                     ts=excluded.ts""",
                (user["id"], body.practice_id, target_date, completed, count,
                 resp_text, resp_photo, resp_video, ts),
            )
        # Стрик-мотивашка для практики — только если запись закрывает сегодня и день засчитан
        if target_date == today_d.isoformat():
            t = practice["type"]
            target = practice["target"] or 1
            if t == "binary":
                day_counted = (completed == 1)
            elif t == "count":
                day_counted = (count >= target)
            elif t == "text":
                day_counted = bool((resp_text or "").strip())
            elif t == "photo":
                day_counted = bool(resp_photo)
            elif t == "video":
                day_counted = bool((resp_video or "").strip())
            elif t == "rounds":
                day_counted = (completed == 1)
            elif t == "timer":
                day_counted = (count >= target)
            else:
                day_counted = False
            if day_counted:
                _check_practice_streak_motivation(c, user["id"], body.practice_id, today_d)
        # Подвинуть прогресс программ — переход уровня может стрельнуть прямо сейчас.
        _advance_user_programs(c, user["id"], today_d)
    return {"ok": True}


def _check_practice_streak_motivation(c, user_id: int, practice_id: str, today_d: date):
    """Если у юзера есть мотивашка с kind='streak' value=N, и текущий стрик практики
    равен N (только что хитнули) — отправить. Дедуп по дате через motivations_sent."""
    done_rows = c.execute(
        f"""SELECT e.date FROM entries e
            JOIN practices p ON p.id = e.practice_id
            WHERE e.user_id = ? AND e.practice_id = ? AND {DONE_SQL}""",
        (user_id, practice_id),
    ).fetchall()
    cur_streak, _ = compute_streaks({r["date"] for r in done_rows}, today_d)
    if cur_streak <= 0:
        return
    # Есть ли мотивашка ровно на эту величину стрика?
    has = c.execute(
        """SELECT 1 FROM motivations
           WHERE practice_id=? AND kind='streak' AND value=? LIMIT 1""",
        (practice_id, cur_streak),
    ).fetchone()
    if has:
        _send_motivation(c, user_id, "practice", practice_id, "streak", cur_streak, today_d)


@app.get("/api/leaderboard")
def leaderboard(period: Literal["week", "month", "all"] = "month",
                user: dict = Depends(current_user)):
    """Рейтинг по проценту выполнения за период."""
    today = datetime.now(TZ).date()
    if period == "week":
        start = today - timedelta(days=7)
    elif period == "month":
        start = today - timedelta(days=30)
    else:
        start = today - timedelta(days=365)

    with db() as c:
        # для каждого юзера: сколько практико-дней должны были закрыть и сколько закрыли
        rows = c.execute(f"""
            SELECT u.user_id, u.first_name, u.username,
                   (SELECT COUNT(*) FROM entries e
                      JOIN practices p ON p.id = e.practice_id
                      WHERE e.user_id = u.user_id AND e.date >= ? AND e.date <= ?
                      AND {DONE_SQL}
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


# ─── API: Авторизация (Telegram Login Widget для веб-юзеров) ──────────────
def verify_telegram_login(data: dict) -> bool:
    """Проверяет HMAC-подпись от Telegram Login Widget. Алгоритм:
    secret = sha256(BOT_TOKEN), затем HMAC-SHA256 от data_check_string.
    https://core.telegram.org/widgets/login#checking-authorization"""
    if not BOT_TOKEN:
        return False
    received_hash = data.get("hash")
    if not received_hash:
        return False
    pairs = sorted(
        f"{k}={v}" for k, v in data.items()
        if k != "hash" and v is not None and v != ""
    )
    data_check = "\n".join(pairs)
    secret_key = hashlib.sha256(BOT_TOKEN.encode()).digest()
    calc = hmac.new(secret_key, data_check.encode(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(calc, received_hash)


# bot_username получаем при старте через bot.get_me()
BOT_USERNAME: str = ""


@app.get("/api/auth/config")
def auth_config():
    """Параметры для рендеринга Telegram Login Widget на клиенте."""
    return {"bot_username": BOT_USERNAME}


@app.post("/api/auth/telegram")
def auth_telegram(body: TelegramLoginIn, response: Response,
                  x_user_tz: str = Header(default="")):
    """Получает данные от Telegram Login Widget, проверяет подпись,
    создаёт/обновляет users и identities, ставит cookie-сессию."""
    data = body.model_dump(exclude_none=True)
    # Все поля для подписи — кроме hash. Конвертим всё в строки.
    check_data = {k: str(v) for k, v in data.items()}
    if not verify_telegram_login(check_data):
        raise HTTPException(401, "Подпись Telegram не сошлась")
    if int(datetime.now(TZ).timestamp()) - body.auth_date > 86400:
        raise HTTPException(401, "Данные авторизации устарели — открой /login заново")

    uid = body.id
    now = datetime.now(TZ).isoformat()
    auto_tz = x_user_tz if _safe_tz(x_user_tz) else None
    with db() as c:
        c.execute(
            """INSERT INTO users (user_id, username, first_name, language, created_at, last_seen, tz)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(user_id) DO UPDATE SET
                 username=excluded.username,
                 first_name=excluded.first_name,
                 last_seen=excluded.last_seen,
                 tz=COALESCE(users.tz, excluded.tz)""",
            (uid, body.username, body.first_name, None, now, now, auto_tz),
        )
        c.execute(
            """INSERT OR IGNORE INTO identities
               (user_id, provider, external_id, created_at)
               VALUES (?, 'telegram', ?, ?)""",
            (uid, str(uid), now),
        )
    _set_session_cookie(response, uid)
    return {"ok": True, "user_id": uid, "name": body.first_name or body.username}


@app.post("/api/auth/logout")
def auth_logout(response: Response):
    response.delete_cookie(SESSION_COOKIE_NAME, path="/")
    return {"ok": True}


@app.get("/login")
def login_page():
    return FileResponse("frontend/login.html")


# ─── API: Админка ──────────────────────────────────────────────────────────
@app.get("/api/admin/practices")
def admin_list(user: dict = Depends(current_admin)):
    with db() as c:
        rows = c.execute("SELECT * FROM practices ORDER BY created_at DESC").fetchall()
        cat_map = _load_practice_categories(c, [r["id"] for r in rows])
    return [practice_to_dict(r, cat_map.get(r["id"], [])) for r in rows]


@app.get("/api/admin/categories")
def admin_list_categories(user: dict = Depends(current_admin)):
    with db() as c:
        rows = c.execute(
            """SELECT cat.*,
                  (SELECT COUNT(*) FROM practice_categories pc WHERE pc.category_id = cat.id) AS practice_count
               FROM categories cat
               ORDER BY level, sort_order, name"""
        ).fetchall()
    return [dict(r) for r in rows]


@app.post("/api/admin/categories")
def admin_create_category(body: CategoryIn, user: dict = Depends(current_admin)):
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
def admin_update_category(cid: str, body: CategoryIn, user: dict = Depends(current_admin)):
    """Можно менять name, icon, sort_order. parent_id фиксирован после создания."""
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


AUDIO_DIR = PHOTOS_DIR / "audios"
AUDIO_EXT_BY_MIME = {
    "audio/mpeg": "mp3", "audio/mp3": "mp3",
    "audio/ogg": "ogg", "audio/wav": "wav", "audio/x-wav": "wav",
    "audio/aac": "aac", "audio/mp4": "m4a", "audio/x-m4a": "m4a",
    "audio/webm": "webm", "audio/flac": "flac",
}
ALLOWED_AUDIO_EXT = {"mp3", "ogg", "wav", "aac", "m4a", "webm", "flac"}
MAX_AUDIO_BYTES = 30 * 1024 * 1024  # 30 MB

VIDEO_DIR = PHOTOS_DIR / "videos"
VIDEO_EXT_BY_MIME = {
    "video/mp4": "mp4", "video/webm": "webm",
    "video/quicktime": "mov", "video/x-m4v": "m4v",
}
ALLOWED_VIDEO_EXT = {"mp4", "webm", "mov", "m4v"}
MAX_VIDEO_BYTES = 300 * 1024 * 1024  # 300 MB


def _video_ext_from_name(filename: str) -> Optional[str]:
    if not filename or "." not in filename:
        return None
    ext = filename.rsplit(".", 1)[-1].lower()
    return ext if ext in ALLOWED_VIDEO_EXT else None


def _audio_ext_from_name(filename: str) -> Optional[str]:
    if not filename or "." not in filename:
        return None
    ext = filename.rsplit(".", 1)[-1].lower()
    return ext if ext in ALLOWED_AUDIO_EXT else None


@app.post("/api/admin/audio/upload")
async def admin_audio_upload(file: UploadFile = File(...), user: dict = Depends(current_admin)):
    """Принимает аудио-файл, сохраняет на диск, возвращает {url}.
    Лимит 30 МБ, разрешённые форматы: mp3/ogg/wav/aac/m4a/webm/flac."""
    ext = AUDIO_EXT_BY_MIME.get((file.content_type or "").lower()) or _audio_ext_from_name(file.filename or "")
    if not ext:
        raise HTTPException(400, "Неподдерживаемый формат аудио")
    AUDIO_DIR.mkdir(parents=True, exist_ok=True)
    data = await file.read()
    if not data:
        raise HTTPException(400, "Пустой файл")
    if len(data) > MAX_AUDIO_BYTES:
        raise HTTPException(413, f"Файл слишком большой (>{MAX_AUDIO_BYTES // (1024*1024)} МБ)")
    fname = f"{secrets.token_hex(8)}.{ext}"
    (AUDIO_DIR / fname).write_bytes(data)
    return {"url": f"/photos/audios/{fname}", "label": (file.filename or "").rsplit(".", 1)[0][:200]}


class YandexImportIn(BaseModel):
    url: str


@app.post("/api/admin/audio/import_yandex")
def admin_audio_import_yandex(body: YandexImportIn, user: dict = Depends(current_admin)):
    """Резолвит публичную ссылку Яндекс.Диска через cloud-api, скачивает файл и сохраняет
    локально. Возвращает {url, label}. Работает только с публичными ссылками без пароля."""
    import urllib.request, urllib.parse, json as _json
    public_url = (body.url or "").strip()
    if not public_url.startswith(("http://", "https://")):
        raise HTTPException(400, "Ожидается публичная ссылка Яндекс.Диска")
    # 1) узнать прямой href и имя файла
    api = "https://cloud-api.yandex.net/v1/disk/public/resources/download?public_key=" + urllib.parse.quote(public_url, safe="")
    try:
        with urllib.request.urlopen(api, timeout=15) as r:
            meta = _json.loads(r.read().decode("utf-8"))
        href = meta.get("href")
        if not href:
            raise ValueError("no href")
    except Exception as e:
        raise HTTPException(400, f"Не удалось получить ссылку с Яндекс.Диска: {e}")
    # 2) узнать имя/тип через meta-эндпоинт (необязательно — но для расширения файла)
    name = ""
    try:
        meta_api = "https://cloud-api.yandex.net/v1/disk/public/resources?public_key=" + urllib.parse.quote(public_url, safe="")
        with urllib.request.urlopen(meta_api, timeout=15) as r:
            info = _json.loads(r.read().decode("utf-8"))
        name = info.get("name") or ""
    except Exception:
        pass
    ext = _audio_ext_from_name(name) or "mp3"
    # 3) скачать сам файл (с ограничением размера)
    try:
        req = urllib.request.Request(href, headers={"User-Agent": "practices-bot/1.0"})
        with urllib.request.urlopen(req, timeout=60) as r:
            data = r.read(MAX_AUDIO_BYTES + 1)
    except Exception as e:
        raise HTTPException(400, f"Не удалось скачать файл: {e}")
    if not data:
        raise HTTPException(400, "Файл пустой")
    if len(data) > MAX_AUDIO_BYTES:
        raise HTTPException(413, f"Файл слишком большой (>{MAX_AUDIO_BYTES // (1024*1024)} МБ)")
    AUDIO_DIR.mkdir(parents=True, exist_ok=True)
    fname = f"{secrets.token_hex(8)}.{ext}"
    (AUDIO_DIR / fname).write_bytes(data)
    return {"url": f"/photos/audios/{fname}", "label": (name.rsplit(".", 1)[0] if name else "")[:200]}


@app.post("/api/admin/video/upload")
async def admin_video_upload(file: UploadFile = File(...), user: dict = Depends(current_admin)):
    """Принимает видео, сохраняет в /photos/videos/. Лимит 300 МБ."""
    ext = VIDEO_EXT_BY_MIME.get((file.content_type or "").lower()) or _video_ext_from_name(file.filename or "")
    if not ext:
        raise HTTPException(400, "Неподдерживаемый формат видео")
    VIDEO_DIR.mkdir(parents=True, exist_ok=True)
    data = await file.read()
    if not data:
        raise HTTPException(400, "Пустой файл")
    if len(data) > MAX_VIDEO_BYTES:
        raise HTTPException(413, f"Файл слишком большой (>{MAX_VIDEO_BYTES // (1024*1024)} МБ)")
    fname = f"{secrets.token_hex(8)}.{ext}"
    (VIDEO_DIR / fname).write_bytes(data)
    return {"url": f"/photos/videos/{fname}", "label": (file.filename or "").rsplit(".", 1)[0][:200]}


@app.post("/api/admin/video/import_yandex")
def admin_video_import_yandex(body: YandexImportIn, user: dict = Depends(current_admin)):
    """Импорт публичной ссылки Я.Диска как видео: резолвит direct-href, скачивает, кладёт в /photos/videos/."""
    import urllib.request, urllib.parse, json as _json
    public_url = (body.url or "").strip()
    if not public_url.startswith(("http://", "https://")):
        raise HTTPException(400, "Ожидается публичная ссылка Яндекс.Диска")
    api = "https://cloud-api.yandex.net/v1/disk/public/resources/download?public_key=" + urllib.parse.quote(public_url, safe="")
    try:
        with urllib.request.urlopen(api, timeout=15) as r:
            meta = _json.loads(r.read().decode("utf-8"))
        href = meta.get("href")
        if not href:
            raise ValueError("no href")
    except Exception as e:
        raise HTTPException(400, f"Не удалось получить ссылку с Яндекс.Диска: {e}")
    name = ""
    try:
        meta_api = "https://cloud-api.yandex.net/v1/disk/public/resources?public_key=" + urllib.parse.quote(public_url, safe="")
        with urllib.request.urlopen(meta_api, timeout=15) as r:
            info = _json.loads(r.read().decode("utf-8"))
        name = info.get("name") or ""
    except Exception:
        pass
    ext = _video_ext_from_name(name) or "mp4"
    try:
        req = urllib.request.Request(href, headers={"User-Agent": "practices-bot/1.0"})
        with urllib.request.urlopen(req, timeout=180) as r:
            data = r.read(MAX_VIDEO_BYTES + 1)
    except Exception as e:
        raise HTTPException(400, f"Не удалось скачать видео: {e}")
    if not data:
        raise HTTPException(400, "Файл пустой")
    if len(data) > MAX_VIDEO_BYTES:
        raise HTTPException(413, f"Файл слишком большой (>{MAX_VIDEO_BYTES // (1024*1024)} МБ)")
    VIDEO_DIR.mkdir(parents=True, exist_ok=True)
    fname = f"{secrets.token_hex(8)}.{ext}"
    (VIDEO_DIR / fname).write_bytes(data)
    return {"url": f"/photos/videos/{fname}", "label": (name.rsplit(".", 1)[0] if name else "")[:200]}


@app.delete("/api/admin/categories/{cid}")
def admin_delete_category(cid: str, user: dict = Depends(current_admin)):
    """Каскадно удаляет потомков и связи с практиками (FK ON DELETE CASCADE)."""
    with db() as c:
        c.execute("DELETE FROM categories WHERE id=?", (cid,))
    return {"ok": True}


@app.post("/api/admin/seed_demo")
def admin_seed_demo(user: dict = Depends(current_admin)):
    """Принудительная заливка демо. Идемпотентно (INSERT OR IGNORE), флаг игнорируется."""
    with db() as c:
        created = _seed_demo(c)
        c.execute("INSERT OR REPLACE INTO app_meta (key, value) VALUES ('seeded_demo_v1', ?)",
                  (datetime.now(TZ).isoformat(),))
    return {"ok": True, "created_practices": created, "categories": len(DEMO_CATEGORIES)}


@app.get("/api/admin/stats")
def admin_stats(user: dict = Depends(current_admin)):
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
def admin_create(body: PracticeIn, user: dict = Depends(current_admin)):
    pid = "p_" + secrets.token_hex(6)
    photo_value = save_photo_from_input(body.photo, pid)
    # extras не может содержать сам primary type, чтобы не дублировать
    extras = [x for x in body.extras if x != body.type]
    extras_csv = ",".join(extras)
    with db() as c:
        c.execute(
            """INSERT INTO practices
               (id, name, description, type, extras, target, unit, icon, palette, media_url, media_label,
                audios, photo, max_reminders, reminder_from, reminder_to, reminder_times, active, catalog_hidden,
                tag_frequency, tag_activity, tag_inventory, tag_location,
                rounds_count, work_seconds, rest_seconds, rounds_plan,
                created_at, created_by)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (pid, body.name, body.description, body.type, extras_csv, body.target, body.unit, body.icon,
             body.palette, body.media_url, body.media_label, _audios_to_json(body.audios), photo_value,
             body.max_reminders, body.reminder_from, body.reminder_to,
             _reminder_times_to_csv(body.reminder_times),
             int(body.active), int(body.catalog_hidden),
             _tags_to_csv(body.tag_frequency), _tags_to_csv(body.tag_activity),
             _tags_to_csv(body.tag_inventory), _tags_to_csv(body.tag_location),
             body.rounds_count, body.work_seconds, body.rest_seconds, _rounds_plan_to_json(body.rounds_plan),
             datetime.now(TZ).isoformat(), user["id"]),
        )
        _save_practice_categories(c, pid, body.category_ids)
    return {"id": pid}


@app.put("/api/admin/practices/{pid}")
def admin_update(pid: str, body: PracticeIn, user: dict = Depends(current_admin)):
    photo_value = save_photo_from_input(body.photo, pid)
    with db() as c:
        existing = c.execute("SELECT id FROM practices WHERE id=?", (pid,)).fetchone()
        if not existing:
            raise HTTPException(404, "Not found")
        extras = [x for x in body.extras if x != body.type]
        extras_csv = ",".join(extras)
        c.execute(
            """UPDATE practices SET name=?, description=?, type=?, extras=?, target=?, unit=?, icon=?,
               palette=?, media_url=?, media_label=?, audios=?, photo=?, max_reminders=?,
               reminder_from=?, reminder_to=?, reminder_times=?, active=?, catalog_hidden=?,
               tag_frequency=?, tag_activity=?, tag_inventory=?, tag_location=?,
               rounds_count=?, work_seconds=?, rest_seconds=?, rounds_plan=?
               WHERE id=?""",
            (body.name, body.description, body.type, extras_csv, body.target, body.unit, body.icon,
             body.palette, body.media_url, body.media_label, _audios_to_json(body.audios), photo_value,
             body.max_reminders, body.reminder_from, body.reminder_to,
             _reminder_times_to_csv(body.reminder_times),
             int(body.active), int(body.catalog_hidden),
             _tags_to_csv(body.tag_frequency), _tags_to_csv(body.tag_activity),
             _tags_to_csv(body.tag_inventory), _tags_to_csv(body.tag_location),
             body.rounds_count, body.work_seconds, body.rest_seconds, _rounds_plan_to_json(body.rounds_plan),
             pid),
        )
        _save_practice_categories(c, pid, body.category_ids)
    return {"ok": True}


@app.delete("/api/admin/practices/{pid}")
def admin_delete(pid: str, user: dict = Depends(current_admin)):
    with db() as c:
        c.execute("DELETE FROM practices WHERE id=?", (pid,))
    return {"ok": True}


@app.get("/api/admin/users")
def admin_users(user: dict = Depends(current_admin)):
    with db() as c:
        rows = c.execute("""
            SELECT u.user_id, u.username, u.first_name, u.created_at, u.last_seen,
                   (SELECT COUNT(*) FROM user_practices up WHERE up.user_id = u.user_id
                    AND (up.period_end IS NULL OR up.period_end >= ?)) AS active_practices,
                   (SELECT COUNT(*) FROM entries e WHERE e.user_id = u.user_id) AS total_entries
            FROM users u ORDER BY u.last_seen DESC
        """, (today_str(),)).fetchall()
    return [dict(r) for r in rows]


@app.get("/api/admin/users/{uid}/history")
def admin_user_history(uid: int, user: dict = Depends(current_admin)):
    """Полная история юзера: подписки на практики со стриками, программы,
    последние 60 дней отметок."""
    with db() as c:
        u = c.execute(
            "SELECT user_id, username, first_name, tz, created_at, last_seen FROM users WHERE user_id=?",
            (uid,),
        ).fetchone()
        if not u:
            raise HTTPException(404, "Пользователь не найден")
        tz = _safe_tz(u["tz"]) or TZ
        today_d = datetime.now(tz).date()
        today_iso = today_d.isoformat()

        # Подписки на практики (активные и истёкшие)
        subs = c.execute(
            """SELECT up.practice_id, up.period_type, up.period_start, up.period_end, up.joined_at,
                      p.name, p.icon, p.palette, p.type, p.target, p.unit
               FROM user_practices up
               JOIN practices p ON p.id = up.practice_id
               WHERE up.user_id = ?
               ORDER BY up.joined_at DESC""",
            (uid,),
        ).fetchall()
        # Все done-даты по каждой практике для стриков и счётчика
        done_by_practice: dict = {}
        last_done_by_practice: dict = {}
        done_rows = c.execute(
            f"""SELECT e.practice_id, e.date FROM entries e
                JOIN practices p ON p.id = e.practice_id
                WHERE e.user_id = ? AND {DONE_SQL}""",
            (uid,),
        ).fetchall()
        for r in done_rows:
            done_by_practice.setdefault(r["practice_id"], set()).add(r["date"])
            prev = last_done_by_practice.get(r["practice_id"])
            if not prev or r["date"] > prev:
                last_done_by_practice[r["practice_id"]] = r["date"]

        practices = []
        for s in subs:
            pid = s["practice_id"]
            cur, best = compute_streaks(done_by_practice.get(pid, set()), today_d)
            is_active = (not s["period_end"]) or s["period_end"] >= today_iso
            practices.append({
                "id": pid,
                "name": s["name"],
                "icon": s["icon"] or "✨",
                "palette": s["palette"] or "amber",
                "type": s["type"],
                "target": s["target"],
                "unit": s["unit"] or "",
                "period_type": s["period_type"],
                "period_start": s["period_start"],
                "period_end": s["period_end"],
                "joined_at": s["joined_at"],
                "is_active": is_active,
                "current_streak": cur,
                "best_streak": best,
                "total_done_days": len(done_by_practice.get(pid, set())),
                "last_done_date": last_done_by_practice.get(pid),
            })

        # Программы (активные и завершённые)
        user_progs = c.execute(
            "SELECT program_id FROM user_programs WHERE user_id=? ORDER BY joined_at DESC",
            (uid,),
        ).fetchall()
        programs_state = [
            s for s in (_user_program_state(c, uid, r["program_id"]) for r in user_progs)
            if s is not None
        ]

        # Последние 60 дней отметок
        entries_rows = c.execute(
            """SELECT e.date, e.practice_id, e.completed, e.count,
                      e.response_text, e.response_photo, e.response_video_url, e.ts,
                      p.name AS practice_name, p.icon, p.type, p.target, p.unit
               FROM entries e
               JOIN practices p ON p.id = e.practice_id
               WHERE e.user_id = ? AND e.date >= date(?, '-60 days')
               ORDER BY e.date DESC, e.ts DESC""",
            (uid, today_iso),
        ).fetchall()
        entries = [{
            "date": r["date"],
            "practice_id": r["practice_id"],
            "practice_name": r["practice_name"],
            "practice_icon": r["icon"] or "✨",
            "practice_type": r["type"],
            "practice_target": r["target"],
            "practice_unit": r["unit"] or "",
            "completed": bool(r["completed"]),
            "count": r["count"] or 0,
            "response_text": r["response_text"],
            "response_photo": r["response_photo"],
            "response_video_url": r["response_video_url"],
            "ts": r["ts"],
        } for r in entries_rows]

    return {
        "user": dict(u),
        "today": today_iso,
        "practices": practices,
        "programs": programs_state,
        "entries": entries,
    }


# ─── ПРОГРАММЫ: API ────────────────────────────────────────────────────────

def _validate_program_levels(c, levels: list[ProgramLevelIn]):
    if not levels:
        raise HTTPException(400, "Программа должна содержать хотя бы один уровень")
    seen = set()
    for lvl in levels:
        if lvl.practice_id in seen:
            raise HTTPException(400, f"Практика {lvl.practice_id} указана в нескольких уровнях")
        seen.add(lvl.practice_id)
        p = c.execute("SELECT id FROM practices WHERE id=?", (lvl.practice_id,)).fetchone()
        if not p:
            raise HTTPException(404, f"Практика {lvl.practice_id} не найдена")


@app.get("/api/admin/programs")
def admin_list_programs(user: dict = Depends(current_admin)):
    with db() as c:
        rows = c.execute("SELECT * FROM programs ORDER BY created_at DESC").fetchall()
        return [_program_to_dict(c, r, include_levels=True) for r in rows]


@app.post("/api/admin/programs")
def admin_create_program(body: ProgramIn, user: dict = Depends(current_admin)):
    pid = "prog_" + secrets.token_hex(6)
    with db() as c:
        _validate_program_levels(c, body.levels)
        c.execute(
            """INSERT INTO programs (id, name, description, icon, palette, on_miss, visibility,
                                     active, created_at, created_by)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (pid, body.name, body.description or "", body.icon or "🎯",
             body.palette or "amber", body.on_miss, body.visibility,
             int(body.active), datetime.now(TZ).isoformat(), user["id"]),
        )
        for i, lvl in enumerate(body.levels, start=1):
            c.execute(
                """INSERT INTO program_levels (program_id, level_order, practice_id, duration_days)
                   VALUES (?,?,?,?)""",
                (pid, i, lvl.practice_id, lvl.duration_days),
            )
    return {"id": pid}


@app.put("/api/admin/programs/{prog_id}")
def admin_update_program(prog_id: str, body: ProgramIn, user: dict = Depends(current_admin)):
    with db() as c:
        existing = c.execute("SELECT id FROM programs WHERE id=?", (prog_id,)).fetchone()
        if not existing:
            raise HTTPException(404, "Программа не найдена")
        _validate_program_levels(c, body.levels)
        c.execute(
            """UPDATE programs SET name=?, description=?, icon=?, palette=?, on_miss=?,
                                   visibility=?, active=? WHERE id=?""",
            (body.name, body.description or "", body.icon or "🎯",
             body.palette or "amber", body.on_miss, body.visibility,
             int(body.active), prog_id),
        )
        # Полная перезапись уровней. Подписки юзеров (user_programs) не трогаем —
        # они продолжают указывать на свой current_level. Если уровень удалён —
        # _advance_user_programs увидит отсутствие и завершит программу.
        c.execute("DELETE FROM program_levels WHERE program_id=?", (prog_id,))
        for i, lvl in enumerate(body.levels, start=1):
            c.execute(
                """INSERT INTO program_levels (program_id, level_order, practice_id, duration_days)
                   VALUES (?,?,?,?)""",
                (prog_id, i, lvl.practice_id, lvl.duration_days),
            )
    return {"ok": True}


@app.delete("/api/admin/programs/{prog_id}")
def admin_delete_program(prog_id: str, user: dict = Depends(current_admin)):
    with db() as c:
        c.execute("DELETE FROM programs WHERE id=?", (prog_id,))
    return {"ok": True}


@app.get("/api/programs")
def list_programs(user: dict = Depends(current_user)):
    """Публичный каталог программ: только active=1.
    Программы visibility='individual' видны только тем, кому назначены."""
    with db() as c:
        assigned_ids = {
            r["target_id"] for r in c.execute(
                "SELECT target_id FROM user_assignments WHERE user_id=? AND target_type='program'",
                (user["id"],),
            ).fetchall()
        }
        rows = c.execute(
            "SELECT * FROM programs WHERE active=1 ORDER BY created_at DESC"
        ).fetchall()
        rows = [r for r in rows
                if r["visibility"] == "public" or r["id"] in assigned_ids]
        return [_program_to_dict(c, r, include_levels=True) for r in rows]


@app.post("/api/my/programs/join")
def join_program(body: ProgramJoinIn, user: dict = Depends(current_user)):
    with db() as c:
        prog = c.execute(
            "SELECT * FROM programs WHERE id=? AND active=1", (body.program_id,)
        ).fetchone()
        if not prog:
            raise HTTPException(404, "Программа не найдена")
        # Проверка доступа для individual-программ
        if prog["visibility"] == "individual":
            assigned = c.execute(
                """SELECT 1 FROM user_assignments
                   WHERE user_id=? AND target_type='program' AND target_id=?""",
                (user["id"], body.program_id),
            ).fetchone()
            if not assigned:
                raise HTTPException(403, "Программа не назначена этому пользователю")
        levels = _program_levels(c, body.program_id)
        if not levels:
            raise HTTPException(400, "В программе нет уровней")
        today_d = user_today_d(user["id"], c)
        first_practice = levels[0]["practice_id"]
        c.execute(
            """INSERT INTO user_programs (user_id, program_id, current_level, level_started_at,
                                          level_completed_days, status, joined_at)
               VALUES (?,?,1,?,0,'active',?)
               ON CONFLICT(user_id, program_id) DO UPDATE SET
                 current_level=1, level_started_at=excluded.level_started_at,
                 level_completed_days=0, status='active', completed_at=NULL""",
            (user["id"], body.program_id, today_d.isoformat(),
             datetime.now(TZ).isoformat()),
        )
        # Подписываем юзера на практику первого уровня
        _sync_program_user_practice(c, user["id"], first_practice, today_d)
        # Старт-мотивашка
        _send_motivation(c, user["id"], "program", body.program_id, "start", 0, today_d)
    return {"ok": True}


@app.delete("/api/my/programs/leave/{prog_id}")
def leave_program(prog_id: str, user: dict = Depends(current_user)):
    with db() as c:
        c.execute("DELETE FROM user_programs WHERE user_id=? AND program_id=?",
                  (user["id"], prog_id))
    return {"ok": True}


# ─── МОТИВАШКИ: API ────────────────────────────────────────────────────────

def _validate_motivation_target(c, practice_id: Optional[str], program_id: Optional[str]):
    if bool(practice_id) == bool(program_id):
        raise HTTPException(400, "Укажи ровно одно: practice_id или program_id")
    if practice_id:
        if not c.execute("SELECT 1 FROM practices WHERE id=?", (practice_id,)).fetchone():
            raise HTTPException(404, "Практика не найдена")
    else:
        if not c.execute("SELECT 1 FROM programs WHERE id=?", (program_id,)).fetchone():
            raise HTTPException(404, "Программа не найдена")


@app.get("/api/admin/motivations")
def admin_list_motivations(practice_id: Optional[str] = None,
                           program_id: Optional[str] = None,
                           user: dict = Depends(current_admin)):
    with db() as c:
        if practice_id:
            rows = c.execute(
                "SELECT * FROM motivations WHERE practice_id=? ORDER BY kind, value, id",
                (practice_id,),
            ).fetchall()
        elif program_id:
            rows = c.execute(
                "SELECT * FROM motivations WHERE program_id=? ORDER BY kind, value, id",
                (program_id,),
            ).fetchall()
        else:
            rows = c.execute("SELECT * FROM motivations ORDER BY kind, value, id").fetchall()
    return [dict(r) for r in rows]


@app.post("/api/admin/motivations")
def admin_create_motivation(body: MotivationIn, user: dict = Depends(current_admin)):
    text = (body.text or "").strip()
    if not text:
        raise HTTPException(400, "Текст мотивашки не может быть пустым")
    with db() as c:
        _validate_motivation_target(c, body.practice_id, body.program_id)
        cur = c.execute(
            """INSERT INTO motivations (practice_id, program_id, kind, value, text, created_at)
               VALUES (?,?,?,?,?,?)""",
            (body.practice_id, body.program_id, body.kind, body.value, text,
             datetime.now(TZ).isoformat()),
        )
    return {"id": cur.lastrowid}


@app.post("/api/admin/motivations/bulk")
def admin_create_motivations_bulk(body: MotivationBulkIn, user: dict = Depends(current_admin)):
    """Массовое добавление мотивашек одного типа (одна kind+value, много текстов)."""
    texts = [t.strip() for t in body.texts if t and t.strip()]
    if not texts:
        raise HTTPException(400, "Нет ни одного непустого текста")
    with db() as c:
        _validate_motivation_target(c, body.practice_id, body.program_id)
        now_iso = datetime.now(TZ).isoformat()
        c.executemany(
            """INSERT INTO motivations (practice_id, program_id, kind, value, text, created_at)
               VALUES (?,?,?,?,?,?)""",
            [(body.practice_id, body.program_id, body.kind, body.value, t, now_iso)
             for t in texts],
        )
    return {"ok": True, "created": len(texts)}


@app.put("/api/admin/motivations/{mid}")
def admin_update_motivation(mid: int, body: MotivationIn, user: dict = Depends(current_admin)):
    """Редактирование мотивашки: текст, тип триггера (start/streak/miss), число дней.
    Цель (practice_id/program_id) тоже можно сменить — на случай если мотивашка
    привязана не туда. Валидация — та же, что и при создании."""
    text = (body.text or "").strip()
    if not text:
        raise HTTPException(400, "Текст мотивашки не может быть пустым")
    with db() as c:
        existing = c.execute("SELECT 1 FROM motivations WHERE id=?", (mid,)).fetchone()
        if not existing:
            raise HTTPException(404, "Мотивашка не найдена")
        _validate_motivation_target(c, body.practice_id, body.program_id)
        c.execute(
            """UPDATE motivations
               SET practice_id=?, program_id=?, kind=?, value=?, text=?
               WHERE id=?""",
            (body.practice_id, body.program_id, body.kind, body.value, text, mid),
        )
    return {"ok": True}


@app.delete("/api/admin/motivations/{mid}")
def admin_delete_motivation(mid: int, user: dict = Depends(current_admin)):
    with db() as c:
        c.execute("DELETE FROM motivations WHERE id=?", (mid,))
    return {"ok": True}


# ─── ИНДИВИДУАЛЬНЫЕ НАЗНАЧЕНИЯ ─────────────────────────────────────────────

@app.get("/api/admin/users/{uid}/assignments")
def admin_list_assignments(uid: int, user: dict = Depends(current_admin)):
    with db() as c:
        rows = c.execute(
            """SELECT ua.target_type, ua.target_id, ua.assigned_at,
                      CASE ua.target_type
                           WHEN 'practice' THEN (SELECT name FROM practices WHERE id=ua.target_id)
                           WHEN 'program'  THEN (SELECT name FROM programs  WHERE id=ua.target_id)
                      END AS target_name
               FROM user_assignments ua
               WHERE ua.user_id = ?
               ORDER BY ua.assigned_at DESC""",
            (uid,),
        ).fetchall()
    return [dict(r) for r in rows]


@app.post("/api/admin/users/{uid}/assignments")
def admin_create_assignment(uid: int, body: AssignmentIn, user: dict = Depends(current_admin)):
    with db() as c:
        if not c.execute("SELECT 1 FROM users WHERE user_id=?", (uid,)).fetchone():
            raise HTTPException(404, "Пользователь не найден")
        if body.target_type == "practice":
            ok = c.execute("SELECT 1 FROM practices WHERE id=?", (body.target_id,)).fetchone()
        else:
            ok = c.execute("SELECT 1 FROM programs WHERE id=?", (body.target_id,)).fetchone()
        if not ok:
            raise HTTPException(404, f"{body.target_type} не найден(а)")
        c.execute(
            """INSERT INTO user_assignments (user_id, target_type, target_id, assigned_at, assigned_by)
               VALUES (?,?,?,?,?)
               ON CONFLICT(user_id, target_type, target_id) DO NOTHING""",
            (uid, body.target_type, body.target_id, datetime.now(TZ).isoformat(), user["id"]),
        )
        # Авто-подписка, чтобы практика/программа сразу появилась у юзера в «Сегодня».
        today_d = user_today_d(uid, c)
        if body.target_type == "practice":
            existed = c.execute(
                "SELECT 1 FROM user_practices WHERE user_id=? AND practice_id=?",
                (uid, body.target_id),
            ).fetchone()
            c.execute(
                """INSERT INTO user_practices (user_id, practice_id, period_type, period_start,
                                               period_end, joined_at, period_end_notified)
                   VALUES (?,?,'forever',?,NULL,?,0)
                   ON CONFLICT(user_id, practice_id) DO NOTHING""",
                (uid, body.target_id, today_d.isoformat(), datetime.now(TZ).isoformat()),
            )
            if not existed:
                _send_motivation(c, uid, "practice", body.target_id, "start", 0, today_d)
        elif body.target_type == "program":
            levels = _program_levels(c, body.target_id)
            if levels:
                existed = c.execute(
                    "SELECT 1 FROM user_programs WHERE user_id=? AND program_id=?",
                    (uid, body.target_id),
                ).fetchone()
                c.execute(
                    """INSERT INTO user_programs (user_id, program_id, current_level, level_started_at,
                                                  level_completed_days, status, joined_at)
                       VALUES (?,?,1,?,0,'active',?)
                       ON CONFLICT(user_id, program_id) DO NOTHING""",
                    (uid, body.target_id, today_d.isoformat(), datetime.now(TZ).isoformat()),
                )
                _sync_program_user_practice(c, uid, levels[0]["practice_id"], today_d)
                if not existed:
                    _send_motivation(c, uid, "program", body.target_id, "start", 0, today_d)
    return {"ok": True}


@app.delete("/api/admin/users/{uid}/assignments/{target_type}/{target_id}")
def admin_delete_assignment(uid: int, target_type: str, target_id: str,
                            user: dict = Depends(current_admin)):
    if target_type not in ("practice", "program"):
        raise HTTPException(400, "target_type должен быть 'practice' или 'program'")
    with db() as c:
        c.execute(
            """DELETE FROM user_assignments
               WHERE user_id=? AND target_type=? AND target_id=?""",
            (uid, target_type, target_id),
        )
    return {"ok": True}


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
    """Запускается каждые 5 минут. Решает, кому и про что напомнить.
    Два режима выбора срабатывания:
      • Точные времена (reminder_times задан) — шлём ровно в каждое перечисленное
        время. count в reminders_sent работает как индекс «сколько уже отправлено
        сегодня», следующее срабатывание = когда now >= reminder_times[count].
      • Окно — max_reminders пингов размазаны по [reminder_from..to] с
        минимальным гэпом max(30, window/max_r) минут.

    Группировка: за один тик все «созревшие» практики одного юзера объединяются
    в ОДНО сообщение (не 15 отдельных). reminders_sent обновляется для каждой
    практики из группы — счётчики идут параллельно."""
    if not bot:
        return
    server_now_ts = int(datetime.now(TZ).timestamp())

    with db() as c:
        # Берём все активные подписки + tz/mute_until юзера. Дата «сегодня» считается per-user ниже.
        rows = c.execute("""
            SELECT up.user_id, u.tz, u.mute_until,
                   p.id AS practice_id, p.name, p.type, p.target,
                   p.max_reminders, p.reminder_from, p.reminder_to, p.reminder_times,
                   up.period_end
            FROM user_practices up
            JOIN practices p ON p.id = up.practice_id
            JOIN users u ON u.user_id = up.user_id
            WHERE p.active=1
        """).fetchall()

    # Первый проход: собираем что нужно отправить, по юзерам.
    pending = {}  # uid -> list of dicts (что показать)
    user_today = {}  # uid -> "YYYY-MM-DD" по его TZ (для UPDATE reminders_sent)

    for r in rows:
        uid = r["user_id"]
        if r["mute_until"] and r["mute_until"] > server_now_ts:
            continue
        tz = _safe_tz(r["tz"]) or TZ
        u_now = datetime.now(tz)
        u_today = u_now.strftime("%Y-%m-%d")
        u_cur_t = u_now.time()
        if r["period_end"] and r["period_end"] < u_today:
            continue

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

        # ── Режим «точные времена» ─────────────────────────────────────
        exact_times = _parse_reminder_times(r["reminder_times"])
        if exact_times:
            if sent >= len(exact_times):
                continue
            try:
                next_t = datetime.strptime(exact_times[sent], "%H:%M").time()
            except ValueError:
                continue
            if u_cur_t < next_t:
                continue
            total_count = len(exact_times)
        else:
            # ── Режим «окно» ───────────────────────────────────────────
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
            total_count = max_r

        done_cnt = (done_row["count"] if done_row else 0) or 0
        pending.setdefault(uid, []).append({
            "practice_id": r["practice_id"],
            "name": r["name"],
            "type": r["type"],
            "target": r["target"],
            "done_cnt": done_cnt,
            "sent": sent,
            "total_count": total_count,
        })
        user_today[uid] = u_today

    # Второй проход: одно сообщение на юзера, обновление reminders_sent пачкой.
    sent_now = 0
    for uid, items in pending.items():
        u_today = user_today[uid]
        try:
            if len(items) == 1:
                it = items[0]
                text = f"⏰ Напомню про практику: <b>{it['name']}</b>"
                if it["type"] == "count":
                    text += f"\nПрогресс: {it['done_cnt']}/{it['target']}"
                text += f"\n\nНапоминание {it['sent'] + 1} из {it['total_count']} на сегодня."
            else:
                lines = [f"⏰ Сейчас по плану <b>{len(items)} практик</b>:"]
                for it in items:
                    line = f"• {it['name']}"
                    if it["type"] == "count":
                        line += f" — {it['done_cnt']}/{it['target']}"
                    lines.append(line)
                lines.append("")
                lines.append("Открой приложение и отметь, что сделал.")
                text = "\n".join(lines)
            await bot.send_message(uid, text, parse_mode="HTML", reply_markup=webapp_kb("Открыть"))
            with db() as c:
                for it in items:
                    c.execute(
                        """INSERT INTO reminders_sent (user_id, practice_id, date, count, last_at)
                           VALUES (?,?,?,1,?)
                           ON CONFLICT(user_id, practice_id, date) DO UPDATE SET
                             count = count + 1, last_at = excluded.last_at""",
                        (uid, it["practice_id"], u_today, server_now_ts),
                    )
            sent_now += len(items)
        except Exception as e:
            log.warning("send to %s failed: %s", uid, e)
            if "blocked" in str(e).lower() or "Forbidden" in str(e):
                with db() as c:
                    c.execute("DELETE FROM user_practices WHERE user_id=?", (uid,))
    if sent_now:
        log.info("Sent %d reminders across %d users", sent_now, len(pending))


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


async def miss_motivations_tick():
    """Раз в день: для каждой активной подписки юзера считаем «дней пропуска подряд»
    и шлём мотивашку kind='miss' если это число совпадает с одной из value."""
    if not bot:
        return
    with db() as c:
        miss_rows = c.execute(
            "SELECT practice_id, program_id, value FROM motivations WHERE kind='miss'"
        ).fetchall()
        practice_miss: dict = {}
        program_miss: dict = {}
        for r in miss_rows:
            if r["practice_id"]:
                practice_miss.setdefault(r["practice_id"], set()).add(r["value"])
            elif r["program_id"]:
                program_miss.setdefault(r["program_id"], set()).add(r["value"])
        if not practice_miss and not program_miss:
            return

        # Подписки на практики — для практик из practice_miss
        if practice_miss:
            placeholders = ",".join("?" * len(practice_miss))
            pids = list(practice_miss.keys())
            ups = c.execute(
                f"""SELECT up.user_id, up.practice_id, up.period_start, u.tz
                    FROM user_practices up
                    JOIN users u ON u.user_id = up.user_id
                    WHERE up.practice_id IN ({placeholders})
                      AND (up.period_end IS NULL OR up.period_end >= date('now'))""",
                pids,
            ).fetchall()
            for r in ups:
                _maybe_send_miss(c, r["user_id"], "practice", r["practice_id"],
                                 r["practice_id"],
                                 date.fromisoformat(r["period_start"]),
                                 _safe_tz(r["tz"]) or TZ,
                                 practice_miss[r["practice_id"]])

        # Подписки на программы — для каждой активной берём практику текущего уровня
        if program_miss:
            placeholders = ",".join("?" * len(program_miss))
            ups = c.execute(
                f"""SELECT up.user_id, up.program_id, up.current_level, up.level_started_at, u.tz
                    FROM user_programs up
                    JOIN users u ON u.user_id = up.user_id
                    WHERE up.status='active' AND up.program_id IN ({placeholders})""",
                list(program_miss.keys()),
            ).fetchall()
            for r in ups:
                lvl = c.execute(
                    "SELECT practice_id FROM program_levels WHERE program_id=? AND level_order=?",
                    (r["program_id"], r["current_level"]),
                ).fetchone()
                if not lvl:
                    continue
                _maybe_send_miss(c, r["user_id"], "program", r["program_id"],
                                 lvl["practice_id"],
                                 date.fromisoformat(r["level_started_at"]),
                                 _safe_tz(r["tz"]) or TZ,
                                 program_miss[r["program_id"]])


def _maybe_send_miss(c, user_id: int, scope_kind: str, scope_id: str,
                     tracked_practice_id: str, start_d: date, tz: ZoneInfo,
                     target_values: set):
    """Считает дней пропуска подряд (с последнего done) и шлёт, если попали в value."""
    today_d = datetime.now(tz).date()
    last_done = c.execute(
        f"""SELECT MAX(e.date) AS d FROM entries e
            JOIN practices p ON p.id = e.practice_id
            WHERE e.user_id=? AND e.practice_id=? AND {DONE_SQL}""",
        (user_id, tracked_practice_id),
    ).fetchone()
    if last_done and last_done["d"]:
        base_d = date.fromisoformat(last_done["d"])
    else:
        base_d = start_d - timedelta(days=1)  # ни разу не делал — считаем со дня до старта
    days_miss = (today_d - base_d).days - 1  # сегодня не считаем (день ещё может быть закрыт)
    if days_miss <= 0:
        return
    if days_miss in target_values:
        _send_motivation(c, user_id, scope_kind, scope_id, "miss", days_miss, today_d)


# ─── ЗАПУСК ────────────────────────────────────────────────────────────────
scheduler: Optional[AsyncIOScheduler] = None
MAIN_LOOP: Optional[asyncio.AbstractEventLoop] = None  # для отправки сообщений из sync-роутов


@app.on_event("startup")
async def on_startup():
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_running_loop()
    init_db()
    _init_secret()
    seed_demo_once()
    seed_personal_once()
    seed_personal_90d_once()
    seed_motivations_once()
    Path("frontend").mkdir(exist_ok=True)
    _generate_pwa_icons()
    log.info("DB ready at %s", DB_PATH.absolute())
    log.info("Photos at %s", PHOTOS_DIR.absolute())
    log.info("BASE_URL = %s", BASE_URL)
    log.info("Admins   = %s", ADMIN_IDS or "(none)")

    global scheduler, BOT_USERNAME
    if bot:
        # Получаем username бота — нужен для рендеринга Telegram Login Widget на /login
        try:
            me_bot = await bot.get_me()
            BOT_USERNAME = me_bot.username or ""
            log.info("Bot username = @%s", BOT_USERNAME)
        except Exception as e:
            log.warning("Не удалось получить bot username: %s", e)

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

        # Планировщик: напоминания каждые 5 мин (для точности «точных времён»
        # ±2.5 мин), ежедневная проверка периодов.
        scheduler = AsyncIOScheduler(timezone=TZ)
        scheduler.add_job(reminders_tick, "interval", minutes=5,
                          next_run_time=datetime.now(TZ) + timedelta(seconds=30))
        scheduler.add_job(period_check_tick, "cron", hour=10, minute=0,
                          next_run_time=datetime.now(TZ) + timedelta(seconds=60))
        # Мотивашки по пропускам — раз в день в 11:00 по серверному TZ
        scheduler.add_job(miss_motivations_tick, "cron", hour=11, minute=0)
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
