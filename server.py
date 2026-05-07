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

from aiogram import Bot, Dispatcher
from aiogram.types import (
    Message, InlineKeyboardButton, InlineKeyboardMarkup,
    WebAppInfo, MenuButtonWebApp, BotCommand,
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
  last_seen    TEXT
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
  photo         TEXT,                  -- base64 data-url
  max_reminders INTEGER DEFAULT 3,     -- сколько раз в день напоминать
  reminder_from TEXT DEFAULT '08:00',  -- окно напоминаний начало
  reminder_to   TEXT DEFAULT '21:00',  -- окно напоминаний конец
  active        INTEGER DEFAULT 1,
  created_at    TEXT NOT NULL,
  created_by    INTEGER
);

CREATE TABLE IF NOT EXISTS user_practices (
  user_id      INTEGER NOT NULL,
  practice_id  TEXT NOT NULL,
  period_type  TEXT NOT NULL,         -- 'week' | 'month' | 'forever'
  period_start TEXT NOT NULL,         -- YYYY-MM-DD
  period_end   TEXT,                  -- YYYY-MM-DD or NULL for forever
  joined_at    TEXT NOT NULL,
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

CREATE INDEX IF NOT EXISTS idx_entries_date ON entries(date);
CREATE INDEX IF NOT EXISTS idx_user_practices_user ON user_practices(user_id);
"""


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


def init_db():
    with db() as c:
        c.executescript(SCHEMA)


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


def require_user(init_data: str) -> dict:
    user = verify_init_data(init_data)
    if not user:
        # В dev-режиме без BOT_TOKEN допускаем заголовок X-Dev-User-Id
        if not BOT_TOKEN and init_data and init_data.startswith("dev:"):
            return {"id": int(init_data[4:]), "first_name": "Dev", "username": "dev"}
        raise HTTPException(status_code=401, detail="Unauthorized")
    # Touch user record
    now = datetime.now(TZ).isoformat()
    with db() as c:
        c.execute(
            """INSERT INTO users (user_id, username, first_name, language, created_at, last_seen)
               VALUES (?,?,?,?,?,?)
               ON CONFLICT(user_id) DO UPDATE SET
                 username=excluded.username,
                 first_name=excluded.first_name,
                 language=excluded.language,
                 last_seen=excluded.last_seen""",
            (user["id"], user.get("username"), user.get("first_name"),
             user.get("language_code"), now, now),
        )
    return user


def require_admin(init_data: str) -> dict:
    user = require_user(init_data)
    if user["id"] not in ADMIN_IDS:
        raise HTTPException(status_code=403, detail="Admins only")
    return user


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


class JoinIn(BaseModel):
    practice_id: str
    period_type: Literal["week", "month", "forever"] = "month"


class EntryIn(BaseModel):
    practice_id: str
    date: Optional[str] = None      # YYYY-MM-DD, default — сегодня
    completed: Optional[bool] = None
    count: Optional[int] = None     # абсолютное значение, не дельта


# ─── ВСПОМОГАТЕЛЬНЫЕ ──────────────────────────────────────────────────────
def today_str() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d")


def is_done(practice_row, entry_row) -> bool:
    if not entry_row:
        return False
    if practice_row["type"] == "binary":
        return bool(entry_row["completed"])
    return (entry_row["count"] or 0) >= (practice_row["target"] or 1)


def practice_to_dict(row) -> dict:
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
    }


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
def me(x_init_data: str = Header(default="")):
    user = require_user(x_init_data)
    return {"id": user["id"], "name": user.get("first_name"), "is_admin": user["id"] in ADMIN_IDS}


@app.get("/api/practices")
def list_practices(x_init_data: str = Header(default="")):
    require_user(x_init_data)
    with db() as c:
        rows = c.execute("SELECT * FROM practices WHERE active=1 ORDER BY created_at DESC").fetchall()
    return [practice_to_dict(r) for r in rows]


@app.get("/api/my")
def my_data(x_init_data: str = Header(default="")):
    """Полное состояние текущего юзера: его практики + записи."""
    user = require_user(x_init_data)
    today = today_str()
    with db() as c:
        ups = c.execute(
            """SELECT up.*, p.* FROM user_practices up
               JOIN practices p ON p.id = up.practice_id
               WHERE up.user_id = ?
                 AND (up.period_end IS NULL OR up.period_end >= ?)""",
            (user["id"], today),
        ).fetchall()
        practices = []
        for r in ups:
            d = practice_to_dict(r)
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
    return {"practices": practices, "entries": entries}


@app.post("/api/my/join")
def join_practice(body: JoinIn, x_init_data: str = Header(default="")):
    user = require_user(x_init_data)
    today = datetime.now(TZ).date()
    if body.period_type == "week":
        end = today + timedelta(days=7)
    elif body.period_type == "month":
        end = today + timedelta(days=30)
    else:
        end = None
    with db() as c:
        practice = c.execute("SELECT id FROM practices WHERE id=? AND active=1", (body.practice_id,)).fetchone()
        if not practice:
            raise HTTPException(404, "Practice not found")
        c.execute(
            """INSERT INTO user_practices (user_id, practice_id, period_type, period_start, period_end, joined_at)
               VALUES (?,?,?,?,?,?)
               ON CONFLICT(user_id, practice_id) DO UPDATE SET
                 period_type=excluded.period_type,
                 period_start=excluded.period_start,
                 period_end=excluded.period_end""",
            (user["id"], body.practice_id, body.period_type,
             today.isoformat(), end.isoformat() if end else None,
             datetime.now(TZ).isoformat()),
        )
    return {"ok": True}


@app.delete("/api/my/leave/{practice_id}")
def leave_practice(practice_id: str, x_init_data: str = Header(default="")):
    user = require_user(x_init_data)
    with db() as c:
        c.execute("DELETE FROM user_practices WHERE user_id=? AND practice_id=?", (user["id"], practice_id))
    return {"ok": True}


@app.post("/api/my/entry")
def upsert_entry(body: EntryIn, x_init_data: str = Header(default="")):
    user = require_user(x_init_data)
    target_date = body.date or today_str()
    ts = int(datetime.now(TZ).timestamp())
    with db() as c:
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
    return [practice_to_dict(r) for r in rows]


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
    with db() as c:
        c.execute(
            """INSERT INTO practices
               (id, name, description, type, target, unit, icon, palette, media_url, media_label,
                photo, max_reminders, reminder_from, reminder_to, active, created_at, created_by)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (pid, body.name, body.description, body.type, body.target, body.unit, body.icon,
             body.palette, body.media_url, body.media_label, body.photo,
             body.max_reminders, body.reminder_from, body.reminder_to,
             int(body.active), datetime.now(TZ).isoformat(), user["id"]),
        )
    return {"id": pid}


@app.put("/api/admin/practices/{pid}")
def admin_update(pid: str, body: PracticeIn, x_init_data: str = Header(default="")):
    require_admin(x_init_data)
    with db() as c:
        existing = c.execute("SELECT id FROM practices WHERE id=?", (pid,)).fetchone()
        if not existing:
            raise HTTPException(404, "Not found")
        c.execute(
            """UPDATE practices SET name=?, description=?, type=?, target=?, unit=?, icon=?,
               palette=?, media_url=?, media_label=?, photo=?, max_reminders=?,
               reminder_from=?, reminder_to=?, active=? WHERE id=?""",
            (body.name, body.description, body.type, body.target, body.unit, body.icon,
             body.palette, body.media_url, body.media_label, body.photo,
             body.max_reminders, body.reminder_from, body.reminder_to,
             int(body.active), pid),
        )
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
    text += "Команды:\n/open — открыть\n/today — что сегодня\n/stop — отключить напоминания"
    await msg.answer(text, reply_markup=webapp_kb())


@dp.message(Command("open"))
async def on_open(msg: Message):
    await msg.answer("Открой приложение:", reply_markup=webapp_kb())


@dp.message(Command("today"))
async def on_today(msg: Message):
    today = today_str()
    with db() as c:
        rows = c.execute("""
            SELECT p.name, p.type, p.target,
                   (SELECT completed FROM entries WHERE user_id=? AND practice_id=p.id AND date=?) AS done,
                   (SELECT count FROM entries WHERE user_id=? AND practice_id=p.id AND date=?) AS cnt
            FROM user_practices up JOIN practices p ON p.id = up.practice_id
            WHERE up.user_id=? AND (up.period_end IS NULL OR up.period_end >= ?)
            ORDER BY p.name
        """, (msg.from_user.id, today, msg.from_user.id, today, msg.from_user.id, today)).fetchall()
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
    # Сейчас «стоп» = удаление всех активных подписок на практики
    with db() as c:
        c.execute("DELETE FROM user_practices WHERE user_id=?", (msg.from_user.id,))
    await msg.answer("Все подписки на практики сняты — напоминаний больше не будет. "
                     "Чтобы вернуться, открой приложение и присоединись заново.",
                     reply_markup=webapp_kb())


# ─── ПЛАНИРОВЩИК НАПОМИНАНИЙ ──────────────────────────────────────────────
async def reminders_tick():
    """Запускается каждые 30 минут. Шлёт напоминания тем, кто ещё не закрыл практику сегодня."""
    if not bot:
        return
    now = datetime.now(TZ)
    today = now.strftime("%Y-%m-%d")
    cur_t = now.time()

    with db() as c:
        # все активные подписки + информация о практике + сегодняшняя запись + сколько раз уже напоминали
        rows = c.execute("""
            SELECT up.user_id, p.id AS practice_id, p.name, p.type, p.target,
                   p.max_reminders, p.reminder_from, p.reminder_to,
                   (SELECT completed FROM entries WHERE user_id=up.user_id AND practice_id=p.id AND date=?) AS done,
                   (SELECT count FROM entries WHERE user_id=up.user_id AND practice_id=p.id AND date=?) AS cnt,
                   (SELECT count FROM reminders_sent WHERE user_id=up.user_id AND practice_id=p.id AND date=?) AS sent,
                   (SELECT last_at FROM reminders_sent WHERE user_id=up.user_id AND practice_id=p.id AND date=?) AS last_at
            FROM user_practices up JOIN practices p ON p.id = up.practice_id
            WHERE p.active=1
              AND (up.period_end IS NULL OR up.period_end >= ?)
        """, (today, today, today, today, today)).fetchall()

    sent_now = 0
    for r in rows:
        # уже выполнено?
        if r["type"] == "binary" and r["done"]:
            continue
        if r["type"] == "count" and (r["cnt"] or 0) >= (r["target"] or 1):
            continue
        # лимит напоминаний?
        sent = r["sent"] or 0
        max_r = r["max_reminders"] or 0
        if sent >= max_r or max_r == 0:
            continue
        # окно времени?
        try:
            t_from = datetime.strptime(r["reminder_from"] or "08:00", "%H:%M").time()
            t_to = datetime.strptime(r["reminder_to"] or "21:00", "%H:%M").time()
        except Exception:
            t_from, t_to = time(8, 0), time(21, 0)
        if not (t_from <= cur_t <= t_to):
            continue
        # минимальный промежуток между напоминаниями: окно / max_reminders
        window_min = (datetime.combine(date.today(), t_to) - datetime.combine(date.today(), t_from)).total_seconds() / 60
        min_gap_min = max(30, int(window_min / max_r))
        if r["last_at"] and (now.timestamp() - r["last_at"]) < min_gap_min * 60:
            continue

        # отправляем
        try:
            text = f"⏰ Напомню про практику: <b>{r['name']}</b>"
            if r["type"] == "count":
                done_cnt = r["cnt"] or 0
                text += f"\nПрогресс: {done_cnt}/{r['target']}"
            text += f"\n\nНапоминание {sent + 1} из {max_r} на сегодня."
            await bot.send_message(r["user_id"], text, parse_mode="HTML", reply_markup=webapp_kb("Открыть"))
            with db() as c:
                c.execute(
                    """INSERT INTO reminders_sent (user_id, practice_id, date, count, last_at)
                       VALUES (?,?,?,1,?)
                       ON CONFLICT(user_id, practice_id, date) DO UPDATE SET
                         count = count + 1, last_at = excluded.last_at""",
                    (r["user_id"], r["practice_id"], today, int(now.timestamp())),
                )
            sent_now += 1
        except Exception as e:
            log.warning("send to %s failed: %s", r["user_id"], e)
            # если бот заблокирован — тихо снимаем подписки
            if "blocked" in str(e).lower() or "Forbidden" in str(e):
                with db() as c:
                    c.execute("DELETE FROM user_practices WHERE user_id=?", (r["user_id"],))
    if sent_now:
        log.info("Sent %d reminders", sent_now)


# ─── ЗАПУСК ────────────────────────────────────────────────────────────────
scheduler: Optional[AsyncIOScheduler] = None


@app.on_event("startup")
async def on_startup():
    init_db()
    Path("frontend").mkdir(exist_ok=True)
    log.info("DB ready at %s", DB_PATH.absolute())
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
                BotCommand(command="stop", description="Отключить напоминания"),
            ])
            await bot.set_chat_menu_button(
                menu_button=MenuButtonWebApp(text="Практики", web_app=WebAppInfo(url=f"{BASE_URL}/app"))
            )
        except Exception as e:
            log.warning("Bot setup warning: %s", e)

        # Параллельно крутим polling бота
        asyncio.create_task(dp.start_polling(bot))

        # Планировщик каждые 30 минут
        scheduler = AsyncIOScheduler(timezone=TZ)
        scheduler.add_job(reminders_tick, "interval", minutes=30, next_run_time=datetime.now(TZ) + timedelta(seconds=30))
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=PORT, reload=False)
