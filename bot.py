import os
import re
import io
import asyncio
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import quote_plus, unquote_plus
import logging
import uuid
import csv
from datetime import datetime, timedelta, timezone

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import (
    Message,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQueryResultArticle,
    InputTextMessageContent,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiosqlite import connect
from dotenv import load_dotenv


class EditState(StatesGroup):
    waiting_for_value = State()


# ================= INIT =================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_PATH = os.getenv("DB_PATH", "./data.db")
ADMIN_USERS = os.getenv("ADMIN_USERS", "")
ALLOWED_USERS = os.getenv("ALLOWED_USERS", "")

if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN not set")


def parse_ids(s: str) -> set:
    if not s:
        return set()
    return {int(re.sub(r"\D", "", x)) for x in s.split(",") if re.sub(r"\D", "", x)}


ADMIN_IDS = parse_ids(ADMIN_USERS)
ALLOWED_USER_IDS = parse_ids(ALLOWED_USERS)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

PAGE_SIZE = 5

# ================= SEARCH CONTEXT STORAGE =================
SEARCH_CONTEXT: Dict[str, Dict[str, Any]] = {}

# OWNER choice context (fix BUTTON_DATA_INVALID)
OWNER_CONTEXT: Dict[str, Dict[str, Any]] = {}


def create_search_session(query: str, page: int) -> str:
    session_id = str(uuid.uuid4())
    SEARCH_CONTEXT[session_id] = {"query": query, "page": page}
    return session_id


def owner_create_session(candidates: List[str]) -> str:
    sid = uuid.uuid4().hex  # short, safe for callback_data
    OWNER_CONTEXT[sid] = {"candidates": candidates, "created_at": dt_to_iso(utcnow())}
    return sid


def owner_get_candidate(sid: str, idx: int) -> Optional[str]:
    ctx = OWNER_CONTEXT.get(sid)
    if not ctx:
        return None
    items = ctx.get("candidates") or []
    if idx < 0 or idx >= len(items):
        return None
    return items[idx]


# ================= UTIL =================

EN_TO_RU = {
    "A": "А", "a": "а",
    "B": "В", "b": "в",
    "C": "С", "c": "с",
    "E": "Е", "e": "е",
    "H": "Н", "h": "н",
    "K": "К", "k": "к",
    "M": "М", "m": "м",
    "O": "О", "o": "о",
    "P": "Р", "p": "р",
    "T": "Т", "t": "т",
    "X": "Х", "x": "х",
    "Y": "У", "y": "у",
}


def digits_only(s: Optional[str]) -> str:
    return re.sub(r"\D", "", s or "")


def convert_en_to_ru(text: str) -> str:
    return "".join(EN_TO_RU.get(ch, ch) for ch in (text or ""))


def capitalize_words(text: str) -> str:
    words = (text or "").split()
    out = []
    for w in words:
        out.append(w[0].upper() + w[1:].lower() if w else w)
    return " ".join(out)


def build_cbdata(action: str, **kwargs) -> str:
    parts = [action]
    for k, v in kwargs.items():
        parts.append(f"{k}={quote_plus(str(v))}")
    return "|".join(parts)


def parse_cbdata(data: str) -> Tuple[str, Dict[str, str]]:
    parts = data.split("|")
    action = parts[0]
    payload: Dict[str, str] = {}
    for p in parts[1:]:
        if "=" in p:
            k, v = p.split("=", 1)
            payload[k] = unquote_plus(v)
    return action, payload


# ================= DB =================

async def db_execute_fetch(sql: str, params: tuple = (), many: bool = False):
    async with connect(DB_PATH, timeout=10.0) as db:
        db.row_factory = lambda c, r: {col[0]: r[idx] for idx, col in enumerate(c.description)}
        async with db.execute(sql, params) as cur:
            if sql.strip().upper().startswith("SELECT"):
                return await cur.fetchall() if many else await cur.fetchone()
            await db.commit()
            return cur.lastrowid


async def update_people_field(rec_id: int, field: str, value: str) -> bool:
    if field not in EDITABLE_FIELDS:
        return False
    await db_execute_fetch(
        f"UPDATE people SET {field} = ? WHERE id = ?",
        (value, rec_id),
    )
    return True


# ================= SQLITE PRAGMAS (Raspberry Pi / systemd) =================

async def db_init_pragmas():
    async with connect(DB_PATH, timeout=10.0) as db:
        await db.execute("PRAGMA busy_timeout=20000;")
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA synchronous=NORMAL;")
        await db.commit()


async def migrate_people_table():
    """Add note column and drop latitude/longitude/photo_url from people table."""
    columns_to_drop = ("latitude", "longitude", "photo_url")
    async with connect(DB_PATH, timeout=10.0) as db:
        try:
            await db.execute("ALTER TABLE people ADD COLUMN note TEXT")
            await db.commit()
            logger.info("Migration: added 'note' column to people")
        except Exception as e:
            logger.debug(f"Migration: 'note' column already exists or error: {e}")

        for col in columns_to_drop:
            try:
                await db.execute(f"ALTER TABLE people DROP COLUMN {col}")
                await db.commit()
                logger.info(f"Migration: dropped '{col}' column from people")
            except Exception as e:
                logger.debug(f"Migration: could not drop '{col}' column: {e}")


# ================= ACCESS REQUEST + TEMP 24H =================

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def dt_to_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def iso_to_dt(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


async def ensure_temp_allowed_table():
    await db_execute_fetch(
        """
        CREATE TABLE IF NOT EXISTS temp_allowed_users (
            user_id INTEGER PRIMARY KEY,
            added_by INTEGER,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL
        )
        """
    )


async def set_temp_allowed_24h(user_id: int, added_by: int) -> datetime:
    now = utcnow()
    expires = now + timedelta(hours=24)
    await db_execute_fetch(
        """
        INSERT INTO temp_allowed_users (user_id, added_by, created_at, expires_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
          added_by=excluded.added_by,
          created_at=excluded.created_at,
          expires_at=excluded.expires_at
        """,
        (user_id, added_by, dt_to_iso(now), dt_to_iso(expires)),
    )
    return expires


async def remove_temp_allowed(user_id: int):
    await db_execute_fetch("DELETE FROM temp_allowed_users WHERE user_id=?", (user_id,))


async def is_temp_allowed(user_id: int) -> bool:
    await ensure_temp_allowed_table()
    row = await db_execute_fetch("SELECT expires_at FROM temp_allowed_users WHERE user_id=?", (user_id,))
    if not row:
        return False
    try:
        exp = iso_to_dt(row["expires_at"])
    except Exception:
        await remove_temp_allowed(user_id)
        return False
    if exp <= utcnow():
        await remove_temp_allowed(user_id)
        return False
    return True


async def list_temp_allowed_active(limit: int = 500) -> List[Dict[str, Any]]:
    await ensure_temp_allowed_table()
    rows = await db_execute_fetch(
        "SELECT user_id, added_by, created_at, expires_at FROM temp_allowed_users ORDER BY expires_at LIMIT ?",
        (limit,),
        many=True,
    )

    out: List[Dict[str, Any]] = []
    for r in rows or []:
        try:
            exp = iso_to_dt(r["expires_at"])
        except Exception:
            await remove_temp_allowed(int(r["user_id"]))
            continue
        if exp <= utcnow():
            await remove_temp_allowed(int(r["user_id"]))
            continue
        out.append(r)
    return out


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


async def is_user_allowed(user_id: int) -> bool:
    if user_id in ADMIN_IDS:
        return True
    if user_id in ALLOWED_USER_IDS:
        return True
    return await is_temp_allowed(user_id)


# ================= UNIQUE USERS + LOG + RATE LIMIT / BLOCK =================

async def ensure_users_tables():
    await db_execute_fetch(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            full_name TEXT,
            username TEXT
        )
        """
    )
    await db_execute_fetch(
        """
        CREATE TABLE IF NOT EXISTS user_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            event TEXT NOT NULL,
            full_name TEXT,
            username TEXT
        )
        """
    )
    await db_execute_fetch(
        """
        CREATE TABLE IF NOT EXISTS user_blocks (
            user_id INTEGER PRIMARY KEY,
            blocked_until TEXT NOT NULL,
            reason TEXT
        )
        """
    )


async def upsert_user_seen(user: types.User):
    await ensure_users_tables()
    now = dt_to_iso(utcnow())
    full_name = (user.full_name or "").strip()
    username = (user.username or "").strip()

    await db_execute_fetch(
        """
        INSERT INTO users (user_id, first_seen_at, last_seen_at, full_name, username)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
          last_seen_at=excluded.last_seen_at,
          full_name=excluded.full_name,
          username=excluded.username
        """,
        (user.id, now, now, full_name, username),
    )


async def add_user_log(user: types.User, event: str):
    await ensure_users_tables()
    now = dt_to_iso(utcnow())
    full_name = (user.full_name or "").strip()
    username = (user.username or "").strip()
    await db_execute_fetch(
        """
        INSERT INTO user_log (created_at, user_id, event, full_name, username)
        VALUES (?, ?, ?, ?, ?)
        """,
        (now, user.id, event, full_name, username),
    )


async def is_blocked(user_id: int) -> Tuple[bool, Optional[str]]:
    await ensure_users_tables()
    row = await db_execute_fetch("SELECT blocked_until, reason FROM user_blocks WHERE user_id=?", (user_id,))
    if not row:
        return False, None

    try:
        until = iso_to_dt(row["blocked_until"])
    except Exception:
        await db_execute_fetch("DELETE FROM user_blocks WHERE user_id=?", (user_id,))
        return False, None

    if until <= utcnow():
        await db_execute_fetch("DELETE FROM user_blocks WHERE user_id=?", (user_id,))
        return False, None

    return True, row.get("reason") or "blocked"


async def block_user_24h(user_id: int, reason: str):
    await ensure_users_tables()
    until = utcnow() + timedelta(hours=24)
    await db_execute_fetch(
        """
        INSERT INTO user_blocks (user_id, blocked_until, reason)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
          blocked_until=excluded.blocked_until,
          reason=excluded.reason
        """,
        (user_id, dt_to_iso(until), reason),
    )


async def count_request_access_in_window(user_id: int, window: timedelta) -> int:
    await ensure_users_tables()
    since = dt_to_iso(utcnow() - window)
    row = await db_execute_fetch(
        """
        SELECT COUNT(*) as cnt
        FROM user_log
        WHERE user_id = ?
          AND event = 'request_access'
          AND created_at >= ?
        """,
        (user_id, since),
    )
    return int(row["cnt"]) if row and row.get("cnt") is not None else 0


async def get_last_users(limit: int = 10) -> List[Dict[str, Any]]:
    await ensure_users_tables()
    rows = await db_execute_fetch(
        """
        SELECT user_id, last_seen_at, full_name, username
        FROM users
        ORDER BY last_seen_at DESC
        LIMIT ?
        """,
        (limit,),
        many=True,
    )
    return rows or []


# ================= ADMIN UI: /allowed WITH BUTTONS =================

ALLOWED_PAGE_SIZE = 10


def format_dt_utc_short(iso_s: str) -> str:
    try:
        dt = iso_to_dt(iso_s)
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return iso_s


async def send_allowed_page(chat_id: int, page: int = 0, message_id: Optional[int] = None):
    rows = await list_temp_allowed_active(limit=500)

    if not rows:
        text = "Активных временных доступов нет."
        if message_id:
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text)
        else:
            await bot.send_message(chat_id, text)
        return

    total = len(rows)
    pages = max(1, (total + ALLOWED_PAGE_SIZE - 1) // ALLOWED_PAGE_SIZE)
    page = max(0, min(page, pages - 1))

    start = page * ALLOWED_PAGE_SIZE
    chunk = rows[start: start + ALLOWED_PAGE_SIZE]

    lines = [f"🔐 Активные временные доступы: {total} (стр. {page+1}/{pages})", ""]
    for r in chunk:
        uid = r["user_id"]
        exp = format_dt_utc_short(r["expires_at"])
        added_by = r.get("added_by")
        lines.append(f"• {uid} до {exp} (выдал {added_by})")
    text = "\n".join(lines)

    kb_rows: List[List[InlineKeyboardButton]] = []
    for r in chunk:
        uid = int(r["user_id"])
        kb_rows.append(
            [InlineKeyboardButton(text=f"❌ Отменить {uid}", callback_data=build_cbdata("revoke_access", uid=uid, page=page))]
        )

    nav: List[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Назад", callback_data=build_cbdata("allowed_page", page=page - 1)))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("➡️ Далее", callback_data=build_cbdata("allowed_page", page=page + 1)))
    if nav:
        kb_rows.append(nav)

    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

    if message_id:
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, reply_markup=kb)
    else:
        await bot.send_message(chat_id=chat_id, text=text, reply_markup=kb)


# ================= PEOPLE SEARCH + DISPLAY =================

FIELD_LABELS = {
    "floor": "Этаж",
    "object_number": "Объект",
    "sizes": "Площадь",
    "name": "ФИО",
    "gender": "Доля",
    "passport": "Паспорт",
    "plate": "Документ",
    "phone": "Телефон",
    "note": "Аннотация",
}

EDITABLE_FIELDS = [
    "floor", "object_number", "sizes", "name",
    "gender", "passport", "plate", "phone",
    "note",
]


async def search_db_page(query: str, page: int = 0):
    q_formatted = capitalize_words(query)

    offset = page * PAGE_SIZE
    pattern = f"%{q_formatted}%"
    digits = digits_only(query)

    obj_query = convert_en_to_ru(query).strip().upper()
    obj_pattern = f"%{obj_query}%"

    where_clauses = ["name LIKE ?", "UPPER(object_number) LIKE ?"]
    params: List[Any] = [pattern, obj_pattern]

    if digits:
        where_clauses.append("phone_digits LIKE ?")
        params.append(f"%{digits}%")

    sql = f"""
        SELECT id, floor, object_number, sizes, name, gender,
               passport, plate, phone
        FROM people
        WHERE {" OR ".join(where_clauses)}
        ORDER BY id
        LIMIT ? OFFSET ?
    """
    params.extend([PAGE_SIZE, offset])

    rows = await db_execute_fetch(sql, tuple(params), many=True)

    count_sql = f"""
        SELECT COUNT(*) as cnt FROM people
        WHERE {" OR ".join(where_clauses)}
    """
    total_row = await db_execute_fetch(count_sql, tuple(params[:-2]))
    total = total_row["cnt"] if total_row else 0

    return rows or [], total


async def search_by_object_id(object_id: str) -> List[Dict[str, Any]]:
    object_id = convert_en_to_ru(object_id).strip()
    search_query = object_id.upper()
    rows = await db_execute_fetch(
        """
        SELECT id, floor, object_number, sizes, name, gender,
               passport, plate, phone
        FROM people
        WHERE UPPER(object_number) = ?
        ORDER BY floor, name
        """,
        (search_query,),
        many=True,
    )
    return rows or []


def format_card_text(rec: Dict[str, Any], admin: bool = False) -> str:
    lines = [
        f"ID: {rec.get('id')}",
        f"{FIELD_LABELS['floor']}: {rec.get('floor') or '-'}",
        f"{FIELD_LABELS['object_number']}: {rec.get('object_number') or '-'}",
        f"{FIELD_LABELS['sizes']}: {rec.get('sizes') or '-'}",
        f"{FIELD_LABELS['name']}: {rec.get('name') or '-'}",
        f"{FIELD_LABELS['gender']}: {rec.get('gender') or '-'}",
        f"{FIELD_LABELS['passport']}: {rec.get('passport') or '-'}",
        f"{FIELD_LABELS['plate']}: {rec.get('plate') or '-'}",
        f"{FIELD_LABELS['phone']}: {rec.get('phone') or '-'}",
    ]
    if admin and rec.get("note"):
        lines.append(f"{FIELD_LABELS['note']}: {rec['note']}")
    return "\n".join(lines)


async def send_search_page(chat_id: int, query: str, page: int):
    rows, total = await search_db_page(query, page)
    if not rows:
        await bot.send_message(chat_id, "Ничего не найдено.")
        return

    await bot.send_message(chat_id, f"Найдено: {total}. Страница {page+1}")

    for r in rows:
        session_id = create_search_session(query, page)
        kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="Открыть", callback_data=build_cbdata("view", id=r["id"], sid=session_id))]]
        )
        text = f"{r.get('name') or '-'} — {r.get('object_number') or '-'}"
        await bot.send_message(chat_id, text, reply_markup=kb)


async def send_object_search_page(chat_id: int, object_id: str):
    rows = await search_by_object_id(object_id)
    if not rows:
        await bot.send_message(chat_id, f"❌ Объект '{object_id}' не найден.")
        return

    object_id = convert_en_to_ru(object_id)
    await bot.send_message(chat_id, f"📍 Объект: {object_id.upper()}\nНайдено: {len(rows)} записей")

    for r in rows:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="Открыть", callback_data=build_cbdata("view", id=r["id"]))]]
        )
        text = f"Этаж {r.get('floor')}: {r.get('name')} ({r.get('sizes')} м²)"
        await bot.send_message(chat_id, text, reply_markup=kb)


# ================= OWNER (sum area by owner, handles namesakes) =================

def _sizes_to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


async def owner_find_candidates(query: str, limit: int = 30) -> List[str]:
    q = capitalize_words(query)
    rows = await db_execute_fetch(
        """
        SELECT DISTINCT TRIM(name) AS name
        FROM people
        WHERE name IS NOT NULL AND TRIM(name) <> ''
          AND name LIKE ?
        ORDER BY name
        LIMIT ?
        """,
        (f"%{q}%", limit),
        many=True,
    )
    return [r["name"] for r in (rows or []) if r.get("name")]


async def owner_get_rows_by_name(exact_name: str) -> List[Dict[str, Any]]:
    rows = await db_execute_fetch(
        """
        SELECT id, floor, object_number, sizes, name
        FROM people
        WHERE TRIM(name) = TRIM(?)
        ORDER BY floor, object_number, id
        """,
        (exact_name,),
        many=True,
    )
    return rows or []


def owner_build_report(exact_name: str, rows: List[Dict[str, Any]]) -> str:
    total_area = 0.0
    bad = 0
    by_floor: Dict[int, float] = {}
    objs: List[str] = []

    for r in rows:
        if r.get("object_number"):
            objs.append(str(r["object_number"]))
        f = _sizes_to_float(r.get("sizes"))
        if f is None:
            bad += 1
        else:
            total_area += f
            try:
                fl = int(r.get("floor")) if r.get("floor") is not None else None
            except Exception:
                fl = None
            if fl is not None:
                by_floor[fl] = by_floor.get(fl, 0.0) + f

    uniq_objs = sorted(set(objs), key=lambda x: (x or ""))

    lines = [
        f"👤 Собственник: **{exact_name}**",
        f"📦 Записей: **{len(rows)}**",
        f"📐 Суммарная площадь: **{round(total_area, 2)} м²**",
        f"⚠️ Некорректная/пустая площадь (sizes): **{bad}**",
        "",
        "🏢 Площадь по этажам:",
    ]

    if not by_floor:
        lines.append("— нет данных")
    else:
        for fl in sorted(by_floor.keys()):
            lines.append(f"• Этаж {fl}: {round(by_floor[fl], 2)} м²")

    if uniq_objs:
        lines.append("")
        lines.append("🏷 Объекты:")
        preview = uniq_objs[:30]
        lines.extend([f"• {o}" for o in preview])
        if len(uniq_objs) > 30:
            lines.append(f"… и ещё {len(uniq_objs) - 30}")

    return "\n".join(lines)


# ================= EXPORT (ADMIN ONLY) =================

async def get_all_floors() -> List[int]:
    rows = await db_execute_fetch(
        """
        SELECT DISTINCT floor FROM people
        WHERE floor IS NOT NULL
        ORDER BY floor
        """,
        many=True,
    )
    return [int(r["floor"]) for r in rows or []]


async def get_floor_data(floor: int) -> List[Dict[str, Any]]:
    rows = await db_execute_fetch(
        """
        SELECT id, floor, object_number, sizes, name, gender,
               passport, plate, phone, note
        FROM people
        WHERE floor = ?
        ORDER BY object_number, name
        """,
        (floor,),
        many=True,
    )
    return rows or []


def create_csv_for_floor(data: List[Dict[str, Any]]) -> bytes:
    output = io.StringIO()
    fieldnames = [
        "id",
        "floor",
        "object_number",
        "sizes",
        "name",
        "gender",
        "passport",
        "plate",
        "phone",
        "note",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for row in data:
        writer.writerow({k: row.get(k, "") for k in fieldnames})
    return output.getvalue().encode("utf-8-sig")


# ================= STATS (ADMIN ONLY) =================

async def get_unique_owners_count() -> int:
    row = await db_execute_fetch(
        """
        SELECT COUNT(DISTINCT TRIM(name)) AS cnt
        FROM people
        WHERE name IS NOT NULL AND TRIM(name) <> ''
        """
    )
    return int(row["cnt"]) if row and row.get("cnt") is not None else 0


async def get_total_area_stats() -> Dict[str, Any]:
    rows = await db_execute_fetch("SELECT sizes FROM people", many=True)
    total_area = 0.0
    bad_sizes = 0
    for r in rows or []:
        f = _sizes_to_float(r.get("sizes"))
        if f is None:
            bad_sizes += 1
        else:
            total_area += f
    return {"total_area": round(total_area, 2), "bad_sizes": bad_sizes}


async def get_area_by_floor() -> List[Dict[str, Any]]:
    rows = await db_execute_fetch(
        "SELECT floor, sizes FROM people WHERE floor IS NOT NULL ORDER BY floor",
        many=True,
    )

    area: Dict[int, float] = {}
    bad: Dict[int, int] = {}

    for r in rows or []:
        try:
            fl = int(r.get("floor"))
        except Exception:
            continue

        f = _sizes_to_float(r.get("sizes"))
        if f is None:
            bad[fl] = bad.get(fl, 0) + 1
            continue

        area[fl] = area.get(fl, 0.0) + f

    out: List[Dict[str, Any]] = []
    for fl in sorted(area.keys() | bad.keys()):
        out.append({"floor": fl, "area": round(area.get(fl, 0.0), 2), "bad_sizes": bad.get(fl, 0)})
    return out


# ================= COMMANDS =================

@dp.message(Command("start"))
async def start_cmd(message: Message):
    try:
        await upsert_user_seen(message.from_user)
        await add_user_log(message.from_user, "start")
    except Exception as e:
        logger.error(f"Cannot log start: {e}", exc_info=True)

    help_text = """
🤖 **Помощь по командам:**

🔐 **Доступ:**
• `/id` - показать ваш Telegram ID
• `/request_access` - запросить доступ (сообщение администратору)

📝 **Поиск:**
• `/search <запрос>` - поиск по ФИО/объекту/телефону
• `/obj <объект>` - поиск по ID объекта (например: `/obj К-2`)

👤 **Собственник:**
• `/owner <ФИО>` - суммарная площадь по собственнику (учёт однофамильцев)

📊 **Статистика:**
• `/stats` - уникальные собственники + площадь общая и по этажам

👮 **Админ:**
• `/allowed` - список временных доступов + кнопки отмены
• `/guests` - последние 10 пользователей (уникальные)
• `/export` - экспорт CSV по этажам (flor1.csv, flor2.csv ...)
• `/edit <id> <поле> <значение>` - прямое редактирование записи по ID

💡 **Инлайн:** введите запрос после @имя_бота
    """
    await message.answer(help_text)


@dp.message(Command("id"))
async def id_cmd(message: Message):
    await message.answer(f"Ваш Telegram ID: {message.from_user.id}")


@dp.message(Command("request_access"))
async def request_access_cmd(message: Message):
    try:
        await upsert_user_seen(message.from_user)
        await add_user_log(message.from_user, "request_access")
    except Exception as e:
        logger.error(f"Cannot log request_access: {e}", exc_info=True)

    blocked, _ = await is_blocked(message.from_user.id)
    if blocked:
        await message.answer("⛔ Вам временно запрещено отправлять запросы доступа (блокировка на сутки).")
        return

    cnt = await count_request_access_in_window(message.from_user.id, window=timedelta(minutes=5))
    if cnt > 5:
        await block_user_24h(message.from_user.id, reason="request_access spam >5/5min")
        await message.answer("⛔ Слишком много запросов. Вы заблокированы на 24 часа.")
        return

    user = message.from_user
    user_id = user.id
    username = (user.username or "").strip()
    full_name = (user.full_name or "").strip()

    who = full_name or "Пользователь"
    if username:
        who += f" (@{username})"

    await message.answer("✅ Запрос доступа отправлен администратору. Ожидайте подтверждения.")

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Разрешить на 24 часа", callback_data=build_cbdata("access_allow", uid=user_id)),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=build_cbdata("access_deny", uid=user_id)),
            ]
        ]
    )

    text = f"🔐 Запрос доступа:\n{who} — ID: {user_id}"

    if not ADMIN_IDS:
        logger.warning("ADMIN_USERS пустой: некому отправить запрос доступа.")
        return

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(chat_id=admin_id, text=text, reply_markup=kb)
        except Exception as e:
            logger.error(f"Cannot send access request to admin {admin_id}: {e}", exc_info=True)


@dp.message(Command("allowed"))
async def allowed_cmd(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Только администратор.")
        return
    await send_allowed_page(message.chat.id, page=0)


@dp.message(Command("guests"))
async def guests_cmd(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Только администратор.")
        return

    rows = await get_last_users(limit=10)
    if not rows:
        await message.answer("Пользователей пока нет.")
        return

    lines = ["👥 **Последние 10 пользователей:**", ""]
    for r in rows:
        dt = format_dt_utc_short(r["last_seen_at"])
        uid = r["user_id"]
        full_name = r.get("full_name") or "-"
        username = r.get("username") or ""
        u = f"@{username}" if username else ""
        lines.append(f"• {dt} — {full_name} {u} — ID: {uid}")

    await message.answer("\n".join(lines))


@dp.message(Command("search"))
async def search_cmd(message: Message):
    if not await is_user_allowed(message.from_user.id):
        await message.answer("❌ У вас нет доступа к поиску")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Используйте: /search <запрос>")
        return
    await send_search_page(message.chat.id, parts[1], 0)


@dp.message(Command("obj"))
async def obj_cmd(message: Message):
    if not await is_user_allowed(message.from_user.id):
        await message.answer("❌ У вас нет доступа к поиску")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Используйте: /obj <ID объекта>\nПример: /obj К-2")
        return
    await send_object_search_page(message.chat.id, parts[1].strip())


@dp.message(Command("owner"))
async def owner_cmd(message: Message):
    if not await is_user_allowed(message.from_user.id):
        await message.answer("❌ У вас нет доступа")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Используйте: /owner <ФИО или часть>\nПример: /owner Иванов")
        return

    q = parts[1].strip()
    if len(q) < 2:
        await message.answer("Введите минимум 2 символа для поиска.")
        return

    candidates = await owner_find_candidates(q, limit=30)

    if not candidates:
        await message.answer("❌ Собственник не найден.")
        return

    if len(candidates) == 1:
        exact_name = candidates[0]
        rows = await owner_get_rows_by_name(exact_name)
        await message.answer(owner_build_report(exact_name, rows))
        return

    sid = owner_create_session(candidates)

    kb_rows: List[List[InlineKeyboardButton]] = []
    for idx, nm in enumerate(candidates[:20]):
        kb_rows.append([InlineKeyboardButton(text=nm, callback_data=build_cbdata("owner_pick", sid=sid, i=idx))])

    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    await message.answer("Найдено несколько совпадений (однофамильцы). Выберите:", reply_markup=kb)


@dp.message(Command("cancel"))
async def cancel_edit_cmd(message: Message, state: FSMContext):
    current = await state.get_state()
    if current is not None:
        await state.clear()
        await message.answer("❌ Редактирование отменено.")
    else:
        await message.answer("Нечего отменять.")


@dp.message(Command("edit"))
async def edit_cmd(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Только администратор.")
        return

    # Формат: /edit <id> <field> <value>
    parts = message.text.split(maxsplit=3)
    if len(parts) < 4:
        await message.answer(
            "Использование: `/edit <id> <поле> <значение>`\n"
            "Пример: `/edit 42 phone +79001234567`\n\n"
            f"Доступные поля: {', '.join(EDITABLE_FIELDS)}"
        )
        return

    try:
        rec_id = int(parts[1])
    except ValueError:
        await message.answer("❌ ID должен быть числом.")
        return

    field = parts[2].strip().lower()
    new_value = parts[3].strip()

    success = await update_people_field(rec_id, field, new_value)
    if success:
        label = FIELD_LABELS.get(field, field)
        try:
            await add_user_log(message.from_user, f"edit_field:{field}:id={rec_id}")
        except Exception as e:
            logger.warning(f"Cannot log edit: {e}")
        await message.answer(f"✅ Поле **{label}** записи ID {rec_id} обновлено на: `{new_value}`")
    else:
        await message.answer(f"❌ Недопустимое поле. Допустимые: {', '.join(EDITABLE_FIELDS)}")


@dp.message(Command("stats"))
async def stats_cmd(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Только администратор.")
        return

    unique_owners = await get_unique_owners_count()
    total = await get_total_area_stats()
    by_floor = await get_area_by_floor()

    lines = [
        "📊 **Статистика**",
        "",
        f"👤 Уникальных собственников: **{unique_owners}**",
        f"📐 Общая площадь: **{total['total_area']} м²**",
        f"⚠️ Некорректная/пустая площадь (sizes): **{total['bad_sizes']}** строк(и)",
        "",
        "🏢 **Площадь по этажам:**",
    ]

    if not by_floor:
        lines.append("— нет данных")
    else:
        for item in by_floor:
            extra = f" (⚠️ bad: {item['bad_sizes']})" if item["bad_sizes"] else ""
            lines.append(f"• Этаж {item['floor']}: {item['area']} м²{extra}")

    await message.answer("\n".join(lines))


@dp.message(Command("export"))
async def export_cmd(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Только администратор может экспортировать данные.")
        return

    floors = await get_all_floors()
    if not floors:
        await message.answer("❌ Нет этажей для экспорта (floor пустой).")
        return

    await message.answer(f"📥 Экспортирую {len(floors)} этаж(а/ей) в CSV...")

    for fl in floors:
        data = await get_floor_data(fl)
        if not data:
            continue

        csv_bytes = create_csv_for_floor(data)
        filename = f"flor{fl}.csv"

        await bot.send_document(
            chat_id=message.chat.id,
            document=types.BufferedInputFile(file=csv_bytes, filename=filename),
            caption=f"Этаж {fl}: {len(data)} записей",
        )
        await asyncio.sleep(0.3)

    await message.answer("✅ Экспорт завершён.")


# ================= CALLBACKS =================

@dp.callback_query(lambda c: c.data.startswith("owner_pick"))
async def owner_pick_handler(callback: types.CallbackQuery):
    await callback.answer()
    _, data = parse_cbdata(callback.data)

    sid = (data.get("sid") or "").strip()
    try:
        idx = int(data.get("i", "-1"))
    except ValueError:
        idx = -1

    nm = owner_get_candidate(sid, idx)
    if not nm:
        await callback.message.answer("❌ Выбор устарел. Повторите /owner заново.")
        return

    rows = await owner_get_rows_by_name(nm)
    await callback.message.answer(owner_build_report(nm, rows))


@dp.callback_query(lambda c: c.data.startswith("access_allow"))
async def access_allow_handler(callback: types.CallbackQuery):
    try:
        if not is_admin(callback.from_user.id):
            return await callback.answer("Только администратор", show_alert=True)

        await callback.answer()
        _, data = parse_cbdata(callback.data)
        user_id = int(data.get("uid", "0"))
        if user_id <= 0:
            await callback.message.answer("❌ Ошибка: uid не найден.")
            return

        expires = await set_temp_allowed_24h(user_id=user_id, added_by=callback.from_user.id)

        await callback.message.answer(
            f"✅ Доступ выдан пользователю {user_id} на 24 часа (до UTC): {expires.isoformat()}"
        )

        try:
            await bot.send_message(chat_id=user_id, text="✅ Вам выдан доступ на 24 часа.")
        except Exception as e:
            logger.warning(f"Cannot notify user {user_id}: {e}")

    except Exception as e:
        logger.error(f"Error in access_allow_handler: {e}", exc_info=True)
        await callback.answer("Ошибка", show_alert=True)


@dp.callback_query(lambda c: c.data.startswith("access_deny"))
async def access_deny_handler(callback: types.CallbackQuery):
    try:
        if not is_admin(callback.from_user.id):
            return await callback.answer("Только администратор", show_alert=True)

        await callback.answer()
        _, data = parse_cbdata(callback.data)
        user_id = int(data.get("uid", "0"))
        if user_id <= 0:
            await callback.message.answer("❌ Ошибка: uid не найден.")
            return

        await remove_temp_allowed(user_id)
        await callback.message.answer(f"❌ Запрос отклонён. Пользователь: {user_id}")

        try:
            await bot.send_message(chat_id=user_id, text="❌ Доступ не выдан (запрос отклонён администратором).")
        except Exception as e:
            logger.warning(f"Cannot notify user {user_id}: {e}")

    except Exception as e:
        logger.error(f"Error in access_deny_handler: {e}", exc_info=True)
        await callback.answer("Ошибка", show_alert=True)


@dp.callback_query(lambda c: c.data.startswith("allowed_page"))
async def allowed_page_handler(callback: types.CallbackQuery):
    try:
        if not is_admin(callback.from_user.id):
            return await callback.answer("Только администратор", show_alert=True)

        await callback.answer()
        _, data = parse_cbdata(callback.data)
        page = int(data.get("page", "0"))

        await send_allowed_page(
            chat_id=callback.message.chat.id,
            page=page,
            message_id=callback.message.message_id,
        )
    except Exception as e:
        logger.error(f"Error in allowed_page_handler: {e}", exc_info=True)
        await callback.answer("Ошибка", show_alert=True)


@dp.callback_query(lambda c: c.data.startswith("revoke_access"))
async def revoke_access_handler(callback: types.CallbackQuery):
    try:
        if not is_admin(callback.from_user.id):
            return await callback.answer("Только администратор", show_alert=True)

        await callback.answer()
        _, data = parse_cbdata(callback.data)
        uid = int(data.get("uid", "0"))
        page = int(data.get("page", "0"))

        if uid <= 0:
            await callback.message.answer("❌ Ошибка: uid не найден.")
            return

        await remove_temp_allowed(uid)

        try:
            await bot.send_message(chat_id=uid, text="⛔ Ваш временный доступ был отменён администратором.")
        except Exception:
            pass

        await send_allowed_page(
            chat_id=callback.message.chat.id,
            page=page,
            message_id=callback.message.message_id,
        )

    except Exception as e:
        logger.error(f"Error in revoke_access_handler: {e}", exc_info=True)
        await callback.answer("Ошибка", show_alert=True)


@dp.callback_query(lambda c: c.data.startswith("view"))
async def view_handler(callback: types.CallbackQuery):
    try:
        await callback.answer()
        _, data = parse_cbdata(callback.data)

        rec_id = int(data.get("id", 0))
        if rec_id == 0:
            await callback.message.answer("Ошибка: неправильный ID.")
            return

        rec = await db_execute_fetch("SELECT * FROM people WHERE id=?", (rec_id,))
        if not rec:
            await callback.message.answer("Запись не найдена.")
            return

        text = format_card_text(rec, admin=is_admin(callback.from_user.id))

        if is_admin(callback.from_user.id):
            kb_rows = [
                [InlineKeyboardButton(
                    text=f"✏️ {FIELD_LABELS.get(f, f)}",
                    callback_data=build_cbdata("edit_field", id=rec_id, field=f)
                )]
                for f in EDITABLE_FIELDS
            ]
            kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
            await callback.message.answer(text, reply_markup=kb)
        else:
            await callback.message.answer(text)

    except Exception as e:
        logger.error(f"Error in view_handler: {e}", exc_info=True)
        await callback.answer("Ошибка", show_alert=True)


@dp.callback_query(lambda c: c.data.startswith("edit_field"))
async def edit_field_handler(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return await callback.answer("Только администратор", show_alert=True)

    await callback.answer()
    _, data = parse_cbdata(callback.data)
    rec_id = int(data.get("id", 0))
    field = data.get("field", "")

    if not rec_id or not field:
        await callback.message.answer("❌ Ошибка: не указан ID или поле.")
        return

    await state.set_state(EditState.waiting_for_value)
    await state.update_data(rec_id=rec_id, field=field)

    label = FIELD_LABELS.get(field, field)
    await callback.message.answer(
        f"✏️ Введите новое значение для поля **{label}** (запись ID: {rec_id}):\n"
        f"Для отмены отправьте /cancel"
    )


@dp.message(EditState.waiting_for_value)
async def edit_value_received(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return

    fsm_data = await state.get_data()
    rec_id = fsm_data.get("rec_id")
    field = fsm_data.get("field")
    new_value = (message.text or "").strip()

    await state.clear()

    if not rec_id or not field:
        await message.answer("❌ Ошибка состояния. Попробуйте заново.")
        return

    success = await update_people_field(rec_id, field, new_value)

    if success:
        label = FIELD_LABELS.get(field, field)
        try:
            await add_user_log(message.from_user, f"edit_field:{field}:id={rec_id}")
        except Exception as e:
            logger.warning(f"Cannot log edit: {e}")
        await message.answer(
            f"✅ Поле **{label}** записи ID {rec_id} обновлено.\n"
            f"Новое значение: `{new_value}`"
        )
    else:
        await message.answer("❌ Ошибка обновления. Недопустимое поле.")


# ================= INLINE =================

@dp.inline_query()
async def inline_handler(inline_query: types.InlineQuery):
    try:
        if not await is_user_allowed(inline_query.from_user.id):
            await bot.answer_inline_query(inline_query.id, results=[], cache_time=0, is_personal=True)
            return

        q = inline_query.query.strip()
        if not q:
            await bot.answer_inline_query(inline_query.id, results=[], cache_time=0, is_personal=True)
            return

        object_id_pattern = r"^[а-яА-ЯЁё]+-\d+$|^[A-Za-z]+-\d+$"

        if re.match(object_id_pattern, q):
            q_converted = convert_en_to_ru(q)
            rows = await search_by_object_id(q_converted)
        else:
            q_formatted = capitalize_words(q)
            pattern = f"%{q_formatted}%"
            digits = digits_only(q)

            where_clauses = ["name LIKE ?"]
            params = [pattern]

            if digits:
                where_clauses.append("phone_digits LIKE ?")
                params.append(f"%{digits}%")

            sql = f"""
                SELECT id, floor, object_number, sizes, name, gender,
                       passport, plate, phone
                FROM people
                WHERE {" OR ".join(where_clauses)}
                ORDER BY id
                LIMIT 20
            """
            rows = await db_execute_fetch(sql, tuple(params), many=True)

        if not rows:
            await bot.answer_inline_query(inline_query.id, results=[], cache_time=0, is_personal=True)
            return

        results = []
        for r in rows:
            text = format_card_text(r)
            results.append(
                InlineQueryResultArticle(
                    id=str(r["id"]),
                    title=f"{r.get('name') or '-'} — {r.get('object_number') or '-'}",
                    description=f"Этаж {r.get('floor') or '-'} | Площадь: {r.get('sizes') or '-'}",
                    input_message_content=InputTextMessageContent(message_text=text),
                )
            )

        await bot.answer_inline_query(inline_query.id, results=results, cache_time=0, is_personal=True)
    except Exception as e:
        logger.error(f"Error in inline_handler: {e}", exc_info=True)
        await bot.answer_inline_query(inline_query.id, results=[], cache_time=0, is_personal=True)


# ================= RUN =================

async def main():
    logger.info("Bot starting...")
    try:
        await db_init_pragmas()
        await migrate_people_table()
        await ensure_temp_allowed_table()
        await ensure_users_tables()
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())