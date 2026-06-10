"""
═══════════════════════════════════════════════════════════════
  BUSINESS BOT — платформа мини-ботов для малого бизнеса
═══════════════════════════════════════════════════════════════

Один Telegram-бот хранит ВНУТРИ СЕБЯ ботов разных компаний.
Каждый владелец бизнеса:
  1. Открывает бота → жмёт «Меню» → заполняет анкету (WebApp)
  2. Получает свою ссылку: t.me/ИМЯБОТА?start=biz_42
  3. Делится ссылкой с клиентами (Instagram, визитка, 2GIS)
  4. Клиенты пишут по этой ссылке → AI отвечает + принимает записи
  5. Владелец видит все заявки и статистику в своей панели

Технологии: aiogram 3.x (Telegram) + YandexGPT (AI-ответы)
            + aiohttp (встроенный API сервер для WebApp)
            + aiosqlite (база данных, отдельный файл, не пропадает)

Установка:
    pip install -r requirements.txt

Переменные окружения (Railway → Variables):
    BOT_TOKEN         — токен от @BotFather
    YANDEX_API_KEY    — API-ключ Yandex Cloud (ai.languageModels.user)
    YANDEX_FOLDER_ID  — ID каталога Yandex Cloud
    WEBAPP_URL        — ссылка на задеплоенный index.html (Vercel)
    PORT              — порт для API (Railway подставляет сам)
═══════════════════════════════════════════════════════════════
"""

import asyncio
import json
import logging
import os
from datetime import datetime

import aiohttp
import aiosqlite
from aiohttp import web
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo

# ═══════════════════════ CONFIG ═══════════════════════════════
BOT_TOKEN        = os.getenv("BOT_TOKEN", "PASTE_BOT_TOKEN")
YANDEX_API_KEY   = os.getenv("YANDEX_API_KEY", "PASTE_YANDEX_KEY")
YANDEX_FOLDER_ID = os.getenv("YANDEX_FOLDER_ID", "PASTE_FOLDER_ID")
WEBAPP_URL       = os.getenv("WEBAPP_URL", "https://example.vercel.app")
PORT             = int(os.getenv("PORT", "8080"))
DB_PATH          = "business.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("business-bot")


# ═══════════════════════ DATABASE ═════════════════════════════
SCHEMA = """
CREATE TABLE IF NOT EXISTS businesses (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_id        INTEGER NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    description     TEXT DEFAULT '',
    services        TEXT DEFAULT '',
    address         TEXT DEFAULT '',
    phone           TEXT DEFAULT '',
    working_hours   TEXT DEFAULT '',
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bookings (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    business_id     INTEGER NOT NULL,
    user_id         INTEGER NOT NULL,
    user_name       TEXT DEFAULT '',
    service         TEXT DEFAULT '',
    booking_date    TEXT DEFAULT '',
    booking_time    TEXT DEFAULT '',
    note            TEXT DEFAULT '',
    status          TEXT DEFAULT 'pending',
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    business_id     INTEGER NOT NULL,
    user_id         INTEGER NOT NULL,
    role            TEXT NOT NULL,
    content         TEXT NOT NULL,
    created_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_bookings_biz ON bookings(business_id);
CREATE INDEX IF NOT EXISTS idx_messages_biz_user ON messages(business_id, user_id);
"""

async def db_init():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA)
        await db.commit()

async def db_get_biz_by_owner(owner_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM businesses WHERE owner_id=?", (owner_id,))
        row = await cur.fetchone()
        return dict(row) if row else None

async def db_get_biz_by_id(bid: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM businesses WHERE id=?", (bid,))
        row = await cur.fetchone()
        return dict(row) if row else None

async def db_upsert_biz(owner_id: int, **fields) -> dict:
    existing = await db_get_biz_by_owner(owner_id)
    async with aiosqlite.connect(DB_PATH) as db:
        if existing:
            cols = ", ".join(f"{k}=?" for k in fields)
            await db.execute(f"UPDATE businesses SET {cols} WHERE owner_id=?",
                              [*fields.values(), owner_id])
        else:
            fields["owner_id"]   = owner_id
            fields["created_at"] = datetime.now().isoformat()
            cols = ", ".join(fields.keys())
            qs   = ", ".join("?" for _ in fields)
            await db.execute(f"INSERT INTO businesses ({cols}) VALUES ({qs})",
                              list(fields.values()))
        await db.commit()
    return await db_get_biz_by_owner(owner_id)

async def db_add_booking(business_id, user_id, user_name, service, date, time, note=""):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """INSERT INTO bookings
               (business_id,user_id,user_name,service,booking_date,booking_time,note,created_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (business_id, user_id, user_name, service, date, time, note,
             datetime.now().isoformat()))
        await db.commit()
        return cur.lastrowid

async def db_get_bookings(business_id, status=None, limit=50):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        q  = "SELECT * FROM bookings WHERE business_id=?"
        ps = [business_id]
        if status:
            q += " AND status=?"
            ps.append(status)
        q += " ORDER BY id DESC LIMIT ?"
        ps.append(limit)
        cur = await db.execute(q, ps)
        return [dict(r) for r in await cur.fetchall()]

async def db_set_booking_status(booking_id, status):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bookings SET status=? WHERE id=?", (status, booking_id))
        await db.commit()

async def db_get_history(business_id, user_id, limit=6):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT role, content FROM messages WHERE business_id=? AND user_id=? "
            "ORDER BY id DESC LIMIT ?", (business_id, user_id, limit))
        rows = await cur.fetchall()
        return [dict(r) for r in reversed(rows)]

async def db_save_message(business_id, user_id, role, content):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO messages (business_id,user_id,role,content,created_at) VALUES (?,?,?,?,?)",
            (business_id, user_id, role, content, datetime.now().isoformat()))
        await db.commit()

async def db_get_stats(business_id):
    async with aiosqlite.connect(DB_PATH) as db:
        async def one(sql, *p):
            cur = await db.execute(sql, p)
            row = await cur.fetchone()
            return row[0] if row else 0
        total   = await one("SELECT COUNT(*) FROM bookings WHERE business_id=?", business_id)
        pending = await one("SELECT COUNT(*) FROM bookings WHERE business_id=? AND status='pending'", business_id)
        today   = datetime.now().date().isoformat()
        today_n = await one("SELECT COUNT(*) FROM bookings WHERE business_id=? AND booking_date=?", business_id, today)
        clients = await one("SELECT COUNT(DISTINCT user_id) FROM messages WHERE business_id=?", business_id)
    return {"total": total, "pending": pending, "today": today_n, "clients": clients}


# ═══════════════════════ YANDEX GPT ═══════════════════════════
def build_ai_prompt(biz: dict) -> str:
    return (
        f"Ты — вежливый AI-помощник компании «{biz['name']}». "
        f"Описание: {biz.get('description') or '—'}. "
        f"Услуги и цены: {biz.get('services') or '—'}. "
        f"Адрес: {biz.get('address') or '—'}. "
        f"Телефон: {biz.get('phone') or '—'}. "
        f"Часы работы: {biz.get('working_hours') or '—'}. "
        "Отвечай кратко, дружелюбно, на русском языке. "
        "Если клиент хочет записаться — скажи что для записи есть кнопка «Записаться» под сообщением. "
        "Если не знаешь ответ — предложи позвонить по указанному телефону."
    )

async def ask_yandex_gpt(system_prompt: str, history: list[dict], user_text: str) -> str:
    messages = [{"role": "system", "text": system_prompt}]
    for h in history:
        messages.append({"role": h["role"], "text": h["content"]})
    messages.append({"role": "user", "text": user_text})

    payload = {
        "modelUri": f"gpt://{YANDEX_FOLDER_ID}/yandexgpt-lite",
        "completionOptions": {"stream": False, "temperature": 0.5, "maxTokens": 350},
        "messages": messages,
    }
    headers = {
        "Authorization": f"Api-Key {YANDEX_API_KEY}",
        "Content-Type": "application/json",
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                "https://llm.api.cloud.yandex.net/foundationModels/v1/completion",
                json=payload, headers=headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                data = await r.json()
        if "result" in data:
            return data["result"]["alternatives"][0]["message"]["text"]
        if "alternatives" in data:
            return data["alternatives"][0]["message"]["text"]
        log.error(f"YandexGPT unexpected response: {data}")
    except Exception as e:
        log.error(f"YandexGPT error: {e}")
    return "Извините, сейчас не могу ответить. Попробуйте написать позже или позвоните нам напрямую 🙏"


# ═══════════════════════ KEYBOARDS ════════════════════════════
def kb_owner_home(has_biz: bool) -> InlineKeyboardMarkup:
    if has_biz:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🖥 Открыть панель управления",
                                   web_app=WebAppInfo(url=f"{WEBAPP_URL}?mode=owner"))],
            [InlineKeyboardButton(text="📋 Заявки",     callback_data="owner:bookings"),
             InlineKeyboardButton(text="📊 Статистика", callback_data="owner:stats")],
            [InlineKeyboardButton(text="🔗 Ссылка для клиентов", callback_data="owner:link")],
        ])
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать бота для бизнеса",
                               web_app=WebAppInfo(url=f"{WEBAPP_URL}?mode=setup"))],
    ])

def kb_client_home(bid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗓 Записаться онлайн",
                               web_app=WebAppInfo(url=f"{WEBAPP_URL}?mode=client&bid={bid}"))],
        [InlineKeyboardButton(text="💬 Спросить AI", callback_data=f"client:ask:{bid}"),
         InlineKeyboardButton(text="📍 Контакты",     callback_data=f"client:contacts:{bid}")],
    ])

def kb_booking_admin(booking_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"bk:ok:{booking_id}"),
         InlineKeyboardButton(text="❌ Отменить",    callback_data=f"bk:no:{booking_id}")],
    ])

def kb_back(cb_data="owner:home") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=cb_data)]
    ])


# ═══════════════════════ TEXTS ════════════════════════════════
WELCOME_NEW_OWNER = (
    "👋 <b>Business Bot</b> — платформа AI-помощников для бизнеса\n\n"
    "Что получает ваш бизнес:\n"
    "✅ AI отвечает клиентам 24/7\n"
    "✅ Принимает онлайн-записи через мини-приложение\n"
    "✅ Уведомляет вас о каждой новой заявке\n"
    "✅ Показывает статистику клиентов\n\n"
    "Настройка занимает 2 минуты — нажмите кнопку 👇"
)

def welcome_owner_back(name: str) -> str:
    return f"👋 Панель управления — <b>{name}</b>"

def welcome_client(biz: dict) -> str:
    desc = f"\n\n{biz['description']}" if biz.get("description") else ""
    return f"👋 Добро пожаловать в <b>{biz['name']}</b>!{desc}\n\nЧем можем помочь?"


# ═══════════════════════ HANDLERS: COMMANDS ═══════════════════
async def cmd_start(msg: types.Message):
    uid  = msg.from_user.id
    args = msg.text.split(maxsplit=1)

    # Клиент перешёл по ссылке вида /start biz_42
    if len(args) > 1 and args[1].startswith("biz_"):
        try:
            bid = int(args[1].removeprefix("biz_"))
            biz = await db_get_biz_by_id(bid)
            if biz:
                await db_save_message(bid, uid, "system", "session_start")
                await msg.answer(welcome_client(biz), parse_mode="HTML",
                                  reply_markup=kb_client_home(bid))
                return
        except ValueError:
            pass

    # Владелец
    biz = await db_get_biz_by_owner(uid)
    if biz:
        await msg.answer(welcome_owner_back(biz["name"]), parse_mode="HTML",
                          reply_markup=kb_owner_home(True))
    else:
        await msg.answer(WELCOME_NEW_OWNER, parse_mode="HTML",
                          reply_markup=kb_owner_home(False))


# ═══════════════════════ HANDLERS: OWNER CALLBACKS ════════════
async def cb_owner_home(cb: types.CallbackQuery):
    biz = await db_get_biz_by_owner(cb.from_user.id)
    text = welcome_owner_back(biz["name"]) if biz else WELCOME_NEW_OWNER
    await cb.message.edit_text(text, parse_mode="HTML",
                                reply_markup=kb_owner_home(bool(biz)))
    await cb.answer()

async def cb_owner_bookings(cb: types.CallbackQuery):
    biz = await db_get_biz_by_owner(cb.from_user.id)
    if not biz:
        return await cb.answer("Сначала создайте бота через кнопку «Меню»!", show_alert=True)

    bookings = await db_get_bookings(biz["id"], status="pending", limit=8)
    if not bookings:
        await cb.message.edit_text("📋 Новых заявок нет.", reply_markup=kb_back())
        return await cb.answer()

    lines = ["📋 <b>Новые заявки:</b>\n"]
    rows  = []
    for b in bookings:
        lines.append(
            f"#{b['id']} <b>{b['service']}</b>\n"
            f"👤 {b['user_name']}\n"
            f"📅 {b['booking_date']} в {b['booking_time']}\n"
            + (f"💬 {b['note']}\n" if b['note'] else "")
        )
        rows.append([
            InlineKeyboardButton(text=f"✅ #{b['id']}", callback_data=f"bk:ok:{b['id']}"),
            InlineKeyboardButton(text=f"❌ #{b['id']}", callback_data=f"bk:no:{b['id']}"),
        ])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="owner:home")])
    await cb.message.edit_text("\n".join(lines), parse_mode="HTML",
                                reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

async def cb_owner_stats(cb: types.CallbackQuery):
    biz = await db_get_biz_by_owner(cb.from_user.id)
    if not biz:
        return await cb.answer("Сначала создайте бота через кнопку «Меню»!", show_alert=True)
    s = await db_get_stats(biz["id"])
    await cb.message.edit_text(
        f"📊 <b>Статистика — {biz['name']}</b>\n\n"
        f"👥 Клиентов писало: <b>{s['clients']}</b>\n"
        f"📋 Всего заявок: <b>{s['total']}</b>\n"
        f"⏳ Ожидают подтверждения: <b>{s['pending']}</b>\n"
        f"📅 Записей сегодня: <b>{s['today']}</b>",
        parse_mode="HTML", reply_markup=kb_back())
    await cb.answer()

async def cb_owner_link(cb: types.CallbackQuery):
    biz = await db_get_biz_by_owner(cb.from_user.id)
    if not biz:
        return await cb.answer("Сначала создайте бота через кнопку «Меню»!", show_alert=True)
    me   = await cb.bot.get_me()
    link = f"https://t.me/{me.username}?start=biz_{biz['id']}"
    await cb.message.edit_text(
        f"🔗 <b>Ваша ссылка для клиентов:</b>\n\n<code>{link}</code>\n\n"
        "Разместите её в Instagram, ВКонтакте, 2GIS, на визитке или сайте.\n"
        "Клиент перейдёт по ней прямо к AI-помощнику вашего бизнеса.",
        parse_mode="HTML", reply_markup=kb_back())
    await cb.answer()

async def cb_booking_set_status(cb: types.CallbackQuery):
    _, action, bid_str = cb.data.split(":")
    bid = int(bid_str)
    status = "confirmed" if action == "ok" else "cancelled"
    await db_set_booking_status(bid, status)
    text = "✅ Заявка подтверждена! Клиент получит уведомление." if action == "ok" \
           else "❌ Заявка отменена."
    await cb.answer(text, show_alert=True)


# ═══════════════════════ HANDLERS: CLIENT CALLBACKS ═══════════
async def cb_client_ask(cb: types.CallbackQuery):
    bid = int(cb.data.split(":")[2])
    biz = await db_get_biz_by_id(bid)
    if not biz:
        return await cb.answer()
    await db_save_message(bid, cb.from_user.id, "system", "ask_mode")
    await cb.message.edit_text(
        f"💬 Напишите вопрос — AI-помощник <b>{biz['name']}</b> ответит сразу:",
        parse_mode="HTML")
    await cb.answer()

async def cb_client_contacts(cb: types.CallbackQuery):
    bid = int(cb.data.split(":")[2])
    biz = await db_get_biz_by_id(bid)
    if not biz:
        return await cb.answer()
    lines = [f"📍 <b>{biz['name']}</b>\n"]
    if biz.get("address"):       lines.append(f"📍 Адрес: {biz['address']}")
    if biz.get("phone"):         lines.append(f"📞 Телефон: {biz['phone']}")
    if biz.get("working_hours"): lines.append(f"🕐 Часы работы: {biz['working_hours']}")
    if biz.get("services"):      lines.append(f"\n💼 Услуги:\n{biz['services']}")
    await cb.message.edit_text("\n".join(lines), parse_mode="HTML",
                                reply_markup=kb_client_home(bid))
    await cb.answer()


# ═══════════════════════ HANDLERS: TEXT (AI CHAT) ═════════════
async def on_text(msg: types.Message):
    uid = msg.from_user.id

    # Владелец пишет текстом — просто открываем меню
    biz_owner = await db_get_biz_by_owner(uid)
    if biz_owner:
        return await msg.answer("Управляйте ботом через панель 👇",
                                 reply_markup=kb_owner_home(True))

    # Ищем последний бизнес с которым общался этот клиент
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT business_id FROM messages WHERE user_id=? ORDER BY id DESC LIMIT 1", (uid,))
        row = await cur.fetchone()

    if not row:
        return await msg.answer(
            "Здравствуйте! Чтобы начать общение, перейдите по ссылке вашего заведения "
            "(её можно найти в Instagram/2GIS/визитке)."
        )

    bid = row["business_id"]
    biz = await db_get_biz_by_id(bid)
    if not biz:
        return

    history = await db_get_history(bid, uid)
    await db_save_message(bid, uid, "user", msg.text)

    typing_msg = await msg.answer("⏳ Печатаю...")
    reply = await ask_yandex_gpt(build_ai_prompt(biz), history, msg.text)
    await typing_msg.delete()

    await msg.answer(reply, reply_markup=kb_client_home(bid))
    await db_save_message(bid, uid, "assistant", reply)


# ═══════════════════════ HANDLERS: WEBAPP DATA ════════════════
# (срабатывает если WebApp открыт ЧЕРЕЗ кнопку бота — tg.sendData)
async def on_webapp_data(msg: types.Message):
    try:
        data = json.loads(msg.web_app_data.data)
        await process_setup_or_booking(data, msg.from_user, bot=msg.bot, reply_msg=msg)
    except Exception as e:
        log.error(f"webapp_data error: {e}")
        await msg.answer("⚠️ Ошибка обработки данных. Попробуйте ещё раз.")


# ═══════════════════════ SHARED LOGIC (bot + HTTP API) ═══════
async def process_setup_or_booking(data: dict, user, bot: Bot, reply_msg=None):
    """
    Общая точка входа для двух источников данных:
      1) Telegram WebApp tg.sendData()
      2) HTTP POST /api/... (если WebApp открыт в обычном браузере)
    `user` — объект с .id, .full_name, .username (или dict с теми же полями)
    """
    action = data.get("action")
    uid = user.id if hasattr(user, "id") else int(user["id"])
    full_name = user.full_name if hasattr(user, "full_name") else user.get("full_name", "Клиент")
    username  = getattr(user, "username", None) or (user.get("username") if isinstance(user, dict) else None)

    if action == "setup":
        biz = await db_upsert_biz(
            uid,
            name=data.get("name", "Мой бизнес")[:100],
            description=data.get("description", "")[:500],
            services=data.get("services", "")[:1000],
            address=data.get("address", "")[:200],
            phone=data.get("phone", "")[:50],
            working_hours=data.get("hours", "")[:100],
        )
        me = await bot.get_me()
        link = f"https://t.me/{me.username}?start=biz_{biz['id']}"
        text = (
            f"🎉 <b>Бот для «{biz['name']}» готов!</b>\n\n"
            f"🔗 Ссылка для клиентов:\n<code>{link}</code>\n\n"
            "Разместите её в Instagram, 2GIS или на визитке."
        )
        try:
            await bot.send_message(uid, text, parse_mode="HTML",
                                    reply_markup=kb_owner_home(True))
        except Exception as e:
            log.error(f"send setup confirm: {e}")
        return {"ok": True, "biz_id": biz["id"], "link": link}

    if action == "booking":
        bid = int(data.get("business_id", 0))
        biz = await db_get_biz_by_id(bid)
        if not biz:
            return {"ok": False, "error": "business_not_found"}

        booking_id = await db_add_booking(
            bid, uid, full_name,
            data.get("service", "")[:200],
            data.get("date", ""),
            data.get("time", ""),
            data.get("note", "")[:500],
        )

        # Подтверждение клиенту
        try:
            await bot.send_message(
                uid,
                f"✅ <b>Запись принята!</b>\n\n"
                f"Услуга: {data.get('service')}\n"
                f"Дата: {data.get('date')} в {data.get('time')}\n\n"
                f"Ждите подтверждения от «{biz['name']}» 👍",
                parse_mode="HTML")
        except Exception as e:
            log.error(f"notify client: {e}")

        # Уведомление владельцу
        try:
            uname = f"@{username}" if username else "без username"
            await bot.send_message(
                biz["owner_id"],
                f"🔔 <b>Новая заявка #{booking_id}!</b>\n\n"
                f"👤 {full_name} ({uname})\n"
                f"💼 {data.get('service')}\n"
                f"📅 {data.get('date')} в {data.get('time')}\n"
                + (f"💬 {data.get('note')}\n" if data.get('note') else ""),
                parse_mode="HTML",
                reply_markup=kb_booking_admin(booking_id))
        except Exception as e:
            log.error(f"notify owner: {e}")

        return {"ok": True, "booking_id": booking_id}

    return {"ok": False, "error": "unknown_action"}


# ═══════════════════════ HTTP API (для WebApp в браузере) ═════
def cors_response(data: dict, status: int = 200) -> web.Response:
    return web.json_response(data, status=status, headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
    })

async def http_health(request):
    return web.Response(text="OK")

async def http_options(request):
    return cors_response({})

async def http_setup(request: web.Request):
    try:
        data = await request.json()
        owner_id = int(data.get("owner_id", 0))
        if not owner_id:
            return cors_response({"ok": False, "error": "no_owner_id"}, 400)
        user = type("U", (), {"id": owner_id, "full_name": data.get("name", "Owner"),
                               "username": None})()
        result = await process_setup_or_booking(data, user, request.app["bot"])
        return cors_response(result)
    except Exception as e:
        log.error(f"http_setup: {e}")
        return cors_response({"ok": False, "error": str(e)}, 500)

async def http_booking(request: web.Request):
    try:
        data = await request.json()
        user_id = int(data.get("user_id", 0))
        if not user_id:
            return cors_response({"ok": False, "error": "no_user_id"}, 400)
        user = type("U", (), {"id": user_id,
                               "full_name": data.get("user_name", "Клиент"),
                               "username": data.get("username")})()
        result = await process_setup_or_booking(data, user, request.app["bot"])
        return cors_response(result)
    except Exception as e:
        log.error(f"http_booking: {e}")
        return cors_response({"ok": False, "error": str(e)}, 500)

async def http_get_biz(request: web.Request):
    try:
        bid = int(request.match_info["bid"])
        biz = await db_get_biz_by_id(bid)
        if not biz:
            return cors_response({"ok": False}, 404)
        return cors_response({"ok": True, "biz": {
            "id": biz["id"], "name": biz["name"],
            "description": biz["description"], "services": biz["services"],
            "address": biz["address"], "phone": biz["phone"],
            "working_hours": biz["working_hours"],
        }})
    except Exception as e:
        return cors_response({"ok": False, "error": str(e)}, 500)

async def http_get_owner_data(request: web.Request):
    """Владелец открывает панель — отдаём его текущие данные + заявки + статистику."""
    try:
        owner_id = int(request.match_info["owner_id"])
        biz = await db_get_biz_by_owner(owner_id)
        if not biz:
            return cors_response({"ok": True, "biz": None})
        stats    = await db_get_stats(biz["id"])
        bookings = await db_get_bookings(biz["id"], limit=30)
        me = await request.app["bot"].get_me()
        return cors_response({
            "ok": True,
            "biz": {k: biz[k] for k in
                    ("id","name","description","services","address","phone","working_hours")},
            "stats": stats,
            "bookings": bookings,
            "link": f"https://t.me/{me.username}?start=biz_{biz['id']}",
        })
    except Exception as e:
        return cors_response({"ok": False, "error": str(e)}, 500)

async def http_booking_status(request: web.Request):
    try:
        data = await request.json()
        booking_id = int(data["booking_id"])
        status = data["status"]
        if status not in ("confirmed", "cancelled"):
            return cors_response({"ok": False, "error": "bad_status"}, 400)
        await db_set_booking_status(booking_id, status)
        return cors_response({"ok": True})
    except Exception as e:
        return cors_response({"ok": False, "error": str(e)}, 500)


def build_web_app(bot: Bot) -> web.Application:
    app = web.Application()
    app["bot"] = bot
    app.router.add_get ("/health",                 http_health)
    app.router.add_route("OPTIONS", "/{tail:.*}",  http_options)
    app.router.add_post("/api/setup",              http_setup)
    app.router.add_post("/api/booking",            http_booking)
    app.router.add_post("/api/booking_status",     http_booking_status)
    app.router.add_get ("/api/biz/{bid}",          http_get_biz)
    app.router.add_get ("/api/owner/{owner_id}",   http_get_owner_data)
    return app


# ═══════════════════════ MAIN ═════════════════════════════════
async def main():
    await db_init()

    bot = Bot(token=BOT_TOKEN)
    dp  = Dispatcher(storage=MemoryStorage())
    await bot.delete_webhook(drop_pending_updates=True)

    # Команды
    dp.message.register(cmd_start, Command("start"))

    # WebApp данные (когда открыт через кнопку бота)
    dp.message.register(on_webapp_data, F.web_app_data)

    # AI-чат с клиентами
    dp.message.register(on_text, F.text)

    # Owner callbacks
    dp.callback_query.register(cb_owner_home,     F.data == "owner:home")
    dp.callback_query.register(cb_owner_bookings, F.data == "owner:bookings")
    dp.callback_query.register(cb_owner_stats,    F.data == "owner:stats")
    dp.callback_query.register(cb_owner_link,     F.data == "owner:link")
    dp.callback_query.register(cb_booking_set_status, F.data.startswith("bk:"))

    # Client callbacks
    dp.callback_query.register(cb_client_ask,      F.data.startswith("client:ask:"))
    dp.callback_query.register(cb_client_contacts, F.data.startswith("client:contacts:"))

    # HTTP API сервер (для WebApp в обычном браузере + health-check для Railway)
    app = build_web_app(bot)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info(f"HTTP API запущен на порту {PORT}")

    log.info("✅ Business Bot запущен и готов к работе!")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())
