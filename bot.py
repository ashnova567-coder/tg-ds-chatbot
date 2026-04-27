import os
import sqlite3
import json
import random
import asyncio
import logging
import time
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, User
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from telegram.constants import ParseMode
from telegram.error import TelegramError, NetworkError, TimedOut, RetryAfter
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pytz

os.environ['HTTPX_TIMEOUT'] = '30.0'
# ==================== НАСТРОЙКИ ====================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ID администратора (получить у @userinfobot)
ADMIN_ID = 123456789  # ← ЗАМЕНИ НА СВОЙ ID!

# Токен бота
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# Константы
SEND_COMMISSION = 0.01
REP_PLUS_COST = 1
REP_MINUS_COST = 10

RANKS = [
    (0, "🌱 Пиздюк"),
    (500, "🗣 Пиздун"),
    (1500, "✨ Кайфун"),
    (5000, "🎙 Магистр пиздежа"),
    (10000, "👑 Легенда пиздежа"),
]

DB_FILE = 'bot_database.db'

# ==================== БАЗА ДАННЫХ SQLite ====================

def get_db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db():
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.executescript('''
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            username TEXT DEFAULT '',
            first_name TEXT DEFAULT '',
            account TEXT UNIQUE,
            rys INTEGER DEFAULT 100,
            rep INTEGER DEFAULT 0,
            exp INTEGER DEFAULT 0,
            total_messages INTEGER DEFAULT 0,
            duels_won INTEGER DEFAULT 0,
            duels_lost INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS bank (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            total_commission INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS bank_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            amount INTEGER NOT NULL,
            reason TEXT DEFAULT '',
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS cases (
            case_id TEXT PRIMARY KEY,
            chat_id TEXT,
            bet INTEGER,
            prize INTEGER,
            win_index INTEGER,
            opened TEXT DEFAULT '[]',
            creator_id TEXT,
            creator_name TEXT,
            opponent_id TEXT,
            opponent_name TEXT,
            current_turn TEXT,
            status TEXT DEFAULT 'waiting',
            message_id INTEGER,
            winner_id TEXT,
            winner_name TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            finished_at TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS duels (
            duel_id TEXT PRIMARY KEY,
            chat_id TEXT,
            challenger_id TEXT,
            challenger_name TEXT,
            bet INTEGER,
            prize INTEGER,
            opponent_id TEXT,
            opponent_name TEXT,
            status TEXT DEFAULT 'waiting',
            message_id INTEGER,
            winner_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            finished_at TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS weekly_stats (
            user_id TEXT PRIMARY KEY,
            messages INTEGER DEFAULT 0,
            week_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS weekly_reset (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_reset TIMESTAMP
        );
    ''')
    
    cursor.execute("INSERT OR IGNORE INTO bank (id, total_commission) VALUES (1, 0)")
    conn.commit()
    conn.close()
    logger.info("База данных SQLite инициализирована")

init_db()

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

def generate_account_number():
    conn = get_db()
    existing = {row['account'] for row in conn.execute("SELECT account FROM users").fetchall()}
    conn.close()
    
    while True:
        acc = f"GESH-{random.randint(1000, 9999)}"
        if acc not in existing:
            return acc

def get_user(user_id):
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE user_id = ?", (str(user_id),)).fetchone()
    
    if not user:
        account = generate_account_number()
        conn.execute(
            "INSERT INTO users (user_id, account) VALUES (?, ?)",
            (str(user_id), account)
        )
        conn.commit()
        user = conn.execute("SELECT * FROM users WHERE user_id = ?", (str(user_id),)).fetchone()
        logger.info(f"Новый пользователь: {user_id}")
    
    conn.close()
    return dict(user)

def ensure_user(user_id, username=None, first_name=None):
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE user_id = ?", (str(user_id),)).fetchone()
    
    if not user:
        account = generate_account_number()
        conn.execute(
            "INSERT INTO users (user_id, username, first_name, account) VALUES (?, ?, ?, ?)",
            (str(user_id), username or '', first_name or '', account)
        )
        conn.commit()
        logger.info(f"Авто-регистрация: {user_id} ({first_name})")
    elif username or first_name:
        updates = {}
        if username: updates['username'] = username
        if first_name: updates['first_name'] = first_name
        if updates:
            sets = ', '.join(f"{k} = ?" for k in updates)
            values = list(updates.values()) + [str(user_id)]
            conn.execute(f"UPDATE users SET {sets} WHERE user_id = ?", values)
            conn.commit()
    
    conn.close()
    return True

def update_user(user_id, **kwargs):
    if not kwargs: return
    conn = get_db()
    sets = ', '.join(f"{k} = ?" for k in kwargs)
    values = list(kwargs.values()) + [str(user_id)]
    conn.execute(f"UPDATE users SET {sets} WHERE user_id = ?", values)
    conn.commit()
    conn.close()

def get_rank(exp):
    current = RANKS[0][1]
    for threshold, rank in RANKS:
        if exp >= threshold: current = rank
    return current

def find_user_by_account(account):
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE account = ?", (account,)).fetchone()
    conn.close()
    return dict(user) if user else None

def add_to_bank(amount, reason="unknown"):
    conn = get_db()
    conn.execute("UPDATE bank SET total_commission = total_commission + ?, updated_at = CURRENT_TIMESTAMP WHERE id = 1", (amount,))
    conn.execute("INSERT INTO bank_history (amount, reason) VALUES (?, ?)", (amount, reason))
    conn.commit()
    conn.close()

def get_bank_total():
    conn = get_db()
    row = conn.execute("SELECT total_commission FROM bank WHERE id = 1").fetchone()
    conn.close()
    return row['total_commission'] if row else 0

def get_weekly_messages():
    conn = get_db()
    rows = conn.execute("SELECT user_id, messages FROM weekly_stats").fetchall()
    conn.close()
    return {row['user_id']: row['messages'] for row in rows}

def increment_weekly_message(user_id):
    conn = get_db()
    conn.execute(
        "INSERT INTO weekly_stats (user_id, messages, week_start) VALUES (?, 1, CURRENT_TIMESTAMP) "
        "ON CONFLICT(user_id) DO UPDATE SET messages = messages + 1",
        (str(user_id),)
    )
    conn.commit()
    conn.close()

# ==================== ОСНОВНЫЕ КОМАНДЫ ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user(update.effective_user.id, username=update.effective_user.username, first_name=update.effective_user.first_name)
    user = get_user(update.effective_user.id)
    bank_total = get_bank_total()
    
    await update.message.reply_text(
        f"👋 Привет, {update.effective_user.first_name}!\n\n"
        f"📊 Твой счет: `{user['account']}`\n"
        f"💰 Баланс: {user['rys']} RYS\n"
        f"⭐ Репутация: {user['rep']}\n"
        f"✨ EXP: {user['exp']}\n"
        f"🎖 Ранг: {get_rank(user['exp'])}\n"
        f"🏦 Банк комиссий: {bank_total} RYS\n\n"
        f"Используй /s_help для списка команд",
        parse_mode=ParseMode.MARKDOWN
    )

async def s_help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = """
📋 **КОМАНДЫ БОТА**
━━━━━━━━━━━━━━━━

💰 **Финансы:**
• `/send GESH-XXXX [сумма]` — перевод RYS (комиссия 1% в банк)
• `/balance` — проверить баланс

🎲 **Кейс-дуэль:**
• `/case [ставка]` — создать игру (10 кейсов, 1 приз x2)
• Кнопки: Принять / Отклонить

⚔️ **Дуэль:**
• `/duel [ставка]` — создать дуэль
• Кнопки: Принять / Отклонить

⭐ **Репутация (ответ на сообщение):**
• `+rep` — +1 rep (1 RYS → в банк)
• `-rep` — -1 rep (10 RYS → в банк)

📊 **Статистика:**
• `/stats` — полная статистика
• `/top` — недельный топ

🏆 **Еженедельно (пн 00:00 UTC):**
• Топ-10 по сообщениям получают EXP
• Топ-3 также делят банк: 🥇50% 🥈30% 🥉20%

🎖 **Ранги (только через недельный топ):**
• 🌱 Пиздюк — 0 XP
• 🗣 Пиздун — 500 XP
• ✨ Кайфун — 1500 XP
• 🎙 Магистр пиздежа — 5000 XP
• 👑 Легенда пиздежа — 10000 XP
"""
    await update.message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN)

async def balance_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    bank_total = get_bank_total()
    await update.message.reply_text(
        f"💰 Твой баланс: **{user['rys']} RYS**\n"
        f"📊 Счет: `{user['account']}`\n"
        f"🏦 В общем банке: {bank_total} RYS",
        parse_mode=ParseMode.MARKDOWN
    )

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    rank = get_rank(user['exp'])
    weekly = get_weekly_messages()
    weekly_count = weekly.get(str(update.effective_user.id), 0)
    
    next_rank = ""
    for threshold, rank_name in RANKS[1:]:
        if user['exp'] < threshold:
            next_rank = f"\n📈 До следующего ранга: {threshold - user['exp']} XP"
            break
    
    stats_text = (
        f"📊 **СТАТИСТИКА ИГРОКА**\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"👤 Имя: {user['first_name']}\n"
        f"🆔 ID: `{user['user_id']}`\n"
        f"📊 Счет: `{user['account']}`\n"
        f"💰 Баланс: {user['rys']} RYS\n"
        f"⭐ Репутация: {user['rep']}\n"
        f"✨ EXP: {user['exp']}\n"
        f"🎖 Ранг: {rank}{next_rank}\n"
        f"⚔️ Побед в дуэлях: {user['duels_won']}\n"
        f"💀 Поражений в дуэлях: {user['duels_lost']}\n"
        f"💬 Сообщений за неделю: {weekly_count}\n"
        f"📝 Всего сообщений: {user['total_messages']}"
    )
    await update.message.reply_text(stats_text, parse_mode=ParseMode.MARKDOWN)

async def top_weekly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    weekly = get_weekly_messages()
    
    if not weekly:
        await update.message.reply_text("📊 Пока нет данных за эту неделю. Начните общаться!")
        return
    
    sorted_users = sorted(weekly.items(), key=lambda x: x[1], reverse=True)[:10]
    medals = ["🥇", "🥈", "🥉"] + [f"{i}." for i in range(4, 11)]
    
    top_text = "📊 **НЕДЕЛЬНЫЙ ТОП**\n━━━━━━━━━━━━━━━━\n"
    for i, (uid, count) in enumerate(sorted_users):
        user = get_user(uid)
        top_text += f"{medals[i]} {user['first_name']}: {count} сообщ.\n"
    
    bank_total = get_bank_total()
    top_text += f"\n━━━━━━━━━━━━━━━━\n🏦 Банк комиссий: {bank_total} RYS\n🎁 Топ-3 разделят банк в понедельник!"
    
    await update.message.reply_text(top_text, parse_mode=ParseMode.MARKDOWN)

# ==================== ПЕРЕВОДЫ ====================

async def send_rys(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sender = get_user(update.effective_user.id)
    
    if not context.args or len(context.args) != 2:
        await update.message.reply_text("❌ Используй: /send GESH-XXXX [сумма]")
        return
    
    account, amount_str = context.args[0], context.args[1]
    try:
        amount = int(amount_str)
        if amount <= 0: raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Сумма должна быть положительным целым числом")
        return
    
    if sender['rys'] < amount:
        await update.message.reply_text(f"❌ Недостаточно RYS. Твой баланс: {sender['rys']}")
        return
    
    target = find_user_by_account(account)
    if not target:
        await update.message.reply_text("❌ Получатель не найден")
        return
    
    if target['user_id'] == str(update.effective_user.id):
        await update.message.reply_text("❌ Нельзя отправить RYS самому себе")
        return
    
    commission = max(int(amount * SEND_COMMISSION), 1)
    received = amount - commission
    
    update_user(update.effective_user.id, rys=sender['rys'] - amount)
    update_user(target['user_id'], rys=target['rys'] + received)
    add_to_bank(commission, f"Перевод {sender['account']} → {account}")
    
    await update.message.reply_text(
        f"✅ **ПЕРЕВОД ВЫПОЛНЕН**\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"📤 Отправлено: {amount} RYS\n"
        f"💸 Комиссия (1%): {commission} RYS → в банк\n"
        f"📥 Получено: {received} RYS\n"
        f"👤 Получатель: {target['first_name']} ({account})\n"
        f"💰 Твой баланс: {sender['rys'] - amount} RYS",
        parse_mode=ParseMode.MARKDOWN
    )

# ==================== РЕПУТАЦИЯ ====================

async def rep_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        await update.message.reply_text("❌ Ответь на сообщение пользователя")
        return
    
    text = update.message.text.strip()
    is_plus = text.startswith('+rep')
    is_minus = text.startswith('-rep')
    
    if not (is_plus or is_minus): return
    
    sender = get_user(update.effective_user.id)
    target = get_user(update.message.reply_to_message.from_user.id)
    
    if sender['user_id'] == target['user_id']:
        await update.message.reply_text("❌ Нельзя изменять репутацию самому себе")
        return
    
    parts = text.split()
    value = int(parts[1]) if len(parts) > 1 and parts[1].lstrip('-').isdigit() else 1
    if value <= 0:
        await update.message.reply_text("❌ Значение должно быть положительным")
        return
    
    if is_plus:
        cost = value
        if sender['rys'] < cost:
            await update.message.reply_text(f"❌ Недостаточно RYS. Нужно: {cost}")
            return
        update_user(sender['user_id'], rys=sender['rys'] - cost)
        update_user(target['user_id'], rep=target['rep'] + value)
        add_to_bank(cost, f"+rep {sender['first_name']} → {target['first_name']}")
        await update.message.reply_text(f"✅ +{value} rep → {target['first_name']}\n💸 {cost} RYS в банк")
    else:
        cost = value * REP_MINUS_COST
        if sender['rys'] < cost:
            await update.message.reply_text(f"❌ Недостаточно RYS. Нужно: {cost}")
            return
        update_user(sender['user_id'], rys=sender['rys'] - cost)
        update_user(target['user_id'], rep=target['rep'] - value)
        add_to_bank(cost, f"-rep {sender['first_name']} → {target['first_name']}")
        await update.message.reply_text(f"👎 -{value} rep → {target['first_name']}\n💸 {cost} RYS в банк")

# ==================== КЕЙС-ДУЭЛЬ (ИНЛАЙН) ====================

async def case_game(update: Update, context: ContextTypes.DEFAULT_TYPE):
    challenger = get_user(update.effective_user.id)
    chat_id = str(update.effective_chat.id)
    
    if not context.args:
        await update.message.reply_text("❌ Укажи ставку: /case [сумма]")
        return
    
    try:
        bet = int(context.args[0])
        if bet <= 0: raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Ставка должна быть положительным целым числом")
        return
    
    if challenger['rys'] < bet:
        await update.message.reply_text(f"❌ Недостаточно RYS. Баланс: {challenger['rys']}")
        return
    
    conn = get_db()
    active = conn.execute("SELECT case_id FROM cases WHERE chat_id = ? AND status IN ('waiting', 'active')", (chat_id,)).fetchone()
    conn.close()
    
    if active:
        await update.message.reply_text("❌ В чате уже есть активный кейс!")
        return
    
    case_id = f"case_{chat_id}_{datetime.now().timestamp()}"
    prize = bet * 2
    win_index = random.randint(0, 9)
    
    conn = get_db()
    conn.execute(
        "INSERT INTO cases (case_id, chat_id, bet, prize, win_index, opened, creator_id, creator_name, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'waiting')",
        (case_id, chat_id, bet, prize, win_index, json.dumps([False]*10), str(update.effective_user.id), challenger['first_name'])
    )
    conn.commit()
    conn.close()
    
    update_user(update.effective_user.id, rys=challenger['rys'] - bet)
    
    keyboard = [
        [InlineKeyboardButton("✅ Принять", callback_data=f"case_accept_{case_id}"),
         InlineKeyboardButton("❌ Отклонить", callback_data=f"case_decline_{case_id}")],
        [InlineKeyboardButton("ℹ️ Правила", callback_data=f"case_info_{case_id}")]
    ]
    
    msg = await update.message.reply_text(
        f"🎲 **КЕЙС-ДУЭЛЬ**\n━━━━━━━━━━━━━━━━\n"
        f"👤 Вызов от: {challenger['first_name']}\n"
        f"💵 Ставка: {bet} RYS\n"
        f"🏆 Приз: {prize} RYS (x2)\n"
        f"🎮 10 кейсов • 1 приз\n━━━━━━━━━━━━━━━━\n⏳ Ожидание противника...",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN
    )
    
    conn = get_db()
    conn.execute("UPDATE cases SET message_id = ? WHERE case_id = ?", (msg.message_id, case_id))
    conn.commit()
    conn.close()

async def case_accept_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    case_id = query.data.split('_')[2]
    user_id = str(query.from_user.id)
    
    conn = get_db()
    case = conn.execute("SELECT * FROM cases WHERE case_id = ?", (case_id,)).fetchone()
    
    if not case or case['status'] != 'waiting':
        await query.answer("❌ Кейс-дуэль не активна", show_alert=True)
        conn.close()
        return
    
    case = dict(case)
    
    if user_id == case['creator_id']:
        await query.answer("❌ Нельзя принять свой вызов", show_alert=True)
        conn.close()
        return
    
    opponent = get_user(user_id)
    
    if opponent['rys'] < case['bet']:
        await query.answer(f"❌ Недостаточно RYS. Нужно: {case['bet']}", show_alert=True)
        conn.close()
        return
    
    await query.answer("⚔️ Кейс-дуэль начинается!")
    
    update_user(user_id, rys=opponent['rys'] - case['bet'])
    current_turn = random.choice(['creator', 'opponent'])
    
    conn.execute("UPDATE cases SET opponent_id = ?, opponent_name = ?, status = 'active', current_turn = ? WHERE case_id = ?",
                 (user_id, opponent['first_name'], current_turn, case_id))
    conn.commit()
    conn.close()
    
    first_player_id = case['creator_id'] if current_turn == 'creator' else user_id
    first_player = get_user(first_player_id)
    
    keyboard = build_case_buttons(case, case_id)
    
    await query.edit_message_text(
        f"🎲 **КЕЙС-ДУЭЛЬ**\n━━━━━━━━━━━━━━━━\n"
        f"👤 {case['creator_name']} 🆚 {opponent['first_name']}\n"
        f"💵 Ставка: {case['bet']} RYS\n🏆 Приз: {case['prize']} RYS\n"
        f"🎮 Открыто: 0/10\n━━━━━━━━━━━━━━━━\n"
        f"👤 Ход: {first_player['first_name']}\n\n⚡ Последний кейс — 100% победа!",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN
    )

async def case_decline_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    case_id = query.data.split('_')[2]
    user_id = str(query.from_user.id)
    
    conn = get_db()
    case = conn.execute("SELECT * FROM cases WHERE case_id = ?", (case_id,)).fetchone()
    
    if not case or case['status'] != 'waiting':
        await query.answer("❌ Нельзя отменить", show_alert=True)
        conn.close()
        return
    
    case = dict(case)
    
    if user_id != case['creator_id']:
        await query.answer("❌ Только создатель может отменить", show_alert=True)
        conn.close()
        return
    
    creator = get_user(case['creator_id'])
    update_user(case['creator_id'], rys=creator['rys'] + case['bet'])
    
    conn.execute("UPDATE cases SET status = 'declined' WHERE case_id = ?", (case_id,))
    conn.commit()
    conn.close()
    
    await query.edit_message_text(
        f"❌ **КЕЙС-ДУЭЛЬ ОТМЕНЕНА**\n━━━━━━━━━━━━━━━━\n"
        f"👤 {case['creator_name']} отменил вызов\n💰 Ставка {case['bet']} RYS возвращена",
        parse_mode=ParseMode.MARKDOWN
    )

def build_case_buttons(case, case_id):
    opened = json.loads(case['opened']) if isinstance(case['opened'], str) else case['opened']
    remaining = 10 - sum(opened)
    
    keyboard = []
    for i in range(0, 10, 5):
        row = []
        for j in range(i, i+5):
            if opened[j]:
                row.append(InlineKeyboardButton(f"❌ {j+1}", callback_data="case_none"))
            else:
                emoji = "🎯" if remaining == 1 else "🎁"
                row.append(InlineKeyboardButton(f"{emoji} {j+1}", callback_data=f"case_open_{case_id}_{j}"))
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("ℹ️ Правила", callback_data=f"case_info_{case_id}")])
    return keyboard

async def handle_case_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    parts = query.data.split('_')
    case_id = parts[2]
    box_index = int(parts[3])
    user_id = str(query.from_user.id)
    
    conn = get_db()
    case = conn.execute("SELECT * FROM cases WHERE case_id = ?", (case_id,)).fetchone()
    
    if not case or case['status'] != 'active':
        await query.answer("❌ Кейс-дуэль завершена", show_alert=True)
        conn.close()
        return
    
    case = dict(case)
    
    if user_id not in [case['creator_id'], case['opponent_id']]:
        await query.answer("❌ Ты не участвуешь", show_alert=True)
        conn.close()
        return
    
    current_player_id = case['creator_id'] if case['current_turn'] == 'creator' else case['opponent_id']
    
    if user_id != current_player_id:
        current_player = get_user(current_player_id)
        await query.answer(f"⏳ Ход {current_player['first_name']}. Ожидай!", show_alert=True)
        conn.close()
        return
    
    opened = json.loads(case['opened']) if isinstance(case['opened'], str) else case['opened']
    
    if opened[box_index]:
        await query.answer("❌ Уже открыт!", show_alert=True)
        conn.close()
        return
    
    remaining_before = 10 - sum(opened)
    opened[box_index] = True
    
    user = get_user(user_id)
    
    is_last = (remaining_before == 1)
    is_win = (box_index == case['win_index'])
    
    if is_last or is_win:
        update_user(user_id, rys=user['rys'] + case['prize'])
        
        conn.execute(
            "UPDATE cases SET opened = ?, status = 'finished', winner_id = ?, winner_name = ?, finished_at = CURRENT_TIMESTAMP WHERE case_id = ?",
            (json.dumps(opened), user_id, query.from_user.first_name, case_id)
        )
        conn.commit()
        conn.close()
        
        await query.answer(f"🎉 ПОБЕДА! +{case['prize']} RYS!", show_alert=True)
        
        keyboard = build_final_case_buttons(case, opened, box_index)
        
        if is_last and not is_win:
            win_desc = "последний кейс (100% победа)"
        elif is_win and is_last:
            win_desc = "победный кейс (последний)"
        else:
            win_desc = f"победный кейс №{box_index + 1}"
        
        await query.edit_message_text(
            f"🎲 **КЕЙС-ДУЭЛЬ ЗАВЕРШЕНА**\n━━━━━━━━━━━━━━━━\n"
            f"👤 {case['creator_name']} 🆚 {case['opponent_name']}\n"
            f"💵 Ставка: {case['bet']} RYS\n━━━━━━━━━━━━━━━━\n"
            f"🏆 Победитель: **{query.from_user.first_name}**\n"
            f"🎯 {win_desc}\n💰 Выигрыш: {case['prize']} RYS\n"
            f"🎮 Открыто: {sum(opened)}/10\n━━━━━━━━━━━━━━━━\n"
            f"💡 /case [ставка] — новая дуэль",
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN
        )
    else:
        new_turn = 'opponent' if case['current_turn'] == 'creator' else 'creator'
        remaining = 10 - sum(opened)
        
        conn.execute("UPDATE cases SET opened = ?, current_turn = ? WHERE case_id = ?",
                     (json.dumps(opened), new_turn, case_id))
        conn.commit()
        conn.close()
        
        current_player = get_user(case['creator_id'] if new_turn == 'creator' else case['opponent_id'])
        
        await query.answer(f"📦 Пусто! Ход {current_player['first_name']}", show_alert=True)
        
        keyboard = build_case_buttons({**case, 'opened': opened}, case_id)
        hint = "\n⚠️ Последний кейс — 100% победа!" if remaining == 1 else ""
        
        await query.edit_message_text(
            f"🎲 **КЕЙС-ДУЭЛЬ**\n━━━━━━━━━━━━━━━━\n"
            f"👤 {case['creator_name']} 🆚 {case['opponent_name']}\n"
            f"💵 Ставка: {case['bet']} RYS\n🏆 Приз: {case['prize']} RYS\n"
            f"🎮 Открыто: {sum(opened)}/10 (осталось {remaining})\n━━━━━━━━━━━━━━━━\n"
            f"👤 Ход: {current_player['first_name']}{hint}",
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN
        )

def build_final_case_buttons(case, opened, win_box):
    keyboard = []
    for i in range(0, 10, 5):
        row = []
        for j in range(i, i+5):
            if j == case['win_index']:
                emoji = "🎉" if j == win_box else "💎"
            elif opened[j]:
                emoji = "❌"
            else:
                emoji = "🎁"
            row.append(InlineKeyboardButton(f"{emoji} {j+1}", callback_data="none"))
        keyboard.append(row)
    return keyboard

async def case_info_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer(
        "🎲 ПРАВИЛА КЕЙС-ДУЭЛИ:\n\n"
        "• Два игрока ставят одинаковую сумму\n"
        "• Призовой фонд = ставка × 2\n"
        "• 1 из 10 кейсов содержит приз\n"
        "• Игроки открывают по очереди\n"
        "• Кто нашёл приз — забирает всё!\n"
        "• Последний кейс = 100% победа",
        show_alert=True
    )

# ==================== ДУЭЛЬ (ИНЛАЙН) ====================

async def duel_create(update: Update, context: ContextTypes.DEFAULT_TYPE):
    challenger = get_user(update.effective_user.id)
    chat_id = str(update.effective_chat.id)
    
    if not context.args:
        await update.message.reply_text("❌ Укажи ставку: /duel [сумма]")
        return
    
    try:
        bet = int(context.args[0])
        if bet <= 0: raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Ставка должна быть положительным целым числом")
        return
    
    if challenger['rys'] < bet:
        await update.message.reply_text(f"❌ Недостаточно RYS. Баланс: {challenger['rys']}")
        return
    
    conn = get_db()
    active = conn.execute("SELECT duel_id FROM duels WHERE chat_id = ? AND status = 'waiting'", (chat_id,)).fetchone()
    
    if active:
        await update.message.reply_text("❌ В чате уже есть активная дуэль")
        conn.close()
        return
    
    duel_id = f"duel_{chat_id}_{datetime.now().timestamp()}"
    
    conn.execute(
        "INSERT INTO duels (duel_id, chat_id, challenger_id, challenger_name, bet, prize) VALUES (?, ?, ?, ?, ?, ?)",
        (duel_id, chat_id, str(update.effective_user.id), challenger['first_name'], bet, bet * 2)
    )
    conn.commit()
    conn.close()
    
    update_user(update.effective_user.id, rys=challenger['rys'] - bet)
    
    keyboard = [
        [InlineKeyboardButton("⚔️ Принять", callback_data=f"duel_accept_{duel_id}"),
         InlineKeyboardButton("❌ Отклонить", callback_data=f"duel_decline_{duel_id}")],
        [InlineKeyboardButton("ℹ️ Правила", callback_data=f"duel_info_{duel_id}")]
    ]
    
    msg = await update.message.reply_text(
        f"⚔️ **ДУЭЛЬ**\n━━━━━━━━━━━━━━━━\n"
        f"👤 Вызов от: {challenger['first_name']}\n"
        f"💵 Ставка: {bet} RYS\n🏆 Призовой фонд: {bet * 2} RYS\n"
        f"━━━━━━━━━━━━━━━━\n⏳ Ожидание противника...",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN
    )
    
    conn = get_db()
    conn.execute("UPDATE duels SET message_id = ? WHERE duel_id = ?", (msg.message_id, duel_id))
    conn.commit()
    conn.close()

async def duel_accept_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    duel_id = query.data.split('_')[2]
    user_id = str(query.from_user.id)
    
    conn = get_db()
    duel = conn.execute("SELECT * FROM duels WHERE duel_id = ?", (duel_id,)).fetchone()
    
    if not duel or duel['status'] != 'waiting':
        await query.answer("❌ Дуэль не активна", show_alert=True)
        conn.close()
        return
    
    duel = dict(duel)
    
    if user_id == duel['challenger_id']:
        await query.answer("❌ Нельзя принять свой вызов", show_alert=True)
        conn.close()
        return
    
    opponent = get_user(user_id)
    
    if opponent['rys'] < duel['bet']:
        await query.answer(f"❌ Недостаточно RYS. Нужно: {duel['bet']}", show_alert=True)
        conn.close()
        return
    
    await query.answer("⚔️ Дуэль!")
    
    update_user(user_id, rys=opponent['rys'] - duel['bet'])
    challenger = get_user(duel['challenger_id'])
    
    total_weight = challenger['exp'] + opponent['exp'] + 100
    challenger_chance = (challenger['exp'] + 50) / total_weight
    
    winner_id = duel['challenger_id'] if random.random() < challenger_chance else user_id
    loser_id = user_id if winner_id == duel['challenger_id'] else duel['challenger_id']
    
    winner = get_user(winner_id)
    loser = get_user(loser_id)
    
    update_user(winner_id, rys=winner['rys'] + duel['prize'], duels_won=winner['duels_won'] + 1)
    update_user(loser_id, duels_lost=loser['duels_lost'] + 1)
    
    conn.execute(
        "UPDATE duels SET opponent_id = ?, opponent_name = ?, status = 'finished', winner_id = ?, finished_at = CURRENT_TIMESTAMP WHERE duel_id = ?",
        (user_id, opponent['first_name'], winner_id, duel_id)
    )
    conn.commit()
    conn.close()
    
    events = ["💥 Мощный удар в челюсть!", "🎯 Точный выстрел!", "👊👊👊 Серия ударов!",
              "🔄 Уворот и контратака!", "💫 Нокаутирующий удар!", "⚡ Удар молнии!", "🌪 Вихрь атак!"]
    event = random.choice(events)
    
    await query.edit_message_text(
        f"⚔️ **ДУЭЛЬ ЗАВЕРШЕНА**\n━━━━━━━━━━━━━━━━\n"
        f"👤 {challenger['first_name']} 🆚 {opponent['first_name']}\n"
        f"💵 Ставка: {duel['bet']} RYS\n━━━━━━━━━━━━━━━━\n{event}\n━━━━━━━━━━━━━━━━\n"
        f"🏆 Победитель: **{winner['first_name']}**\n💰 Выигрыш: {duel['prize']} RYS\n"
        f"🎖 Ранг: {get_rank(winner['exp'])}",
        parse_mode=ParseMode.MARKDOWN
    )

async def duel_decline_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    duel_id = query.data.split('_')[2]
    user_id = str(query.from_user.id)
    
    conn = get_db()
    duel = conn.execute("SELECT * FROM duels WHERE duel_id = ?", (duel_id,)).fetchone()
    
    if not duel or duel['status'] != 'waiting':
        await query.answer("❌ Нельзя отменить", show_alert=True)
        conn.close()
        return
    
    duel = dict(duel)
    
    if user_id != duel['challenger_id']:
        await query.answer("❌ Только создатель может отменить", show_alert=True)
        conn.close()
        return
    
    challenger = get_user(duel['challenger_id'])
    update_user(duel['challenger_id'], rys=challenger['rys'] + duel['bet'])
    
    conn.execute("UPDATE duels SET status = 'declined' WHERE duel_id = ?", (duel_id,))
    conn.commit()
    conn.close()
    
    await query.edit_message_text(
        f"❌ **ДУЭЛЬ ОТМЕНЕНА**\n━━━━━━━━━━━━━━━━\n"
        f"👤 {duel['challenger_name']} отменил вызов\n💰 Ставка {duel['bet']} RYS возвращена",
        parse_mode=ParseMode.MARKDOWN
    )

async def duel_info_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer(
        "⚔️ ПРАВИЛА ДУЭЛИ:\n\n• Два игрока ставят одинаковую сумму\n"
        "• Победитель забирает всё (x2)\n• Шанс зависит от EXP\n"
        "• У создателя небольшой бонус\n• Без комиссии!",
        show_alert=True
    )

# ==================== ПОДСЧЕТ СООБЩЕНИЙ + АВТО-РЕГИСТРАЦИЯ ====================

async def count_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    
    ensure_user(update.effective_user.id, username=update.effective_user.username, first_name=update.effective_user.first_name)
    
    if len(update.message.text) >= 2:
        user = get_user(update.effective_user.id)
        update_user(update.effective_user.id, total_messages=user['total_messages'] + 1)
        increment_weekly_message(update.effective_user.id)

# ==================== АДМИН-ПАНЕЛЬ ====================

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Недостаточно прав")
        return
    
    if update.message.reply_to_message:
        target = update.message.reply_to_message.from_user
        target_user = get_user(target.id)
        
        keyboard = [
            [InlineKeyboardButton("📊 Информация об игроке", callback_data=f"admin_info_{target.id}")],
            [InlineKeyboardButton("💰 +RYS", callback_data=f"admin_add_rys_{target.id}"),
             InlineKeyboardButton("💰 -RYS", callback_data=f"admin_sub_rys_{target.id}")],
            [InlineKeyboardButton("⭐ +REP", callback_data=f"admin_add_rep_{target.id}"),
             InlineKeyboardButton("⭐ -REP", callback_data=f"admin_sub_rep_{target.id}")],
            [InlineKeyboardButton("✨ +EXP", callback_data=f"admin_add_exp_{target.id}"),
             InlineKeyboardButton("✨ -EXP", callback_data=f"admin_sub_exp_{target.id}")],
            [InlineKeyboardButton("🗑 Удалить из БД", callback_data=f"admin_delete_{target.id}"),
             InlineKeyboardButton("➕ Создать в БД", callback_data=f"admin_add_user_{target.id}")]
        ]
        
        await update.message.reply_text(f"🛡 Админ-панель для {target.first_name}", reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        keyboard = [
            [InlineKeyboardButton("📋 Реестр пользователей", callback_data="admin_list")],
            [InlineKeyboardButton("📢 Рассылка", callback_data="admin_broadcast")],
            [InlineKeyboardButton("🏦 Информация о банке", callback_data="admin_bank_info")]
        ]
        await update.message.reply_text("🛡 **АДМИН-ПАНЕЛЬ**", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

async def admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.from_user.id != ADMIN_ID:
        await query.answer("❌ Нет доступа")
        return
    
    await query.answer()
    parts = query.data.split('_')
    action = parts[1]
    
    if action == 'info':
        target_id = parts[2]
        user = get_user(target_id)
        weekly = get_weekly_messages()
        info = (
            f"📊 **ИНФОРМАЦИЯ ОБ ИГРОКЕ**\n━━━━━━━━━━━━━━━━\n"
            f"👤 Имя: {user['first_name']}\n🆔 ID: `{user['user_id']}`\n"
            f"📊 Счет: `{user['account']}`\n💰 RYS: {user['rys']}\n"
            f"⭐ REP: {user['rep']}\n✨ EXP: {user['exp']}\n"
            f"🎖 Ранг: {get_rank(user['exp'])}\n"
            f"⚔️ Побед: {user['duels_won']} | 💀 Поражений: {user['duels_lost']}\n"
            f"💬 За неделю: {weekly.get(target_id, 0)}\n📝 Всего: {user['total_messages']}"
        )
        await query.edit_message_text(info, parse_mode=ParseMode.MARKDOWN)
    
    elif action in ['add', 'sub']:
        _, operation, currency, target_id = parts
        context.user_data['admin_action'] = {'target_id': target_id, 'currency': currency, 'operation': operation}
        await query.edit_message_text(f"✏️ Введи количество {currency.upper()}: {'+' if operation == 'add' else '-'}50")
    
    elif action == 'delete':
        target_id = parts[2]
        conn = get_db()
        user = conn.execute("SELECT first_name FROM users WHERE user_id = ?", (target_id,)).fetchone()
        if user:
            conn.execute("DELETE FROM users WHERE user_id = ?", (target_id,))
            conn.commit()
            await query.edit_message_text(f"✅ {user['first_name']} удален")
        else:
            await query.edit_message_text("❌ Пользователь не найден")
        conn.close()
    
    elif action == 'add':
        target_id = parts[2]
        user = get_user(target_id)
        await query.edit_message_text(f"✅ {user['first_name']} в БД (счет: {user['account']})")
    
    elif action == 'list':
        conn = get_db()
        users_list = conn.execute("SELECT * FROM users ORDER BY exp DESC").fetchall()
        conn.close()
        
        if not users_list:
            await query.edit_message_text("📋 База пуста")
            return
        
        page = context.user_data.get('admin_page', 0)
        total_pages = (len(users_list) - 1) // 10 + 1
        start, end = page * 10, (page + 1) * 10
        
        text = f"📋 **РЕЕСТР** ({page+1}/{total_pages})\n━━━━━━━━━━━━━━━━\n"
        for u in users_list[start:end]:
            text += f"👤 {u['first_name']} | 💰{u['rys']} | ✨{u['exp']} | 🎖{get_rank(u['exp'])}\n📊 {u['account']} | ⭐{u['rep']}\n───\n"
        
        keyboard = []
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"admin_page_{page-1}"))
        if page < total_pages - 1: nav.append(InlineKeyboardButton("➡️ Вперед", callback_data=f"admin_page_{page+1}"))
        if nav: keyboard.append(nav)
        keyboard.append([InlineKeyboardButton("🔙 В админ-панель", callback_data="admin_back")])
        
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    
    elif action == 'page':
        context.user_data['admin_page'] = int(parts[2])
        await admin_callback(update, context)
    
    elif action == 'back':
        keyboard = [
            [InlineKeyboardButton("📋 Реестр", callback_data="admin_list")],
            [InlineKeyboardButton("📢 Рассылка", callback_data="admin_broadcast")],
            [InlineKeyboardButton("🏦 Банк", callback_data="admin_bank_info")]
        ]
        await query.edit_message_text("🛡 **АДМИН-ПАНЕЛЬ**", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    
    elif action == 'bank':
        conn = get_db()
        total = conn.execute("SELECT total_commission FROM bank WHERE id = 1").fetchone()['total_commission']
        ops = conn.execute("SELECT COUNT(*) as cnt FROM bank_history").fetchone()['cnt']
        recent = conn.execute("SELECT * FROM bank_history ORDER BY id DESC LIMIT 5").fetchall()
        conn.close()
        
        text = f"🏦 **БАНК**: {total} RYS\n📊 Операций: {ops}\n\n📜 **Последние:**\n"
        for op in recent: text += f"• {op['reason']}: +{op['amount']} RYS\n"
        text += "\n🎁 Распределение: пн 00:00 UTC\n🥇50% 🥈30% 🥉20%"
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN)
    
    elif action == 'broadcast':
        context.user_data['awaiting_broadcast'] = True
        await query.edit_message_text("📢 Напиши сообщение для рассылки (/cancel — отмена)")

async def handle_admin_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    
    if update.message.text == '/cancel':
        context.user_data.pop('awaiting_broadcast', None)
        context.user_data.pop('admin_action', None)
        await update.message.reply_text("❌ Отменено")
        return
    
    if 'admin_action' in context.user_data:
        action = context.user_data['admin_action']
        try:
            amount = int(update.message.text)
        except ValueError:
            await update.message.reply_text("❌ Целое число"); return
        
        target = get_user(action['target_id'])
        field = action['currency']
        
        if action['operation'] == 'add':
            update_user(action['target_id'], **{field: target[field] + amount})
        else:
            update_user(action['target_id'], **{field: target[field] - amount})
        
        user = get_user(action['target_id'])
        await update.message.reply_text(
            f"✅ {user['first_name']}\n{field.upper()}: {'+' if action['operation'] == 'add' else ''}{amount}\n"
            f"💰 RYS: {user['rys']} | ⭐ REP: {user['rep']} | ✨ EXP: {user['exp']}\n🎖 {get_rank(user['exp'])}"
        )
        del context.user_data['admin_action']
        return
    
    if context.user_data.get('awaiting_broadcast'):
        msg = update.message.text
        sent = 0
        
        conn = get_db()
        users_list = conn.execute("SELECT user_id FROM users").fetchall()
        conn.close()
        
        for u in users_list:
            try:
                await context.bot.send_message(int(u['user_id']), f"📢 **Рассылка**\n\n{msg}", parse_mode=ParseMode.MARKDOWN)
                sent += 1
                await asyncio.sleep(0.5)
            except: pass
        
        await update.message.reply_text(f"✅ Отправлено {sent}/{len(users_list)}")
        del context.user_data['awaiting_broadcast']

# ==================== ЕЖЕНЕДЕЛЬНЫЙ СБРОС ====================

async def weekly_reset():
    logger.info("=== НАЧАЛО ЕЖЕНЕДЕЛЬНОГО СБРОСА ===")
    
    conn = get_db()
    weekly = conn.execute("SELECT user_id, messages FROM weekly_stats WHERE messages > 0").fetchall()
    
    if not weekly:
        logger.info("Нет сообщений за неделю")
        conn.close()
        return
    
    sorted_users = sorted(weekly, key=lambda x: x['messages'], reverse=True)
    
    exp_rewards = {1: 500, 2: 400, 3: 300, 4: 200, 5: 150, 6: 100, 7: 75, 8: 50, 9: 30, 10: 20}
    bank_dist = {1: 0.50, 2: 0.30, 3: 0.20}
    
    total_bank = conn.execute("SELECT total_commission FROM bank WHERE id = 1").fetchone()['total_commission']
    distributed = 0
    
    for i, (uid, count) in enumerate([(u['user_id'], u['messages']) for u in sorted_users[:10]], 1):
        exp = exp_rewards.get(i, 0)
        conn.execute("UPDATE users SET exp = exp + ? WHERE user_id = ?", (exp, uid))
        
        bank_reward = 0
        if i <= 3 and total_bank > 0:
            bank_reward = int(total_bank * bank_dist[i])
            conn.execute("UPDATE users SET rys = rys + ? WHERE user_id = ?", (bank_reward, uid))
            distributed += bank_reward
        
        try:
            msg = f"🏆 **ЕЖЕНЕДЕЛЬНЫЕ НАГРАДЫ**\n📊 Место: {i}\n💬 Сообщений: {count}\n✨ +{exp} EXP"
            if bank_reward > 0: msg += f"\n💰 +{bank_reward} RYS из банка"
            await context.bot.send_message(int(uid), msg, parse_mode=ParseMode.MARKDOWN)
        except: pass
    
    conn.execute("UPDATE bank SET total_commission = 0, updated_at = CURRENT_TIMESTAMP WHERE id = 1")
    if distributed > 0:
        conn.execute("INSERT INTO bank_history (amount, reason) VALUES (?, 'weekly_distribution')", (distributed,))
    
    conn.execute("DELETE FROM weekly_stats")
    conn.execute("INSERT OR REPLACE INTO weekly_reset (id, last_reset) VALUES (1, CURRENT_TIMESTAMP)")
    conn.commit()
    conn.close()
    
    logger.info(f"Сброс завершен. Распределено: {distributed} RYS из банка")

# ==================== ОБРАБОТЧИК ОШИБОК ====================

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    error = context.error
    
    if isinstance(error, NetworkError):
        logger.error(f"🌐 Сетевая ошибка: {error}")
    elif isinstance(error, TimedOut):
        logger.error(f"⏰ Таймаут: {error}")
    elif isinstance(error, RetryAfter):
        logger.warning(f"🚦 Flood control. Ждем {error.retry_after} сек")
        await asyncio.sleep(error.retry_after)
    elif isinstance(error, TelegramError):
        logger.error(f"📡 Ошибка Telegram: {error}")
    else:
        logger.error(f"❌ Ошибка: {error}", exc_info=True)

# ==================== ЗАПУСК ====================

from fastapi import FastAPI, Request

app = FastAPI()

@app.on_event("startup")
async def startup():
    if not TOKEN:
        logger.error(...)
        return
    
    from telegram import Bot
    from telegram.ext import ExtBot
    
    class FakeBot(ExtBot):
        async def initialize(self):
            self._bot_user = User(
                id=0,
                username='elitnoshluhburgstatsbot',
                first_name='ГЭШ БОТ',
                is_bot=True
            )
            self._initialized = True
        
        async def get_me(self):
            return self._bot_user
    
    bot = FakeBot(token=TOKEN)
    await bot.initialize()
    
    bot_app = Application.builder().token(TOKEN).build()
    bot_app.add_error_handler(error_handler)
    bot_app._bot = bot
    bot_app._initialized = True
    
    scheduler = AsyncIOScheduler(timezone=pytz.UTC)
    scheduler.add_job(weekly_reset, 'cron', day_of_week='mon', hour=0, minute=0)
    scheduler.start()
    
    bot_app.add_handler(CommandHandler("start", start))
    bot_app.add_handler(CommandHandler("s_help", s_help_command))
    bot_app.add_handler(CommandHandler("balance", balance_cmd))
    bot_app.add_handler(CommandHandler("send", send_rys))
    bot_app.add_handler(CommandHandler("case", case_game))
    bot_app.add_handler(CommandHandler("duel", duel_create))
    bot_app.add_handler(CommandHandler("stats", stats))
    bot_app.add_handler(CommandHandler("top", top_weekly))
    bot_app.add_handler(CommandHandler("admin", admin_panel))
    
    bot_app.add_handler(CallbackQueryHandler(case_accept_callback, pattern="^case_accept_"))
    bot_app.add_handler(CallbackQueryHandler(case_decline_callback, pattern="^case_decline_"))
    bot_app.add_handler(CallbackQueryHandler(handle_case_open, pattern="^case_open_"))
    bot_app.add_handler(CallbackQueryHandler(case_info_callback, pattern="^case_info_"))
    bot_app.add_handler(CallbackQueryHandler(duel_accept_callback, pattern="^duel_accept_"))
    bot_app.add_handler(CallbackQueryHandler(duel_decline_callback, pattern="^duel_decline_"))
    bot_app.add_handler(CallbackQueryHandler(duel_info_callback, pattern="^duel_info_"))
    bot_app.add_handler(CallbackQueryHandler(admin_callback, pattern="^admin_"))
    
    bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.REPLY, rep_command))
    bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, count_message))
    bot_app.add_handler(MessageHandler(filters.TEXT & filters.User(ADMIN_ID), handle_admin_text))
    
    app.state.bot_app = bot_app
    app.state.bot = bot
    logger.info("✅ Бот готов")

@app.on_event("shutdown")
async def shutdown():
    if hasattr(app.state, 'bot_app'):
        await app.state.bot_app.shutdown()

@app.post("/telegram")
async def telegram_webhook(request: Request):
    if hasattr(app.state, 'bot_app'):
        data = await request.json()
        update = Update.de_json(data, app.state.bot)
        await app.state.bot_app.process_update(update)
    return {"ok": True}