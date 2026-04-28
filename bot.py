import os
import sqlite3
import json
import random
import asyncio
import logging
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from telegram.error import TelegramError, NetworkError, TimedOut, RetryAfter
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pytz

# ==================== НАСТРОЙКИ ====================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

ADMIN_ID = 1055271488
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

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

# ==================== БАЗА ДАННЫХ ====================

def get_db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            username TEXT DEFAULT '',
            first_name TEXT DEFAULT '',
            account TEXT UNIQUE,
            rys INTEGER DEFAULT 100,
            rep INTEGER DEFAULT 0,
            exp INTEGER DEFAULT 0,
            total_messages INTEGER DEFAULT 0,
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
    conn.execute("INSERT OR IGNORE INTO bank (id, total_commission) VALUES (1, 0)")
    conn.commit()
    conn.close()

init_db()

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

def generate_account_number():
    conn = get_db()
    existing = {r['account'] for r in conn.execute("SELECT account FROM users").fetchall()}
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
        conn.execute("INSERT INTO users (user_id, account) VALUES (?, ?)", (str(user_id), account))
        conn.commit()
        user = conn.execute("SELECT * FROM users WHERE user_id = ?", (str(user_id),)).fetchone()
    conn.close()
    return dict(user)

def ensure_user(user_id, username=None, first_name=None):
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE user_id = ?", (str(user_id),)).fetchone()
    if not user:
        account = generate_account_number()
        conn.execute("INSERT INTO users (user_id, username, first_name, account) VALUES (?, ?, ?, ?)",
                     (str(user_id), username or '', first_name or '', account))
        conn.commit()
        logger.info(f"Авто-регистрация: {user_id} ({first_name})")
    elif username or first_name:
        updates = {}
        if username: updates['username'] = username
        if first_name: updates['first_name'] = first_name
        if updates:
            sets = ', '.join(f"{k} = ?" for k in updates)
            conn.execute(f"UPDATE users SET {sets} WHERE user_id = ?", list(updates.values()) + [str(user_id)])
            conn.commit()
    conn.close()

def update_user(user_id, **kwargs):
    if not kwargs: return
    conn = get_db()
    sets = ', '.join(f"{k} = ?" for k in kwargs)
    conn.execute(f"UPDATE users SET {sets} WHERE user_id = ?", list(kwargs.values()) + [str(user_id)])
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
    return {r['user_id']: r['messages'] for r in rows}

def increment_weekly_message(user_id):
    conn = get_db()
    conn.execute(
        "INSERT INTO weekly_stats (user_id, messages) VALUES (?, 1) ON CONFLICT(user_id) DO UPDATE SET messages = messages + 1",
        (str(user_id),)
    )
    conn.commit()
    conn.close()

# ==================== БЕЗОПАСНАЯ ОТПРАВКА ====================

async def safe_reply(update: Update, text, reply_markup=None, retries=3):
    for attempt in range(retries):
        try:
            if reply_markup:
                return await update.message.reply_text(text, reply_markup=reply_markup)
            else:
                return await update.message.reply_text(text)
        except (NetworkError, TimedOut) as e:
            logger.warning(f"Повтор {attempt+1}/{retries}: {e}")
            await asyncio.sleep(2)
    logger.error("Не удалось отправить")
    return None

async def safe_edit(query, text, reply_markup=None, retries=3):
    for attempt in range(retries):
        try:
            if reply_markup:
                return await query.edit_message_text(text, reply_markup=reply_markup)
            else:
                return await query.edit_message_text(text)
        except (NetworkError, TimedOut) as e:
            logger.warning(f"Повтор {attempt+1}/{retries}: {e}")
            await asyncio.sleep(2)
    return None

async def safe_answer(query, text, show_alert=False, retries=3):
    for attempt in range(retries):
        try:
            return await query.answer(text, show_alert=show_alert)
        except (NetworkError, TimedOut) as e:
            logger.warning(f"Повтор {attempt+1}/{retries}: {e}")
            await asyncio.sleep(1)
    return None

# ==================== КОМАНДЫ ====================

async def hello(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user(update.effective_user.id, update.effective_user.username, update.effective_user.first_name)
    user = get_user(update.effective_user.id)
    await safe_reply(update,
        f"👋 {update.effective_user.first_name}, твой профиль:\n\n"
        f"📊 Счёт: {user['account']}\n"
        f"💰 Баланс: {user['rys']} RYS — основная валюта чата\n"
        f"⭐ Репутация: {user['rep']} очков\n"
        f"✨ EXP: {user['exp']} — опыт для повышения ранга\n"
        f"🎖 Ранг: {get_rank(user['exp'])}\n"
        f"🏦 Банк: {get_bank_total()} RYS — общий котёл комиссий\n\n"
        f"/s_help — все команды\n/stats — подробная статистика"
    )

async def s_help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_reply(update,
        "📋 КОМАНДЫ БОТА\n\n"
        "💰 Экономика:\n"
        "/send GESH-XXXX [сумма] — перевод RYS (1% в банк)\n"
        "/balance — баланс и счёт\n\n"
        "🎲 Кейс-дуэль (ответом на сообщение):\n"
        "/case [ставка] — 10 кейсов, 1 приз x2\n\n"
        "⭐ Репутация (ответом на сообщение):\n"
        "+rep / -rep — повысить/понизить\n\n"
        "📊 Статистика:\n"
        "/stats — подробная информация\n"
        "/top — недельный топ\n\n"
        "🏆 Каждый понедельник:\n"
        "Топ-10 → EXP\n"
        "Топ-3 → доля банка (50/30/20%)\n\n"
        "🎖 Ранги: Пиздюк → Пиздун → Кайфун → Магистр → Легенда\n\n"
        "/hello — профиль"
    )

async def balance_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    await safe_reply(update,
        f"💰 Баланс: {user['rys']} RYS\n"
        f"📊 Счёт: {user['account']}\n"
        f"🏦 Банк: {get_bank_total()} RYS"
    )

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    weekly = get_weekly_messages()
    next_rank = ""
    for t, r in RANKS[1:]:
        if user['exp'] < t:
            next_rank = f" (нужно ещё {t - user['exp']} XP)"
            break
    
    await safe_reply(update,
        f"📊 СТАТИСТИКА ИГРОКА\n\n"
        f"👤 Имя: {user['first_name']}\n"
        f"🆔 ID: {user['user_id']}\n"
        f"📊 Счёт: {user['account']} — номер для переводов\n\n"
        f"💰 RYS: {user['rys']} — основная валюта\n"
        f"   • Тратится: переводы, кейсы, репутация\n"
        f"   • Заработок: переводы, выигрыши, топ-3 недели\n\n"
        f"⭐ Репутация: {user['rep']} очков\n"
        f"   • +rep стоит 1 RYS, -rep стоит 10 RYS\n"
        f"   • Деньги уходят в общий банк\n\n"
        f"✨ EXP: {user['exp']} — опыт\n"
        f"   • Начисляется только за недельный топ\n"
        f"   • Нужен для повышения ранга\n\n"
        f"🎖 Ранг: {get_rank(user['exp'])}{next_rank}\n\n"
        f"💬 За неделю: {weekly.get(str(update.effective_user.id), 0)} сообщений\n"
        f"📝 Всего: {user['total_messages']} сообщений"
    )

async def top_weekly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    weekly = get_weekly_messages()
    if not weekly:
        await safe_reply(update, "Нет данных за неделю"); return
    sorted_users = sorted(weekly.items(), key=lambda x: x[1], reverse=True)[:10]
    medals = ["🥇", "🥈", "🥉"] + [f"{i}." for i in range(4, 11)]
    text = "📊 НЕДЕЛЬНЫЙ ТОП\n\n"
    for i, (uid, count) in enumerate(sorted_users):
        user = get_user(uid)
        text += f"{medals[i]} {user['first_name']}: {count} сообщ.\n"
    text += f"\n🏦 Банк: {get_bank_total()} RYS"
    await safe_reply(update, text)

# ==================== ПЕРЕВОДЫ ====================

async def send_rys(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sender = get_user(update.effective_user.id)
    if not context.args or len(context.args) != 2:
        await safe_reply(update, "❌ /send GESH-XXXX [сумма]"); return
    account = context.args[0]
    try:
        amount = int(context.args[1])
        if amount <= 0: raise ValueError
    except ValueError:
        await safe_reply(update, "❌ Положительное число"); return
    if sender['rys'] < amount:
        await safe_reply(update, "❌ Недостаточно RYS"); return
    target = find_user_by_account(account)
    if not target:
        await safe_reply(update, "❌ Счёт не найден"); return
    if target['user_id'] == str(update.effective_user.id):
        await safe_reply(update, "❌ Нельзя себе"); return
    commission = max(int(amount * SEND_COMMISSION), 1)
    received = amount - commission
    update_user(update.effective_user.id, rys=sender['rys'] - amount)
    update_user(target['user_id'], rys=target['rys'] + received)
    add_to_bank(commission, f"Перевод {sender['account']} -> {account}")
    await safe_reply(update,
        f"✅ Перевод выполнен\n"
        f"📤 {amount} RYS\n"
        f"💸 Комиссия: {commission} RYS → банк\n"
        f"📥 Получатель: {target['first_name']} получил {received} RYS"
    )

# ==================== РЕПУТАЦИЯ ====================

async def rep_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.reply_to_message: return
    text = update.message.text.strip()
    if not (text.startswith('+rep') or text.startswith('-rep')): return
    sender = get_user(update.effective_user.id)
    target = get_user(update.message.reply_to_message.from_user.id)
    if sender['user_id'] == target['user_id']:
        await safe_reply(update, "❌ Нельзя себе"); return
    parts = text.split()
    value = int(parts[1]) if len(parts) > 1 and parts[1].lstrip('-').isdigit() else 1
    if value <= 0: await safe_reply(update, "❌ Положительное число"); return
    if text.startswith('+rep'):
        cost = value
        if sender['rys'] < cost: await safe_reply(update, f"❌ Нужно {cost} RYS"); return
        update_user(sender['user_id'], rys=sender['rys'] - cost)
        update_user(target['user_id'], rep=target['rep'] + value)
        add_to_bank(cost, f"+rep {sender['first_name']} -> {target['first_name']}")
        await safe_reply(update, f"✅ +{value} репутации → {target['first_name']} (списано {cost} RYS в банк)")
    else:
        cost = value * REP_MINUS_COST
        if sender['rys'] < cost: await safe_reply(update, f"❌ Нужно {cost} RYS"); return
        update_user(sender['user_id'], rys=sender['rys'] - cost)
        update_user(target['user_id'], rep=target['rep'] - value)
        add_to_bank(cost, f"-rep {sender['first_name']} -> {target['first_name']}")
        await safe_reply(update, f"👎 -{value} репутации → {target['first_name']} (списано {cost} RYS в банк)")

# ==================== КЕЙС-ДУЭЛЬ ====================

async def case_game(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        await safe_reply(update, "❌ Ответь на сообщение игрока: /case [ставка]"); return
    challenger = get_user(update.effective_user.id)
    opponent_user = update.message.reply_to_message.from_user
    if str(update.effective_user.id) == str(opponent_user.id):
        await safe_reply(update, "❌ Нельзя с собой"); return
    chat_id = str(update.effective_chat.id)
    if not context.args:
        await safe_reply(update, "❌ /case [ставка]"); return
    try:
        bet = int(context.args[0])
        if bet <= 0: raise ValueError
    except ValueError:
        await safe_reply(update, "❌ Положительное число"); return
    if challenger['rys'] < bet:
        await safe_reply(update, "❌ Недостаточно RYS"); return
    
    case_id = f"case_{chat_id}_{datetime.now().timestamp()}"
    win_index = random.randint(0, 9)
    
    conn = get_db()
    conn.execute(
        "INSERT INTO cases (case_id, chat_id, bet, prize, win_index, opened, creator_id, creator_name, opponent_id, opponent_name, status) VALUES (?,?,?,?,?,?,?,?,?,?,'waiting')",
        (case_id, chat_id, bet, bet*2, win_index, json.dumps([False]*10),
         str(update.effective_user.id), challenger['first_name'],
         str(opponent_user.id), opponent_user.first_name)
    )
    conn.commit()
    conn.close()
    
    update_user(update.effective_user.id, rys=challenger['rys'] - bet)
    
    kb = [
        [InlineKeyboardButton("✅ Принять", callback_data=f"case_accept_{case_id}"),
         InlineKeyboardButton("❌ Отклонить", callback_data=f"case_decline_{case_id}")],
        [InlineKeyboardButton("ℹ️ Правила", callback_data=f"case_info_{case_id}")]
    ]
    
    msg = await safe_reply(update,
        f"🎲 КЕЙС-ДУЭЛЬ\n\n"
        f"👤 {challenger['first_name']} вызывает {opponent_user.first_name}\n"
        f"💵 Ставка: {bet} RYS\n"
        f"🏆 Приз: {bet*2} RYS\n\n"
        f"⏳ Ожидание {opponent_user.first_name}...",
        reply_markup=InlineKeyboardMarkup(kb)
    )
    
    if msg:
        conn = get_db()
        conn.execute("UPDATE cases SET message_id = ? WHERE case_id = ?", (msg.message_id, case_id))
        conn.commit()
        conn.close()

async def case_accept_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    case_id = q.data.split('_')[2]
    user_id = str(q.from_user.id)
    
    conn = get_db()
    case = conn.execute("SELECT * FROM cases WHERE case_id = ?", (case_id,)).fetchone()
    
    if not case:
        await safe_answer(q, "❌ Кейс не найден", show_alert=True)
        conn.close()
        return
    
    if case['status'] != 'waiting':
        await safe_answer(q, "❌ Кейс уже не активен", show_alert=True)
        conn.close()
        return
    
    if user_id == case['creator_id']:
        await safe_answer(q, "❌ Ты создатель — жди противника", show_alert=True)
        conn.close()
        return
    
    if user_id != case['opponent_id']:
        await safe_answer(q, "❌ Этот вызов не тебе", show_alert=True)
        conn.close()
        return
    
    opponent = get_user(user_id)
    if opponent['rys'] < case['bet']:
        await safe_answer(q, f"❌ Нужно {case['bet']} RYS", show_alert=True)
        conn.close()
        return
    
    await safe_answer(q, "⚔️ Поехали!")
    
    update_user(user_id, rys=opponent['rys'] - case['bet'])
    turn = random.choice(['creator', 'opponent'])
    
    conn.execute("UPDATE cases SET status='active', current_turn=? WHERE case_id=?", (turn, case_id))
    conn.commit()
    conn.close()
    
    first_player = get_user(case['creator_id'] if turn == 'creator' else user_id)
    kb = build_case_buttons(case, case_id)
    
    await safe_edit(q,
        f"🎲 КЕЙС-ДУЭЛЬ\n\n"
        f"👤 {case['creator_name']} VS {opponent['first_name']}\n"
        f"💵 Ставка: {case['bet']} RYS\n"
        f"🏆 Приз: {case['prize']} RYS\n\n"
        f"👤 Ход: {first_player['first_name']}",
        reply_markup=InlineKeyboardMarkup(kb)
    )

async def case_decline_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    case_id = q.data.split('_')[2]
    user_id = str(q.from_user.id)
    
    conn = get_db()
    case = conn.execute("SELECT * FROM cases WHERE case_id = ?", (case_id,)).fetchone()
    
    if not case or case['status'] != 'waiting':
        await safe_answer(q, "❌ Нельзя отменить", show_alert=True)
        conn.close()
        return
    
    if user_id != case['creator_id']:
        await safe_answer(q, "❌ Только создатель может отменить", show_alert=True)
        conn.close()
        return
    
    creator = get_user(case['creator_id'])
    update_user(case['creator_id'], rys=creator['rys'] + case['bet'])
    
    conn.execute("UPDATE cases SET status='declined' WHERE case_id=?", (case_id,))
    conn.commit()
    conn.close()
    
    await safe_edit(q,
        f"❌ Кейс отменён\n"
        f"👤 {case['creator_name']} вернул ставку\n"
        f"💰 {case['bet']} RYS возвращены"
    )

def build_case_buttons(case, case_id):
    opened = json.loads(case['opened']) if isinstance(case['opened'], str) else case['opened']
    rem = 10 - sum(opened)
    kb = []
    for i in range(0, 10, 5):
        row = []
        for j in range(i, i+5):
            if opened[j]:
                row.append(InlineKeyboardButton(f"❌ {j+1}", callback_data="noop"))
            else:
                emoji = "🎯" if rem == 1 else "🎁"
                row.append(InlineKeyboardButton(f"{emoji} {j+1}", callback_data=f"case_open_{case_id}_{j}"))
        kb.append(row)
    kb.append([InlineKeyboardButton("ℹ️", callback_data=f"case_info_{case_id}")])
    return kb

async def handle_case_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split('_')
    case_id = parts[2]
    box = int(parts[3])
    user_id = str(q.from_user.id)
    
    conn = get_db()
    case = conn.execute("SELECT * FROM cases WHERE case_id = ?", (case_id,)).fetchone()
    
    if not case or case['status'] != 'active':
        await safe_answer(q, "❌ Кейс завершён", show_alert=True)
        conn.close()
        return
    
    if user_id not in [case['creator_id'], case['opponent_id']]:
        await safe_answer(q, "❌ Ты не в игре", show_alert=True)
        conn.close()
        return
    
    current_player = case['creator_id'] if case['current_turn'] == 'creator' else case['opponent_id']
    if user_id != current_player:
        player_name = get_user(current_player)['first_name']
        await safe_answer(q, f"⏳ Сейчас ход {player_name}", show_alert=True)
        conn.close()
        return
    
    opened = json.loads(case['opened']) if isinstance(case['opened'], str) else case['opened']
    if opened[box]:
        await safe_answer(q, "❌ Уже открыт", show_alert=True)
        conn.close()
        return
    
    remaining_before = 10 - sum(opened)
    opened[box] = True
    user = get_user(user_id)
    
    is_last = (remaining_before == 1)
    is_win = (box == case['win_index'])
    
    if is_last or is_win:
        update_user(user_id, rys=user['rys'] + case['prize'])
        
        conn.execute(
            "UPDATE cases SET opened=?, status='finished', winner_id=?, winner_name=?, finished_at=CURRENT_TIMESTAMP WHERE case_id=?",
            (json.dumps(opened), user_id, q.from_user.first_name, case_id)
        )
        conn.commit()
        conn.close()
        
        await safe_answer(q, f"🎉 +{case['prize']} RYS!", show_alert=True)
        
        kb = build_final_buttons(case, opened, box)
        
        if is_last and not is_win:
            desc = "последний кейс (100% победа)"
        elif is_win and is_last:
            desc = "победный кейс (последний)"
        else:
            desc = f"победный кейс №{box+1}"
        
        await safe_edit(q,
            f"🎲 КЕЙС-ДУЭЛЬ ЗАВЕРШЕНА\n\n"
            f"👤 {case['creator_name']} VS {case['opponent_name']}\n"
            f"💵 Ставка: {case['bet']} RYS\n\n"
            f"🏆 Победитель: {q.from_user.first_name}\n"
            f"🎯 {desc}\n"
            f"💰 Выигрыш: {case['prize']} RYS",
            reply_markup=InlineKeyboardMarkup(kb)
        )
    else:
        new_turn = 'opponent' if case['current_turn'] == 'creator' else 'creator'
        
        conn.execute(
            "UPDATE cases SET opened=?, current_turn=? WHERE case_id=?",
            (json.dumps(opened), new_turn, case_id)
        )
        conn.commit()
        conn.close()
        
        next_player = get_user(case['creator_id'] if new_turn == 'creator' else case['opponent_id'])
        await safe_answer(q, f"📦 Пусто! Ход {next_player['first_name']}", show_alert=True)
        
        updated_case = dict(case)
        updated_case['opened'] = opened
        updated_case['current_turn'] = new_turn
        
        kb = build_case_buttons(updated_case, case_id)
        remaining = 10 - sum(opened)
        hint = "\n⚠️ Последний кейс — 100% победа!" if remaining == 1 else ""
        
        await safe_edit(q,
            f"🎲 КЕЙС-ДУЭЛЬ\n\n"
            f"👤 {case['creator_name']} VS {case['opponent_name']}\n"
            f"💵 Ставка: {case['bet']} RYS\n"
            f"🏆 Приз: {case['prize']} RYS\n"
            f"🎮 Открыто: {sum(opened)}/10 (осталось {remaining})\n\n"
            f"👤 Ход: {next_player['first_name']}{hint}",
            reply_markup=InlineKeyboardMarkup(kb)
        )

def build_final_buttons(case, opened, win_box):
    kb = []
    for i in range(0, 10, 5):
        row = []
        for j in range(i, i+5):
            if j == case['win_index']:
                emoji = "🎉" if j == win_box else "💎"
            elif opened[j]:
                emoji = "❌"
            else:
                emoji = "🎁"
            row.append(InlineKeyboardButton(f"{emoji} {j+1}", callback_data="noop"))
        kb.append(row)
    return kb

async def case_info_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_answer(update.callback_query,
        "🎲 ПРАВИЛА КЕЙС-ДУЭЛИ:\n\n"
        "• Два игрока ставят одинаковую сумму\n"
        "• Призовой фонд = ставка × 2\n"
        "• 1 из 10 кейсов содержит приз\n"
        "• Ход переходит по очереди\n"
        "• Последний кейс = 100% победа!\n"
        "• Победитель забирает всё",
        show_alert=True
    )

# ==================== СЧЁТЧИК СООБЩЕНИЙ ====================

async def count_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text: return
    ensure_user(update.effective_user.id, update.effective_user.username, update.effective_user.first_name)
    if len(update.message.text) >= 2:
        user = get_user(update.effective_user.id)
        update_user(update.effective_user.id, total_messages=user['total_messages'] + 1)
        increment_weekly_message(update.effective_user.id)

# ==================== АДМИН-ПАНЕЛЬ ====================

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await safe_reply(update, "❌ Нет доступа"); return
    if update.message.reply_to_message:
        tid = update.message.reply_to_message.from_user.id
        kb = [
            [InlineKeyboardButton("📊 Инфо", callback_data=f"admin_info_{tid}")],
            [InlineKeyboardButton("💰 +RYS", callback_data=f"admin_add_rys_{tid}"),
             InlineKeyboardButton("💰 -RYS", callback_data=f"admin_sub_rys_{tid}")],
            [InlineKeyboardButton("⭐ +REP", callback_data=f"admin_add_rep_{tid}"),
             InlineKeyboardButton("⭐ -REP", callback_data=f"admin_sub_rep_{tid}")],
            [InlineKeyboardButton("✨ +EXP", callback_data=f"admin_add_exp_{tid}"),
             InlineKeyboardButton("✨ -EXP", callback_data=f"admin_sub_exp_{tid}")],
            [InlineKeyboardButton("🗑 Удалить", callback_data=f"admin_delete_{tid}"),
             InlineKeyboardButton("➕ Создать", callback_data=f"admin_add_user_{tid}")]
        ]
        await safe_reply(update, f"🛡 {update.message.reply_to_message.from_user.first_name}", reply_markup=InlineKeyboardMarkup(kb))
        return
    kb = [
        [InlineKeyboardButton("📋 Реестр", callback_data="admin_list")],
        [InlineKeyboardButton("📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton("🏦 Банк", callback_data="admin_bank_info")],
        [InlineKeyboardButton("🗑 Сбросить игры", callback_data="admin_reset_games")]
    ]
    await safe_reply(update, "🛡 АДМИН-ПАНЕЛЬ", reply_markup=InlineKeyboardMarkup(kb))

async def admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.from_user.id != ADMIN_ID: await safe_answer(q, "❌"); return
    await safe_answer(q, "")
    p = q.data.split('_')
    a = p[1]
    if a == 'info':
        u = get_user(p[2]); w = get_weekly_messages()
        await safe_edit(q, f"📊 {u['first_name']}\n💰{u['rys']} ⭐{u['rep']} ✨{u['exp']}\n🎖{get_rank(u['exp'])}\n💬{w.get(p[2],0)}")
    elif a in ['add','sub']:
        context.user_data['admin_action'] = {'tid': p[3], 'cur': p[2], 'op': a}
        await safe_edit(q, f"✏️ {'+' if a=='add' else '-'}{p[2].upper()}")
    elif a == 'delete':
        conn = get_db(); u = conn.execute("SELECT first_name FROM users WHERE user_id=?", (p[2],)).fetchone()
        if u: conn.execute("DELETE FROM users WHERE user_id=?", (p[2],)); conn.commit(); await safe_edit(q, f"✅ {u['first_name']} удален")
        else: await safe_edit(q, "❌ Не найден")
        conn.close()
    elif a == 'add': u = get_user(p[2]); await safe_edit(q, f"✅ {u['first_name']} в БД")
    elif a == 'list':
        conn = get_db(); ul = conn.execute("SELECT * FROM users ORDER BY exp DESC").fetchall(); conn.close()
        if not ul: await safe_edit(q, "📋 Пусто"); return
        page = context.user_data.get('ap', 0); tp = (len(ul)-1)//10+1
        text = f"📋 ({page+1}/{tp})\n"
        for u in ul[page*10:(page+1)*10]: text += f"👤 {u['first_name']} | 💰{u['rys']} | ✨{u['exp']} | 🎖{get_rank(u['exp'])}\n"
        kb = []; nav = []
        if page>0: nav.append(InlineKeyboardButton("⬅️", callback_data=f"admin_page_{page-1}"))
        if page<tp-1: nav.append(InlineKeyboardButton("➡️", callback_data=f"admin_page_{page+1}"))
        if nav: kb.append(nav)
        kb.append([InlineKeyboardButton("🔙", callback_data="admin_back")])
        await safe_edit(q, text, reply_markup=InlineKeyboardMarkup(kb))
    elif a == 'page': context.user_data['ap'] = int(p[2]); await admin_callback(update, context)
    elif a == 'back':
        kb = [
            [InlineKeyboardButton("📋 Реестр", callback_data="admin_list")],
            [InlineKeyboardButton("📢 Рассылка", callback_data="admin_broadcast")],
            [InlineKeyboardButton("🏦 Банк", callback_data="admin_bank_info")],
            [InlineKeyboardButton("🗑 Сбросить игры", callback_data="admin_reset_games")]
        ]
        await safe_edit(q, "🛡 АДМИН-ПАНЕЛЬ", reply_markup=InlineKeyboardMarkup(kb))
    elif a == 'bank':
        conn = get_db(); total = conn.execute("SELECT total_commission FROM bank WHERE id=1").fetchone()['total_commission']
        recent = conn.execute("SELECT * FROM bank_history ORDER BY id DESC LIMIT 5").fetchall(); conn.close()
        text = f"🏦 {total} RYS\n"
        for op in recent: text += f"• {op['reason']}: +{op['amount']}\n"
        await safe_edit(q, text)
    elif a == 'reset':
        conn = get_db()
        c = conn.execute("UPDATE cases SET status='finished' WHERE status IN ('waiting','active')").rowcount
        conn.commit(); conn.close()
        await safe_edit(q, f"✅ Сброшено кейсов: {c}")
    elif a == 'broadcast': context.user_data['broadcast'] = True; await safe_edit(q, "📢 Напиши сообщение:")

async def handle_admin_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if update.message.text == '/cancel':
        context.user_data.pop('broadcast', None); context.user_data.pop('admin_action', None)
        await safe_reply(update, "❌ Отменено"); return
    if 'admin_action' in context.user_data:
        ac = context.user_data['admin_action']
        try: amount = int(update.message.text)
        except ValueError: await safe_reply(update, "❌ Число"); return
        u = get_user(ac['tid']); f = ac['cur']
        update_user(ac['tid'], **{f: u[f] + (amount if ac['op']=='add' else -amount)})
        u2 = get_user(ac['tid'])
        await safe_reply(update, f"✅ {u2['first_name']}\n{f.upper()}: {'+'if ac['op']=='add' else ''}{amount}\n💰{u2['rys']} ⭐{u2['rep']} ✨{u2['exp']}")
        del context.user_data['admin_action']; return
    if context.user_data.get('broadcast'):
        sent = 0; conn = get_db(); ul = conn.execute("SELECT user_id FROM users").fetchall(); conn.close()
        for u in ul:
            try: await context.bot.send_message(int(u['user_id']), f"📢 Рассылка\n\n{update.message.text}"); sent += 1; await asyncio.sleep(0.5)
            except: pass
        await safe_reply(update, f"✅ {sent}/{len(ul)}")
        del context.user_data['broadcast']

# ==================== ЕЖЕНЕДЕЛЬНЫЙ СБРОС ====================

async def weekly_reset():
    logger.info("=== СБРОС ===")
    conn = get_db(); w = conn.execute("SELECT user_id, messages FROM weekly_stats WHERE messages > 0").fetchall()
    if not w: logger.info("Нет сообщений"); conn.close(); return
    s = sorted(w, key=lambda x: x['messages'], reverse=True)
    er = {1:500,2:400,3:300,4:200,5:150,6:100,7:75,8:50,9:30,10:20}
    bd = {1:0.5,2:0.3,3:0.2}
    tb = conn.execute("SELECT total_commission FROM bank WHERE id=1").fetchone()['total_commission']; dist = 0
    for i, (uid, cnt) in enumerate([(r['user_id'], r['messages']) for r in s[:10]], 1):
        conn.execute("UPDATE users SET exp = exp + ? WHERE user_id = ?", (er.get(i,0), uid))
        br = int(tb * bd[i]) if i <= 3 and tb > 0 else 0
        if br: conn.execute("UPDATE users SET rys = rys + ? WHERE user_id = ?", (br, uid)); dist += br
        try: await context.bot.send_message(int(uid), f"🏆 Место {i}\n💬 {cnt}\n✨ +{er.get(i,0)} EXP" + (f"\n💰 +{br} RYS" if br else ""))
        except: pass
    conn.execute("UPDATE bank SET total_commission = 0 WHERE id = 1")
    if dist: conn.execute("INSERT INTO bank_history (amount, reason) VALUES (?, 'weekly')", (dist,))
    conn.execute("DELETE FROM weekly_stats")
    conn.execute("INSERT OR REPLACE INTO weekly_reset (id, last_reset) VALUES (1, CURRENT_TIMESTAMP)")
    conn.commit(); conn.close()
    logger.info(f"Сброс: {dist} RYS")

# ==================== ОБРАБОТЧИК ОШИБОК ====================

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    err = context.error
    if isinstance(err, (NetworkError, TimedOut)):
        logger.error(f"⏰ Таймаут — повтор")
        await asyncio.sleep(5)
        if update:
            try: await context.application.process_update(update)
            except Exception as e: logger.error(f"❌ Повтор не удался: {e}")
    elif isinstance(err, RetryAfter):
        await asyncio.sleep(err.retry_after)
        if update:
            try: await context.application.process_update(update)
            except: pass

# ==================== ЗАПУСК ====================

if __name__ == "__main__":
    if not TOKEN: logger.error("❌ Нет токена"); exit(1)
    
    app = Application.builder().token(TOKEN).connect_timeout(30).read_timeout(30).write_timeout(30).build()
    app.add_error_handler(error_handler)
    
    async def start_scheduler(app):
        scheduler = AsyncIOScheduler(timezone=pytz.UTC)
        scheduler.add_job(weekly_reset, 'cron', day_of_week='mon', hour=0, minute=0)
        scheduler.start()
    app.post_init = start_scheduler
    
    app.add_handler(CommandHandler("hello", hello))
    app.add_handler(CommandHandler("s_help", s_help_command))
    app.add_handler(CommandHandler("balance", balance_cmd))
    app.add_handler(CommandHandler("send", send_rys))
    app.add_handler(CommandHandler("case", case_game))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("top", top_weekly))
    app.add_handler(CommandHandler("admin", admin_panel))
    
    app.add_handler(CallbackQueryHandler(case_accept_callback, pattern="^case_accept_"))
    app.add_handler(CallbackQueryHandler(case_decline_callback, pattern="^case_decline_"))
    app.add_handler(CallbackQueryHandler(handle_case_open, pattern="^case_open_"))
    app.add_handler(CallbackQueryHandler(case_info_callback, pattern="^case_info_"))
    app.add_handler(CallbackQueryHandler(admin_callback, pattern="^admin_"))
    
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.REPLY, rep_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, count_message))
    app.add_handler(MessageHandler(filters.TEXT & filters.User(ADMIN_ID), handle_admin_text))
    
    logger.info("Бот запущен")
    app.run_polling(allowed_updates=Update.ALL_TYPES)
