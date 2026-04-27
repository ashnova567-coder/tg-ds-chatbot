import os
import sqlite3
import json
import random
import asyncio
import logging
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from telegram.constants import ParseMode
from telegram.error import TelegramError, NetworkError, TimedOut, RetryAfter
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pytz

# ==================== НАСТРОЙКИ ====================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

ADMIN_ID = 1055271488  # ← ЗАМЕНИ НА СВОЙ ID!
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
        if exp >= threshold:
            current = rank
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

# ==================== КОМАНДЫ ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user(update.effective_user.id, update.effective_user.username, update.effective_user.first_name)
    user = get_user(update.effective_user.id)
    await update.message.reply_text(
        f"👋 Привет, {update.effective_user.first_name}!\n\n"
        f"📊 Счет: {user['account']}\n💰 Баланс: {user['rys']} RYS\n"
        f"⭐ Репутация: {user['rep']}\n✨ EXP: {user['exp']}\n"
        f"🎖 Ранг: {get_rank(user['exp'])}\n🏦 Банк: {get_bank_total()} RYS\n\n"
        f"/s_help — список команд"
    )

async def s_help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📋 КОМАНДЫ БОТА\n━━━━━━━━━━━━━━━━\n\n"
        "💰 Финансы:\n• /send GESH-XXXX [сумма] — перевод (1% в банк)\n• /balance — баланс\n\n"
        "🎲 Кейс-дуэль (ответом на сообщение):\n• /case [ставка]\n\n"
        "⚔️ Дуэль (ответом на сообщение):\n• /duel [ставка]\n\n"
        "⭐ Репутация (ответом на сообщение):\n• +rep / -rep\n\n"
        "📊 Статистика:\n• /stats / /top\n\n"
        "🏆 Недельный топ: понедельник 00:00 UTC\n"
        "🎖 Ранги: Пиздюк → Пиздун → Кайфун → Магистр → Легенда"
    )

async def balance_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    await update.message.reply_text(f"💰 {user['rys']} RYS\n📊 {user['account']}\n🏦 Банк: {get_bank_total()} RYS")

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    weekly = get_weekly_messages()
    next_rank = ""
    for t, r in RANKS[1:]:
        if user['exp'] < t:
            next_rank = f"\n📈 До следующего: {t - user['exp']} XP"
            break
    await update.message.reply_text(
        f"📊 СТАТИСТИКА\n━━━━━━━━━━━━━━━━\n👤 {user['first_name']}\n📊 {user['account']}\n"
        f"💰 {user['rys']} RYS | ⭐ {user['rep']} rep\n✨ {user['exp']} XP | 🎖 {get_rank(user['exp'])}{next_rank}\n"
        f"⚔️ Побед: {user['duels_won']} | 💀 Поражений: {user['duels_lost']}\n"
        f"💬 За неделю: {weekly.get(str(update.effective_user.id), 0)}\n📝 Всего: {user['total_messages']}"
    )

async def top_weekly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    weekly = get_weekly_messages()
    if not weekly:
        await update.message.reply_text("📊 Нет данных за неделю"); return
    sorted_users = sorted(weekly.items(), key=lambda x: x[1], reverse=True)[:10]
    medals = ["🥇", "🥈", "🥉"] + [f"{i}." for i in range(4, 11)]
    text = "📊 НЕДЕЛЬНЫЙ ТОП\n━━━━━━━━━━━━━━━━\n"
    for i, (uid, count) in enumerate(sorted_users):
        user = get_user(uid)
        text += f"{medals[i]} {user['first_name']}: {count} сообщ.\n"
    text += f"\n🏦 Банк: {get_bank_total()} RYS"
    await update.message.reply_text(text)

# ==================== ПЕРЕВОДЫ ====================

async def send_rys(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sender = get_user(update.effective_user.id)
    if not context.args or len(context.args) != 2:
        await update.message.reply_text("❌ /send GESH-XXXX [сумма]"); return
    account = context.args[0]
    try:
        amount = int(context.args[1])
        if amount <= 0: raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Сумма — положительное число"); return
    if sender['rys'] < amount:
        await update.message.reply_text("❌ Недостаточно RYS"); return
    target = find_user_by_account(account)
    if not target:
        await update.message.reply_text("❌ Получатель не найден"); return
    if target['user_id'] == str(update.effective_user.id):
        await update.message.reply_text("❌ Нельзя себе"); return
    commission = max(int(amount * SEND_COMMISSION), 1)
    received = amount - commission
    update_user(update.effective_user.id, rys=sender['rys'] - amount)
    update_user(target['user_id'], rys=target['rys'] + received)
    add_to_bank(commission, f"Перевод {sender['account']} -> {account}")
    await update.message.reply_text(
        f"✅ Перевод\n📤 {amount} | 💸 {commission} | 📥 {received}\n👤 {target['first_name']}\n💰 Баланс: {sender['rys'] - amount}"
    )

# ==================== РЕПУТАЦИЯ ====================

async def rep_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.reply_to_message:
        return
    
    text = update.message.text.strip()
    if not (text.startswith('+rep') or text.startswith('-rep')):
        return
    
    sender = get_user(update.effective_user.id)
    target = get_user(update.message.reply_to_message.from_user.id)
    
    if sender['user_id'] == target['user_id']:
        await update.message.reply_text("❌ Нельзя себе")
        return
    
    parts = text.split()
    value = int(parts[1]) if len(parts) > 1 and parts[1].lstrip('-').isdigit() else 1
    if value <= 0:
        await update.message.reply_text("❌ Положительное число")
        return
    
    if text.startswith('+rep'):
        cost = value
        if sender['rys'] < cost:
            await update.message.reply_text(f"❌ Нужно {cost} RYS")
            return
        update_user(sender['user_id'], rys=sender['rys'] - cost)
        update_user(target['user_id'], rep=target['rep'] + value)
        add_to_bank(cost, f"+rep {sender['first_name']} -> {target['first_name']}")
        await update.message.reply_text(f"✅ +{value} rep -> {target['first_name']} 💸{cost} RYS")
    else:
        cost = value * REP_MINUS_COST
        if sender['rys'] < cost:
            await update.message.reply_text(f"❌ Нужно {cost} RYS")
            return
        update_user(sender['user_id'], rys=sender['rys'] - cost)
        update_user(target['user_id'], rep=target['rep'] - value)
        add_to_bank(cost, f"-rep {sender['first_name']} -> {target['first_name']}")
        await update.message.reply_text(f"👎 -{value} rep -> {target['first_name']} 💸{cost} RYS")

# ==================== КЕЙС-ДУЭЛЬ ====================

async def case_game(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        await update.message.reply_text("❌ Ответь на сообщение игрока: /case [ставка]")
        return
    challenger = get_user(update.effective_user.id)
    opponent_user = update.message.reply_to_message.from_user
    if str(update.effective_user.id) == str(opponent_user.id):
        await update.message.reply_text("❌ Нельзя с собой"); return
    chat_id = str(update.effective_chat.id)
    if not context.args:
        await update.message.reply_text("❌ /case [ставка]"); return
    try:
        bet = int(context.args[0])
        if bet <= 0: raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Положительное число"); return
    if challenger['rys'] < bet:
        await update.message.reply_text("❌ Недостаточно RYS"); return
    conn = get_db()
    if conn.execute("SELECT 1 FROM cases WHERE chat_id = ? AND status IN ('waiting','active')", (chat_id,)).fetchone():
        await update.message.reply_text("❌ Активный кейс уже есть"); conn.close(); return
    case_id = f"case_{chat_id}_{datetime.now().timestamp()}"
    conn.execute(
        "INSERT INTO cases (case_id, chat_id, bet, prize, win_index, opened, creator_id, creator_name, opponent_id, opponent_name, status) VALUES (?,?,?,?,?,?,?,?,?,?,'waiting')",
        (case_id, chat_id, bet, bet*2, random.randint(0,9), json.dumps([False]*10),
         str(update.effective_user.id), challenger['first_name'], str(opponent_user.id), opponent_user.first_name)
    )
    conn.commit(); conn.close()
    update_user(update.effective_user.id, rys=challenger['rys'] - bet)
    kb = [
        [InlineKeyboardButton("✅ Принять", callback_data=f"case_accept_{case_id}"),
         InlineKeyboardButton("❌ Отклонить", callback_data=f"case_decline_{case_id}")],
        [InlineKeyboardButton("ℹ️ Правила", callback_data=f"case_info_{case_id}")]
    ]
    msg = await update.message.reply_text(
        f"🎲 КЕЙС-ДУЭЛЬ\n👤 {challenger['first_name']} вызывает {opponent_user.first_name}\n💵 {bet} | 🏆 {bet*2}\n⏳ Ожидание...",
        reply_markup=InlineKeyboardMarkup(kb)
    )
    conn = get_db(); conn.execute("UPDATE cases SET message_id = ? WHERE case_id = ?", (msg.message_id, case_id)); conn.commit(); conn.close()

async def case_accept_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; case_id = q.data.split('_')[2]; user_id = str(q.from_user.id)
    conn = get_db(); case = conn.execute("SELECT * FROM cases WHERE case_id = ?", (case_id,)).fetchone()
    if not case or case['status'] != 'waiting':
        await q.answer("❌ Кейс не активен", show_alert=True); conn.close(); return
    case = dict(case)
    if user_id == case['creator_id']:
        await q.answer("❌ Жди противника или нажми Отклонить", show_alert=True); conn.close(); return
    if user_id != case['opponent_id']:
        await q.answer("❌ Этот вызов не тебе", show_alert=True); conn.close(); return
    opponent = get_user(user_id)
    if opponent['rys'] < case['bet']:
        await q.answer(f"❌ Нужно {case['bet']} RYS", show_alert=True); conn.close(); return
    await q.answer("⚔️ Поехали!")
    update_user(user_id, rys=opponent['rys'] - case['bet'])
    turn = random.choice(['creator', 'opponent'])
    conn.execute("UPDATE cases SET status='active', current_turn=? WHERE case_id=?", (turn, case_id))
    conn.commit(); conn.close()
    fp = get_user(case['creator_id'] if turn == 'creator' else user_id)
    kb = build_case_buttons(case, case_id)
    await q.edit_message_text(
        f"🎲 КЕЙС-ДУЭЛЬ\n👤 {case['creator_name']} vs {opponent['first_name']}\n💵 {case['bet']} | 🏆 {case['prize']}\n👤 Ход: {fp['first_name']}",
        reply_markup=InlineKeyboardMarkup(kb)
    )

async def case_decline_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; case_id = q.data.split('_')[2]; user_id = str(q.from_user.id)
    conn = get_db(); case = conn.execute("SELECT * FROM cases WHERE case_id = ?", (case_id,)).fetchone()
    if not case or case['status'] != 'waiting':
        await q.answer("❌ Нельзя отменить", show_alert=True); conn.close(); return
    case = dict(case)
    if user_id != case['creator_id']:
        await q.answer("❌ Только создатель", show_alert=True); conn.close(); return
    update_user(case['creator_id'], rys=get_user(case['creator_id'])['rys'] + case['bet'])
    conn.execute("UPDATE cases SET status='declined' WHERE case_id=?", (case_id,)); conn.commit(); conn.close()
    await q.edit_message_text(f"❌ {case['creator_name']} отменил игру\n💰 {case['bet']} RYS возвращены")

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
                row.append(InlineKeyboardButton(f"{'🎯' if rem==1 else '🎁'} {j+1}", callback_data=f"case_open_{case_id}_{j}"))
        kb.append(row)
    kb.append([InlineKeyboardButton("ℹ️", callback_data=f"case_info_{case_id}")])
    return kb

async def handle_case_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; p = q.data.split('_'); case_id = p[2]; box = int(p[3]); user_id = str(q.from_user.id)
    conn = get_db(); case = conn.execute("SELECT * FROM cases WHERE case_id = ?", (case_id,)).fetchone()
    if not case or case['status'] != 'active':
        await q.answer("❌ Завершена", show_alert=True); conn.close(); return
    case = dict(case)
    if user_id not in [case['creator_id'], case['opponent_id']]:
        await q.answer("❌ Не участвуешь", show_alert=True); conn.close(); return
    cur = case['creator_id'] if case['current_turn'] == 'creator' else case['opponent_id']
    if user_id != cur:
        await q.answer(f"⏳ Ход {get_user(cur)['first_name']}", show_alert=True); conn.close(); return
    opened = json.loads(case['opened']) if isinstance(case['opened'], str) else case['opened']
    if opened[box]:
        await q.answer("❌ Открыт", show_alert=True); conn.close(); return
    rem_before = 10 - sum(opened); opened[box] = True; user = get_user(user_id)
    is_last = (rem_before == 1); is_win = (box == case['win_index'])
    if is_last or is_win:
        update_user(user_id, rys=user['rys'] + case['prize'])
        conn.execute(
            "UPDATE cases SET opened=?, status='finished', winner_id=?, winner_name=?, finished_at=CURRENT_TIMESTAMP WHERE case_id=?",
            (json.dumps(opened), user_id, q.from_user.first_name, case_id)
        ); conn.commit(); conn.close()
        await q.answer(f"🎉 +{case['prize']} RYS!", show_alert=True)
        kb = build_final_buttons(case, opened, box)
        desc = "последний кейс (100%)" if is_last and not is_win else ("победный (последний)" if is_win and is_last else f"кейс №{box+1}")
        await q.edit_message_text(
            f"🎲 ЗАВЕРШЕНА\n👤 {case['creator_name']} vs {case['opponent_name']}\n💵 {case['bet']} RYS\n"
            f"🏆 {q.from_user.first_name}\n🎯 {desc}\n💰 {case['prize']} RYS",
            reply_markup=InlineKeyboardMarkup(kb)
        )
    else:
        new_turn = 'opponent' if case['current_turn'] == 'creator' else 'creator'
        conn.execute("UPDATE cases SET opened=?, current_turn=? WHERE case_id=?", (json.dumps(opened), new_turn, case_id))
        conn.commit(); conn.close()
        cur_player = get_user(case['creator_id'] if new_turn == 'creator' else case['opponent_id'])
        await q.answer(f"📦 Ход {cur_player['first_name']}", show_alert=True)
        kb = build_case_buttons({**case, 'opened': opened}, case_id)
        rem = 10 - sum(opened)
        await q.edit_message_text(
            f"🎲 КЕЙС-ДУЭЛЬ\n👤 {case['creator_name']} vs {case['opponent_name']}\n💵 {case['bet']} | 🏆 {case['prize']}\n"
            f"🎮 {sum(opened)}/10 (ост. {rem})\n👤 Ход: {cur_player['first_name']}{' ⚠️ 100% победа!' if rem==1 else ''}",
            reply_markup=InlineKeyboardMarkup(kb)
        )

def build_final_buttons(case, opened, win_box):
    kb = []
    for i in range(0, 10, 5):
        row = []
        for j in range(i, i+5):
            if j == case['win_index']: e = "🎉" if j == win_box else "💎"
            elif opened[j]: e = "❌"
            else: e = "🎁"
            row.append(InlineKeyboardButton(f"{e} {j+1}", callback_data="noop"))
        kb.append(row)
    return kb

async def case_info_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer("🎲 2 игрока, 10 кейсов, 1 приз x2\nПоследний кейс = 100% победа!", show_alert=True)

# ==================== ДУЭЛЬ ====================

async def duel_create(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        await update.message.reply_text("❌ Ответь на сообщение игрока: /duel [ставка]")
        return
    challenger = get_user(update.effective_user.id)
    opponent_user = update.message.reply_to_message.from_user
    if str(update.effective_user.id) == str(opponent_user.id):
        await update.message.reply_text("❌ Нельзя с собой"); return
    chat_id = str(update.effective_chat.id)
    if not context.args:
        await update.message.reply_text("❌ /duel [ставка]"); return
    try:
        bet = int(context.args[0])
        if bet <= 0: raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Положительное число"); return
    if challenger['rys'] < bet:
        await update.message.reply_text("❌ Недостаточно RYS"); return
    conn = get_db()
    if conn.execute("SELECT 1 FROM duels WHERE chat_id = ? AND status = 'waiting'", (chat_id,)).fetchone():
        await update.message.reply_text("❌ Активная дуэль есть"); conn.close(); return
    duel_id = f"duel_{chat_id}_{datetime.now().timestamp()}"
    conn.execute(
        "INSERT INTO duels (duel_id, chat_id, challenger_id, challenger_name, opponent_id, opponent_name, bet, prize) VALUES (?,?,?,?,?,?,?,?)",
        (duel_id, chat_id, str(update.effective_user.id), challenger['first_name'], str(opponent_user.id), opponent_user.first_name, bet, bet*2)
    ); conn.commit(); conn.close()
    update_user(update.effective_user.id, rys=challenger['rys'] - bet)
    kb = [
        [InlineKeyboardButton("⚔️ Принять", callback_data=f"duel_accept_{duel_id}"),
         InlineKeyboardButton("❌ Отклонить", callback_data=f"duel_decline_{duel_id}")],
        [InlineKeyboardButton("ℹ️", callback_data=f"duel_info_{duel_id}")]
    ]
    msg = await update.message.reply_text(
        f"⚔️ ДУЭЛЬ\n👤 {challenger['first_name']} вызывает {opponent_user.first_name}\n💵 {bet} | 🏆 {bet*2}\n⏳ Ожидание...",
        reply_markup=InlineKeyboardMarkup(kb)
    )
    conn = get_db(); conn.execute("UPDATE duels SET message_id = ? WHERE duel_id = ?", (msg.message_id, duel_id)); conn.commit(); conn.close()

async def duel_accept_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; duel_id = q.data.split('_')[2]; user_id = str(q.from_user.id)
    conn = get_db(); duel = conn.execute("SELECT * FROM duels WHERE duel_id = ?", (duel_id,)).fetchone()
    if not duel or duel['status'] != 'waiting':
        await q.answer("❌ Недоступно", show_alert=True); conn.close(); return
    duel = dict(duel)
    if user_id == duel['challenger_id']:
        await q.answer("❌ Жди противника или нажми Отклонить", show_alert=True); conn.close(); return
    if user_id != duel['opponent_id']:
        await q.answer("❌ Этот вызов не тебе", show_alert=True); conn.close(); return
    opponent = get_user(user_id)
    if opponent['rys'] < duel['bet']:
        await q.answer(f"❌ Нужно {duel['bet']} RYS", show_alert=True); conn.close(); return
    await q.answer("⚔️ Бой!")
    update_user(user_id, rys=opponent['rys'] - duel['bet'])
    challenger = get_user(duel['challenger_id'])
    total = challenger['exp'] + opponent['exp'] + 100
    winner_id = duel['challenger_id'] if random.random() < (challenger['exp']+50)/total else user_id
    loser_id = user_id if winner_id == duel['challenger_id'] else duel['challenger_id']
    w = get_user(winner_id); l = get_user(loser_id)
    update_user(winner_id, rys=w['rys']+duel['prize'], duels_won=w['duels_won']+1)
    update_user(loser_id, duels_lost=l['duels_lost']+1)
    conn.execute(
        "UPDATE duels SET status='finished', winner_id=?, finished_at=CURRENT_TIMESTAMP WHERE duel_id=?",
        (winner_id, duel_id)
    ); conn.commit(); conn.close()
    ev = random.choice(["💥 Мощный удар!","🎯 Точный выстрел!","👊👊👊 Серия!","🔄 Контратака!","💫 Нокаут!","⚡ Молния!","🌪 Вихрь!"])
    await q.edit_message_text(
        f"⚔️ ДУЭЛЬ\n👤 {challenger['first_name']} vs {opponent['first_name']}\n💵 {duel['bet']}\n{ev}\n🏆 {w['first_name']}\n💰 {duel['prize']} RYS"
    )

async def duel_decline_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; duel_id = q.data.split('_')[2]; user_id = str(q.from_user.id)
    conn = get_db(); duel = conn.execute("SELECT * FROM duels WHERE duel_id = ?", (duel_id,)).fetchone()
    if not duel or duel['status'] != 'waiting' or user_id != duel['challenger_id']:
        await q.answer("❌ Только создатель", show_alert=True); conn.close(); return
    update_user(duel['challenger_id'], rys=get_user(duel['challenger_id'])['rys'] + duel['bet'])
    conn.execute("UPDATE duels SET status='declined' WHERE duel_id=?", (duel_id,)); conn.commit(); conn.close()
    await q.edit_message_text(f"❌ Отменена\n💰 {duel['bet']} RYS возвращены")

async def duel_info_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer("⚔️ Ставка x2\nШанс зависит от EXP\nУ создателя бонус", show_alert=True)

# ==================== СЧЁТЧИК СООБЩЕНИЙ ====================

async def count_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    user_id = update.effective_user.id
    username = update.effective_user.username
    first_name = update.effective_user.first_name
    ensure_user(user_id, username, first_name)
    if len(update.message.text) >= 2:
        user = get_user(user_id)
        update_user(user_id, total_messages=user['total_messages'] + 1)
        increment_weekly_message(user_id)
        weekly = get_weekly_messages()
        logger.info(f"📝 Сообщение: {first_name} — за неделю: {weekly.get(str(user_id), 0)}")

# ==================== АДМИН-ПАНЕЛЬ ====================

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Нет доступа")
        return
    
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
        await update.message.reply_text(f"🛡 {update.message.reply_to_message.from_user.first_name}", reply_markup=InlineKeyboardMarkup(kb))
        return
    
    kb = [
        [InlineKeyboardButton("📋 Реестр", callback_data="admin_list")],
        [InlineKeyboardButton("📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton("🏦 Банк", callback_data="admin_bank_info")],
        [InlineKeyboardButton("🗑 Сбросить все игры", callback_data="admin_reset_games")]
    ]
    await update.message.reply_text("🛡 АДМИН-ПАНЕЛЬ", reply_markup=InlineKeyboardMarkup(kb))

async def admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.from_user.id != ADMIN_ID:
        await q.answer("❌ Нет доступа"); return
    
    await q.answer()
    p = q.data.split('_')
    a = p[1]
    
    if a == 'info':
        u = get_user(p[2]); w = get_weekly_messages()
        await q.edit_message_text(f"📊 {u['first_name']}\n💰{u['rys']} ⭐{u['rep']} ✨{u['exp']}\n🎖{get_rank(u['exp'])}\n⚔️{u['duels_won']}/{u['duels_lost']}\n💬{w.get(p[2],0)}")
    
    elif a in ['add', 'sub']:
        context.user_data['admin_action'] = {'tid': p[3], 'cur': p[2], 'op': a}
        await q.edit_message_text(f"✏️ {'+' if a=='add' else '-'}{p[2].upper()}")
    
    elif a == 'delete':
        conn = get_db()
        u = conn.execute("SELECT first_name FROM users WHERE user_id=?", (p[2],)).fetchone()
        if u:
            conn.execute("DELETE FROM users WHERE user_id=?", (p[2],))
            conn.commit()
            await q.edit_message_text(f"✅ {u['first_name']} удален")
        else:
            await q.edit_message_text("❌ Не найден")
        conn.close()
    
    elif a == 'add':
        u = get_user(p[2])
        await q.edit_message_text(f"✅ {u['first_name']} в БД")
    
    elif a == 'list':
        conn = get_db()
        ul = conn.execute("SELECT * FROM users ORDER BY exp DESC").fetchall()
        conn.close()
        if not ul:
            await q.edit_message_text("📋 Пусто"); return
        page = context.user_data.get('ap', 0)
        tp = (len(ul)-1)//10+1
        text = f"📋 ({page+1}/{tp})\n"
        for u in ul[page*10:(page+1)*10]:
            text += f"👤 {u['first_name']} | 💰{u['rys']} | ✨{u['exp']} | 🎖{get_rank(u['exp'])}\n"
        kb = []
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️", callback_data=f"admin_page_{page-1}"))
        if page < tp-1:
            nav.append(InlineKeyboardButton("➡️", callback_data=f"admin_page_{page+1}"))
        if nav:
            kb.append(nav)
        kb.append([InlineKeyboardButton("🔙", callback_data="admin_back")])
        await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb))
    
    elif a == 'page':
        context.user_data['ap'] = int(p[2])
        await admin_callback(update, context)
    
    elif a == 'back':
        kb = [
            [InlineKeyboardButton("📋 Реестр", callback_data="admin_list")],
            [InlineKeyboardButton("📢 Рассылка", callback_data="admin_broadcast")],
            [InlineKeyboardButton("🏦 Банк", callback_data="admin_bank_info")],
            [InlineKeyboardButton("🗑 Сбросить все игры", callback_data="admin_reset_games")]
        ]
        await q.edit_message_text("🛡 АДМИН-ПАНЕЛЬ", reply_markup=InlineKeyboardMarkup(kb))
    
    elif a == 'bank':
        conn = get_db()
        total = conn.execute("SELECT total_commission FROM bank WHERE id=1").fetchone()['total_commission']
        recent = conn.execute("SELECT * FROM bank_history ORDER BY id DESC LIMIT 5").fetchall()
        conn.close()
        text = f"🏦 {total} RYS\n"
        for op in recent:
            text += f"• {op['reason']}: +{op['amount']}\n"
        await q.edit_message_text(text)
    
    elif a == 'reset':
        conn = get_db()
        c = conn.execute("UPDATE cases SET status='finished' WHERE status IN ('waiting','active')").rowcount
        d = conn.execute("UPDATE duels SET status='finished' WHERE status='waiting'").rowcount
        conn.commit()
        conn.close()
        await q.edit_message_text(f"✅ Сброшено: {c} кейсов, {d} дуэлей")
    
    elif a == 'broadcast':
        context.user_data['broadcast'] = True
        await q.edit_message_text("📢 Напиши сообщение для рассылки:")

async def handle_admin_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
    if update.message.text == '/cancel':
        context.user_data.pop('broadcast', None)
        context.user_data.pop('admin_action', None)
        await update.message.reply_text("❌ Отменено")
        return
    
    if 'admin_action' in context.user_data:
        ac = context.user_data['admin_action']
        try:
            amount = int(update.message.text)
        except ValueError:
            await update.message.reply_text("❌ Число"); return
        u = get_user(ac['tid']); f = ac['cur']
        update_user(ac['tid'], **{f: u[f] + (amount if ac['op']=='add' else -amount)})
        u2 = get_user(ac['tid'])
        await update.message.reply_text(f"✅ {u2['first_name']}\n{f.upper()}: {'+'if ac['op']=='add' else ''}{amount}\n💰{u2['rys']} ⭐{u2['rep']} ✨{u2['exp']}")
        del context.user_data['admin_action']
        return
    
    if context.user_data.get('broadcast'):
        sent = 0
        conn = get_db()
        ul = conn.execute("SELECT user_id FROM users").fetchall()
        conn.close()
        for u in ul:
            try:
                await context.bot.send_message(int(u['user_id']), f"📢 Рассылка\n\n{update.message.text}")
                sent += 1
                await asyncio.sleep(0.5)
            except:
                pass
        await update.message.reply_text(f"✅ {sent}/{len(ul)}")
        del context.user_data['broadcast']

# ==================== ЕЖЕНЕДЕЛЬНЫЙ СБРОС ====================

async def weekly_reset():
    logger.info("=== СБРОС ===")
    conn = get_db()
    w = conn.execute("SELECT user_id, messages FROM weekly_stats WHERE messages > 0").fetchall()
    if not w:
        logger.info("Нет сообщений"); conn.close(); return
    s = sorted(w, key=lambda x: x['messages'], reverse=True)
    er = {1:500,2:400,3:300,4:200,5:150,6:100,7:75,8:50,9:30,10:20}
    bd = {1:0.5,2:0.3,3:0.2}
    tb = conn.execute("SELECT total_commission FROM bank WHERE id=1").fetchone()['total_commission']
    dist = 0
    for i, (uid, cnt) in enumerate([(r['user_id'], r['messages']) for r in s[:10]], 1):
        conn.execute("UPDATE users SET exp = exp + ? WHERE user_id = ?", (er.get(i,0), uid))
        br = int(tb * bd[i]) if i <= 3 and tb > 0 else 0
        if br:
            conn.execute("UPDATE users SET rys = rys + ? WHERE user_id = ?", (br, uid))
            dist += br
        try:
            await context.bot.send_message(int(uid), f"🏆 Место {i}\n💬 {cnt}\n✨ +{er.get(i,0)} EXP" + (f"\n💰 +{br} RYS" if br else ""))
        except:
            pass
    conn.execute("UPDATE bank SET total_commission = 0 WHERE id = 1")
    if dist:
        conn.execute("INSERT INTO bank_history (amount, reason) VALUES (?, 'weekly')", (dist,))
    conn.execute("DELETE FROM weekly_stats")
    conn.execute("INSERT OR REPLACE INTO weekly_reset (id, last_reset) VALUES (1, CURRENT_TIMESTAMP)")
    conn.commit()
    conn.close()
    logger.info(f"Сброс: {dist} RYS")

# ==================== ЗАПУСК ====================

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    err = context.error
    if isinstance(err, (NetworkError, TimedOut)):
        logger.error(f"⏰ {err}")
    elif isinstance(err, RetryAfter):
        await asyncio.sleep(err.retry_after)
    else:
        logger.error(f"❌ {err}", exc_info=True)

if __name__ == "__main__":
    if not TOKEN:
        logger.error("❌ Нет токена")
        exit(1)
    
    app = Application.builder().token(TOKEN).connect_timeout(30).read_timeout(30).write_timeout(30).build()
    app.add_error_handler(error_handler)
    
    async def start_scheduler(app):
        scheduler = AsyncIOScheduler(timezone=pytz.UTC)
        scheduler.add_job(weekly_reset, 'cron', day_of_week='mon', hour=0, minute=0)
        scheduler.start()
    
    app.post_init = start_scheduler
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("s_help", s_help_command))
    app.add_handler(CommandHandler("balance", balance_cmd))
    app.add_handler(CommandHandler("send", send_rys))
    app.add_handler(CommandHandler("case", case_game))
    app.add_handler(CommandHandler("duel", duel_create))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("top", top_weekly))
    app.add_handler(CommandHandler("admin", admin_panel))
    
    app.add_handler(CallbackQueryHandler(case_accept_callback, pattern="^case_accept_"))
    app.add_handler(CallbackQueryHandler(case_decline_callback, pattern="^case_decline_"))
    app.add_handler(CallbackQueryHandler(handle_case_open, pattern="^case_open_"))
    app.add_handler(CallbackQueryHandler(case_info_callback, pattern="^case_info_"))
    app.add_handler(CallbackQueryHandler(duel_accept_callback, pattern="^duel_accept_"))
    app.add_handler(CallbackQueryHandler(duel_decline_callback, pattern="^duel_decline_"))
    app.add_handler(CallbackQueryHandler(duel_info_callback, pattern="^duel_info_"))
    app.add_handler(CallbackQueryHandler(admin_callback, pattern="^admin_"))
    
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.REPLY, rep_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, count_message))
    app.add_handler(MessageHandler(filters.TEXT & filters.User(ADMIN_ID), handle_admin_text))
    
    logger.info("✅ Бот запущен")
    app.run_polling(allowed_updates=Update.ALL_TYPES)
