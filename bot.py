import os
import logging
from pathlib import Path
from typing import Dict, Any, List, Set, Optional
from dotenv import load_dotenv
from datetime import datetime, timedelta
import re
import sqlite3
import random

# Загрузка переменных окружения из .env файла
load_dotenv()

import asyncio
import json
import time
import aiohttp

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    Message, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery, LabeledPrice, PreCheckoutQuery,
    SuccessfulPayment, WebAppInfo
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums import ParseMode

# Для ЮKassa
try:
    from yookassa import Payment, Configuration
    from yookassa.domain.notification import WebhookNotificationEventType, WebhookNotificationFactory
    YOOKASSA_AVAILABLE = True
except ImportError:
    YOOKASSA_AVAILABLE = False
    logging.warning("⚠️ ЮKassa не установлена. Установите: pip install yookassa")

# ================= СИСТЕМА РАНГОВ =================
RANKS = [
    {"name": "🌱 Новенький", "min_messages": 0, "emoji": "🌱", "color": "⚪️"},
    {"name": "🥉 Бронза I", "min_messages": 100, "emoji": "🥉", "color": "🟤"},
    {"name": "🥉 Бронза II", "min_messages": 200, "emoji": "🥉", "color": "🟤"},
    {"name": "🥉 Бронза III", "min_messages": 500, "emoji": "🥉", "color": "🟤"},
    {"name": "🥈 Серебро I", "min_messages": 1000, "emoji": "🥈", "color": "⚪️"},
    {"name": "🥈 Серебро II", "min_messages": 2000, "emoji": "🥈", "color": "⚪️"},
    {"name": "🥈 Серебро III", "min_messages": 3000, "emoji": "🥈", "color": "⚪️"},
    {"name": "🏅 Золото I", "min_messages": 5000, "emoji": "🏅", "color": "🟡"},
    {"name": "🏅 Золото II", "min_messages": 7500, "emoji": "🏅", "color": "🟡"},
    {"name": "🏅 Золото III", "min_messages": 10000, "emoji": "🏅", "color": "🟡"},
    {"name": "💎 Платина", "min_messages": 15000, "emoji": "💎", "color": "🔵"},
    {"name": "👑 Алмаз", "min_messages": 20000, "emoji": "👑", "color": "💜"},
    {"name": "🔥 Мастер", "min_messages": 30000, "emoji": "🔥", "color": "🔴"},
    {"name": "🌟 Легенда", "min_messages": 50000, "emoji": "🌟", "color": "✨"},
    {"name": "👾 Гуру чата", "min_messages": 75000, "emoji": "👾", "color": "🌈"},
    {"name": "⚡️ Бог общения", "min_messages": 100000, "emoji": "⚡️", "color": "💫"},
]

# ================= УРОВНИ VIP =================
VIP_LEVELS = {
    "bronze": {"emoji": "👑", "name": "Бронзовый VIP", "min_days": 30, "color": "🟤"},
    "silver": {"emoji": "👑✨", "name": "Серебряный VIP", "min_days": 90, "color": "⚪️"},
    "gold": {"emoji": "👑🔥", "name": "Золотой VIP", "min_days": 180, "color": "🟡"},
    "platinum": {"emoji": "👑💫", "name": "Платиновый VIP", "min_days": 365, "color": "🔵"},
    "forever": {"emoji": "👑⚡️", "name": "Королевский VIP", "min_days": 999999, "color": "💫", "forever": True}
}

# ================= ФУНКЦИЯ ПОЛУЧЕНИЯ УРОВНЯ VIP =================
def get_vip_level(total_days: int) -> dict:
    """Получить уровень VIP по количеству дней"""
    if total_days >= 999999:
        return VIP_LEVELS["forever"]
    elif total_days >= 365:
        return VIP_LEVELS["platinum"]
    elif total_days >= 180:
        return VIP_LEVELS["gold"]
    elif total_days >= 90:
        return VIP_LEVELS["silver"]
    else:
        return VIP_LEVELS["bronze"]

# ================= ТУРНИРНАЯ СИСТЕМА =================
TOURNAMENT_NAMES = {
    "герои": ["Воин", "Маг", "Лучник", "Рыцарь", "Паладин", "Друид", "Шаман", "Варвар"],
    "животные": ["Волк", "Лис", "Медведь", "Орел", "Сокол", "Тигр", "Лев", "Пантера"],
    "стихии": ["Огонь", "Вода", "Ветер", "Земля", "Молния", "Лед", "Туман", "Буря"],
    "космос": ["Звезда", "Комета", "Галактика", "Туманность", "Солнце", "Луна", "Марс", "Венера"],
    "мистика": ["Призрак", "Тень", "Фантом", "Дух", "Сфинкс", "Феникс", "Дракон", "Единорог"]
}

TOURNAMENT_ADJECTIVES = [
    "Храбрый", "Мудрый", "Быстрый", "Сильный", "Таинственный",
    "Древний", "Скрытный", "Дикий", "Свободный", "Вечный",
    "Лунный", "Солнечный", "Звездный", "Теневой", "Огненный"
]

# ================= ЦВЕТА ДЛЯ VIP =================
VIP_COLORS = {
    "red": "🔴 Красный",
    "blue": "🔵 Синий",
    "green": "🟢 Зеленый",
    "yellow": "🟡 Желтый",
    "purple": "🟣 Фиолетовый",
    "orange": "🟠 Оранжевый"
}

VIP_COLOR_CODES = {
    "red": "#FF0000",
    "blue": "#0000FF",
    "green": "#00FF00",
    "yellow": "#FFFF00",
    "purple": "#800080",
    "orange": "#FFA500"
}

# ================= АНОНИМНЫЕ ИМЕНА ДЛЯ ТУРНИРОВ =================
def generate_tournament_name(user_id: int, tournament_id: str) -> str:
    """Генерирует анонимное имя для турнира"""
    seed = hash(f"{user_id}_{tournament_id}")
    random.seed(seed)
    
    category = random.choice(list(TOURNAMENT_NAMES.keys()))
    name = random.choice(TOURNAMENT_NAMES[category])
    adjective = random.choice(TOURNAMENT_ADJECTIVES)
    number = random.randint(1, 99)
    
    random.seed()
    return f"{adjective} {name} #{number:02d}"

def get_rank(message_count: int) -> dict:
    """Получить ранг по количеству сообщений"""
    current_rank = RANKS[0]
    next_rank = None
    
    for i, rank in enumerate(RANKS):
        if message_count >= rank["min_messages"]:
            current_rank = rank
            if i + 1 < len(RANKS):
                next_rank = RANKS[i + 1]
        else:
            break
    
    return {
        "current": current_rank,
        "next": next_rank,
        "progress": message_count,
        "next_needed": next_rank["min_messages"] if next_rank else None,
        "progress_percent": calculate_progress(message_count, current_rank, next_rank)
    }

def calculate_progress(messages: int, current: dict, next_rank: dict) -> int:
    if not next_rank:
        return 100
    
    current_min = current["min_messages"]
    next_min = next_rank["min_messages"]
    needed = next_min - current_min
    current_progress = messages - current_min
    
    if needed == 0:
        return 100
    
    return min(100, int((current_progress / needed) * 100))

# ================= СИСТЕМА ФИЛЬТРАЦИИ 18+ =================
import re
from collections import defaultdict

BANNED_CONTENT = {
    "warning": {
        "keywords": [
            "порно", "эротика", "интим", "18+", "порн", "xxx",
            "порнуха", "порнушка", "клубничка", "эротический", "порнографический",
            "porn", "erotic", "nsfw", "xxx", "adult", "explicit",
            "1 8 +", "18 +",
        ],
        "ban_hours": 1,
        "strikes_before_permanent": 3
    },
    
    "serious": {
        "keywords": [
            "проститутк", "шлюх", "минет", "анальный", "вагина", "член", "пенис",
            "влагалище", "трахнуть", "трахать", "сексуальный", "половой акт",
            "оральный", "групповой", "жесткий", "порноактриса", "порнозвезда",
            "fuck", "dick", "cock", "pussy", "asshole", "blowjob", "handjob",
            "masturbat", "orgasm", "cum", "semen", "penis", "vagina",
            "breast", "tits", "boobs", "nipples", "clit", "labia",
            "fucking", "fucked", "motherfucker", "bitch", "whore", "slut",
        ],
        "ban_hours": 24,
        "strikes_before_permanent": 2
    },
    
    "critical": {
        "keywords": [
            "детск", "малолет", "педо",
            "насил", "растл", "изнасилован", "педофил",
            "педофилия", "инцест", "кровосмешение", "зоофилия",
            "child", "minor", "underage", "loli", "lolita", "pedo",
            "rape", "sexual assault", "molest", "abuse", "incest",
            "pedophile", "pedophilia", "child porn", "cp",
        ],
        "ban_hours": 168,
        "strikes_before_permanent": 999
    }
}

COMPILED_PATTERNS = {}
for level, data in BANNED_CONTENT.items():
    patterns = []
    for keyword in data["keywords"]:
        pattern = re.escape(keyword).replace(r'\ ', r'\s*')
        patterns.append(re.compile(r'\b' + pattern + r'\b', re.IGNORECASE))
    COMPILED_PATTERNS[level] = patterns

user_violations = defaultdict(lambda: {"warning": 0, "serious": 0, "critical": 0})

def check_content(text: str) -> tuple:
    if not text:
        return None, "", False
    
    text_lower = text.lower()
    
    age_pattern = re.compile(r'\b(\d{1,2})\s*(лет|год|года)\b', re.IGNORECASE)
    if age_pattern.search(text_lower):
        for pattern in COMPILED_PATTERNS["serious"] + COMPILED_PATTERNS["critical"]:
            if pattern.search(text_lower):
                if "шлюх" in text_lower or "трах" in text_lower or "секс" in text_lower:
                    return "serious", f"Обнаружен запрещенный контент с упоминанием возраста", True
                break
        return None, "", False
    
    for pattern in COMPILED_PATTERNS["critical"]:
        if pattern.search(text_lower):
            return "critical", f"Обнаружен критический запрещенный контент", True
    
    for pattern in COMPILED_PATTERNS["serious"]:
        if pattern.search(text_lower):
            return "serious", f"Обнаружен серьезный контент 18+", True
    
    for pattern in COMPILED_PATTERNS["warning"]:
        if pattern.search(text_lower):
            return "warning", f"Обнаружен контент 18+", True
    
    return None, "", False

async def handle_violation(user_id: int, message: Message, level: str, reason: str) -> bool:
    violation_data = BANNED_CONTENT[level]
    
    user_violations[user_id][level] += 1
    strikes = user_violations[user_id][level]
    
    ban_hours = violation_data["ban_hours"]
    ban_text = f"{ban_hours} час{'ов' if ban_hours > 1 else ''}"
    if ban_hours >= 24:
        days = ban_hours // 24
        ban_text = f"{days} дн{'ень' if days == 1 else 'ей'}"
    
    db.add_violation(
        user_id=user_id,
        username=message.from_user.username or "",
        full_name=message.from_user.full_name,
        violation_type=f"{level}_content",
        message_text=message.text or message.caption or ""
    )
    
    db.ban_user(
        user_id=user_id,
        username=message.from_user.username or "",
        full_name=message.from_user.full_name,
        reason=f"{level}: {reason} (нарушение #{strikes})",
        admin_id=None,
        duration_hours=ban_hours
    )
    
    if user_id in pairs:
        partner_id = pairs[user_id]
        
        if user_id in pairs:
            del pairs[user_id]
        if partner_id in pairs:
            del pairs[partner_id]
        
        save_data()
        
        try:
            await bot.send_message(
                partner_id,
                "❌ <b>Диалог завершен</b>\n\n"
                "Ваш собеседник был забанен за нарушение правил.",
                parse_mode=ParseMode.HTML,
                reply_markup=main_kb
            )
            logger.info(f"Диалог между {user_id} и {partner_id} завершен из-за бана")
        except Exception as e:
            logger.error(f"Не удалось уведомить собеседника {partner_id} о завершении диалога: {e}")
    
    user_message = (
        f"🚫 <b>Вы забанены {ban_text}!</b>\n\n"
        f"<b>Причина:</b> {reason}\n"
        f"<b>Нарушение:</b> #{strikes}\n"
        f"<b>Длительность:</b> {ban_text}\n\n"
    )
    
    if level == "warning":
        remaining = violation_data.get("strikes_before_permanent", 3) - strikes
        if remaining > 0:
            user_message += f"⚠️ <b>Предупреждение!</b> Еще {remaining} нарушение{'ий' if remaining > 1 else ''} - бан на 24 часа!\n"
    elif level == "serious":
        remaining = violation_data.get("strikes_before_permanent", 2) - strikes
        if remaining > 0:
            user_message += f"⚠️ <b>Предупреждение!</b> Еще {remaining} нарушение - бан на 7 дней!\n"
    elif level == "critical":
        user_message += f"⚠️ <b>Это серьезное нарушение!</b> При повторении - бан на 7 дней!\n"
    
    try:
        await message.answer(user_message, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Не удалось отправить уведомление пользователю {user_id}: {e}")
    
    await notify_admin(
        f"🚨 <b>Автоматический бан</b>\n\n"
        f"<b>Уровень:</b> {level.upper()}\n"
        f"<b>Длительность:</b> {ban_text}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>Пользователь:</b> <code>{user_id}</code>\n"
        f"📛 <b>Имя:</b> {message.from_user.full_name}\n"
        f"🔗 <b>Username:</b> @{message.from_user.username or 'нет'}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📌 <b>Причина:</b> {reason}\n"
        f"🔢 <b>Нарушение #:</b> {strikes}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💬 <b>Сообщение:</b> {(message.text or message.caption or '')[:200]}",
        parse_mode=ParseMode.HTML
    )
    
    logger.info(f"Пользователь {user_id} забанен: {level}, причина: {reason}, нарушение #{strikes}")
    return True

# ================= НАСТРОЙКА ЛОГГИРОВАНИЯ =================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ================= НАСТРОЙКИ ИЗ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ =================
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    logger.error("❌ BOT_TOKEN не найден в переменных окружения!")
    raise ValueError("BOT_TOKEN не найден в переменных окружения")

ADMIN_IDS: Set[int] = set()
admin_ids_str = os.getenv("ADMIN_IDS", "")
if admin_ids_str:
    try:
        ADMIN_IDS = {int(id.strip()) for id in admin_ids_str.split(",") if id.strip().isdigit()}
        logger.info(f"✅ Загружены ID администраторов: {ADMIN_IDS}")
    except Exception as e:
        logger.error(f"❌ Ошибка при парсинге ADMIN_IDS: {e}")

YOOKASSA_SHOP_ID = os.getenv("YOOKASSA_SHOP_ID")
YOOKASSA_SECRET_KEY = os.getenv("YOOKASSA_SECRET_KEY")

PARENT_IP_NAME = os.getenv("PARENT_IP_NAME", "ИП")
PARENT_IP_INN = os.getenv("PARENT_IP_INN", "")
PARENT_IP_OGRN = os.getenv("PARENT_IP_OGRN", "")
PARENT_IP_ADDRESS = os.getenv("PARENT_IP_ADDRESS", "")
PARENT_IP_EMAIL = os.getenv("PARENT_IP_EMAIL", "")
PARENT_IP_PHONE = os.getenv("PARENT_IP_PHONE", "")

OFFER_URL = os.getenv("OFFER_URL", "#")
PRIVACY_URL = os.getenv("PRIVACY_URL", "#")
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "@support")

DEFAULT_COUNTRY = os.getenv("DEFAULT_COUNTRY", "RU")
FORCE_YOOKASSA_FOR_RU = os.getenv("FORCE_YOOKASSA_FOR_RU", "true").lower() == "true"
ALLOW_STARS_FOR_RU = os.getenv("ALLOW_STARS_FOR_RU", "false").lower() == "true"

VIP_PLANS_STARS: Dict[int, Dict[str, Any]] = {
    3:   {"seconds": 3 * 86400,   "stars": 19,   "title": "VIP на 3 дня", "level": "bronze"},
    7:   {"seconds": 7 * 86400,   "stars": 49,   "title": "VIP на 7 дней", "level": "bronze"},
    30:  {"seconds": 30 * 86400,  "stars": 99,   "title": "VIP на месяц", "level": "bronze"},
    90:  {"seconds": 90 * 86400,  "stars": 199,  "title": "VIP на 3 месяца", "level": "silver"},
    180: {"seconds": 180 * 86400, "stars": 299,  "title": "VIP на 6 месяцев", "level": "gold"},
    365: {"seconds": 365 * 86400, "stars": 499,  "title": "VIP на 1 год", "level": "platinum"},
}

VIP_PLANS_RUB: Dict[int, Dict[str, Any]] = {
    3:   {"seconds": 3 * 86400,   "price": 49,   "title": "VIP на 3 дня", "level": "bronze"},
    7:   {"seconds": 7 * 86400,   "price": 99,   "title": "VIP на 7 дней", "level": "bronze"},
    30:  {"seconds": 30 * 86400,  "price": 299,  "title": "VIP на месяц", "level": "bronze"},
    90:  {"seconds": 90 * 86400,  "price": 799,  "title": "VIP на 3 месяца", "level": "silver"},
    180: {"seconds": 180 * 86400, "price": 1499, "title": "VIP на 6 месяцев", "level": "gold"},
    365: {"seconds": 365 * 86400, "price": 2499, "title": "VIP на 1 год", "level": "platinum"},
}

# Королевский VIP навсегда (не для продажи, только админ)
FOREVER_VIP = {"seconds": 999999 * 86400, "title": "Королевский VIP навсегда", "level": "forever"}

DATA_FILE = Path("bot_data.json")
PAYMENTS_DB_FILE = Path("payments.db")

CIS_COUNTRIES = {"RU", "UA", "BY", "KZ", "UZ", "AZ", "AM", "KG", "TJ", "MD", "GE", "TM"}

# ================= БАЗА ДАННЫХ =================
class Database:
    def __init__(self):
        self.conn = sqlite3.connect(PAYMENTS_DB_FILE, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.create_tables()
        self.migrate_vip_history()
        self.init_badges()
        self.remove_purchases_badges()
    
    def create_tables(self):
        cursor = self.conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL UNIQUE,
                username TEXT,
                full_name TEXT,
                messages INTEGER DEFAULT 0,
                dialogs INTEGER DEFAULT 0,
                reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT,
                full_name TEXT,
                days INTEGER NOT NULL,
                amount REAL NOT NULL,
                currency TEXT NOT NULL,
                payment_method TEXT NOT NULL,
                payment_id TEXT UNIQUE NOT NULL,
                status TEXT NOT NULL,
                country TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                metadata TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS vip_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                days INTEGER NOT NULL,
                level TEXT DEFAULT 'bronze',
                activated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP,
                payment_id INTEGER,
                FOREIGN KEY (payment_id) REFERENCES payments(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL UNIQUE,
                username TEXT,
                full_name TEXT,
                reason TEXT,
                admin_id INTEGER,
                banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                unbanned_at TIMESTAMP,
                duration_hours INTEGER DEFAULT 1
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS violations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT,
                full_name TEXT,
                violation_type TEXT NOT NULL,
                message_text TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS badges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                badge_id TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                description TEXT NOT NULL,
                emoji TEXT NOT NULL,
                rarity TEXT NOT NULL,
                category TEXT NOT NULL,
                requirement_type TEXT NOT NULL,
                requirement_value INTEGER NOT NULL
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_badges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                badge_id TEXT NOT NULL,
                earned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, badge_id)
            )
        ''')
        
        # НОВАЯ ТАБЛИЦА: для оценок диалогов и возврата собеседников
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dialog_ratings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                partner_id INTEGER NOT NULL,
                rating INTEGER,
                dialog_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                invite_sent INTEGER DEFAULT 0,
                invite_expires TIMESTAMP,
                UNIQUE(user_id, partner_id, dialog_date)
            )
        ''')
        
        # НОВАЯ ТАБЛИЦА: для турниров
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tournaments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tournament_id TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                anon_name TEXT NOT NULL,
                messages INTEGER DEFAULT 0,
                dialogs INTEGER DEFAULT 0,
                UNIQUE(tournament_id, user_id)
            )
        ''')
        
        # НОВАЯ ТАБЛИЦА: для кастомизации VIP
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS vip_customization (
                user_id INTEGER PRIMARY KEY,
                nickname_color TEXT DEFAULT '⚪️',
                message_frame TEXT DEFAULT 'none',
                favorite_color TEXT DEFAULT '⚪️'
            )
        ''')
        
        self.conn.commit()
    
    def migrate_vip_history(self):
        """Добавляет колонку level в таблицу vip_history, если её нет"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("ALTER TABLE vip_history ADD COLUMN level TEXT DEFAULT 'bronze'")
            self.conn.commit()
            logger.info("✅ Добавлена колонка level в таблицу vip_history")
        except sqlite3.OperationalError:
            # Колонка уже существует
            pass
    
    def init_badges(self):
        badges = [
            ("msg_100", "🗣 Болтун-любитель", "Отправить 100 сообщений", "🗣", "common", "messages", "messages", 100),
            ("msg_500", "💬 Опытный говорун", "Отправить 500 сообщений", "💬", "common", "messages", "messages", 500),
            ("msg_1000", "🎙️ Мастер разговора", "Отправить 1000 сообщений", "🎙️", "rare", "messages", "messages", 1000),
            ("msg_5000", "📢 Профессиональный болтун", "Отправить 5000 сообщений", "📢", "rare", "messages", "messages", 5000),
            ("msg_10000", "👑 Легенда чата", "Отправить 10000 сообщений", "👑", "epic", "messages", "messages", 10000),
            ("msg_50000", "⚡️ Бог общения", "Отправить 50000 сообщений", "⚡️", "legendary", "messages", "messages", 50000),
            ("msg_100000", "🌟 Бессмертный говорун", "Отправить 100000 сообщений", "🌟", "legendary", "messages", "messages", 100000),
            
            ("dial_5", "🤝 Знакомлюсь понемногу", "Пообщаться с 5 разными людьми", "🤝", "common", "dialogs", "dialogs", 5),
            ("dial_20", "🫂 Душа компании", "Пообщаться с 20 разными людьми", "🫂", "common", "dialogs", "dialogs", 20),
            ("dial_50", "🎉 Заводила", "Пообщаться с 50 разными людьми", "🎉", "rare", "dialogs", "dialogs", 50),
            ("dial_100", "⭐️ Звезда общения", "Пообщаться со 100 разными людьми", "⭐️", "rare", "dialogs", "dialogs", 100),
            ("dial_250", "🎪 Тусовщик", "Пообщаться с 250 разными людьми", "🎪", "epic", "dialogs", "dialogs", 250),
            ("dial_500", "🏛️ Городской голова", "Пообщаться с 500 разными людьми", "🏛️", "epic", "dialogs", "dialogs", 500),
            ("dial_1000", "👑 Властелин общения", "Пообщаться с 1000 разными людьми", "👑", "legendary", "dialogs", "dialogs", 1000),
            
            ("time_1", "🌱 Новичок", "Пользоваться ботом 1 день", "🌱", "common", "time", "days_active", 1),
            ("time_7", "🌿 Осваиваюсь", "Пользоваться ботом 7 дней", "🌿", "common", "time", "days_active", 7),
            ("time_30", "🌳 Свой человек", "Пользоваться ботом 30 дней", "🌳", "rare", "time", "days_active", 30),
            ("time_100", "🏡 Старожил", "Пользоваться ботом 100 дней", "🏡", "rare", "time", "days_active", 100),
            ("time_365", "🎂 Ветеран", "Пользоваться ботом 365 дней", "🎂", "epic", "time", "days_active", 365),
            ("time_730", "🏆 Легенда", "Пользоваться ботом 730 дней (2 года)", "🏆", "legendary", "time", "days_active", 730),
            
            ("night_10", "🦉 Ночной житель", "Отправить 10 сообщений ночью (0:00-4:00)", "🦉", "rare", "time_of_day", "night_messages", 10),
            ("night_50", "🌙 Сова", "Отправить 50 сообщений ночью (0:00-4:00)", "🌙", "epic", "time_of_day", "night_messages", 50),
            ("morning_10", "🐦 Ранняя пташка", "Отправить 10 сообщений утром (5:00-8:00)", "🐦", "rare", "time_of_day", "morning_messages", 10),
            ("morning_50", "☀️ Жаворонок", "Отправить 50 сообщений утром (5:00-8:00)", "☀️", "epic", "time_of_day", "morning_messages", 50),
            
            ("streak_3", "🔥 Огонек", "Заходить 3 дня подряд", "🔥", "common", "streak", "streak", 3),
            ("streak_7", "🔥🔥 Костёр", "Заходить 7 дней подряд", "🔥🔥", "rare", "streak", "streak", 7),
            ("streak_30", "🔥🔥🔥 Пламя", "Заходить 30 дней подряд", "🔥🔥🔥", "epic", "streak", "streak", 30),
            ("streak_100", "🔥🔥🔥🔥 Неугасимый", "Заходить 100 дней подряд", "🔥🔥🔥🔥", "legendary", "streak", "streak", 100),
            
            ("daily_100", "⚡ Ударник", "Отправить 100 сообщений за 1 день", "⚡", "rare", "daily", "daily_messages", 100),
            ("daily_500", "🏭 Стахановец", "Отправить 500 сообщений за 1 день", "🏭", "epic", "daily", "daily_messages", 500),
            ("daily_1000", "🖨️ Бешеный принтер", "Отправить 1000 сообщений за 1 день", "🖨️", "legendary", "daily", "daily_messages", 1000),
            
            ("first_dialog", "🚀 Первый шаг", "Начать первый диалог", "🚀", "common", "special", "first_dialog", 1),
            ("first_message", "📨 Первое сообщение", "Отправить первое сообщение", "📨", "common", "special", "first_message", 1),
            ("marathon", "🏃 Марафонец", "Диалог длиной больше 1 часа", "🏃", "rare", "special", "long_dialog", 1),
            ("countries_10", "🌍 Космополит", "Пообщаться с людьми из 10 стран", "🌍", "rare", "special", "countries", 10),
            ("countries_25", "🌎 Путешественник", "Пообщаться с людьми из 25 стран", "🌎", "epic", "special", "countries", 25),
            ("countries_50", "🌏 Гражданин мира", "Пообщаться с людьми из 50 стран", "🌏", "legendary", "special", "countries", 50),
            
            ("new_year", "🎄 Новогодний", "Зайти в бота 31 декабря или 1 января", "🎄", "rare", "holiday", "new_year", 1),
            ("march_8", "🌷 Весенний", "Зайти в бота 8 марта", "🌷", "rare", "holiday", "march_8", 1),
            ("feb_23", "🛡️ Защитник", "Зайти в бота 23 февраля", "🛡️", "rare", "holiday", "feb_23", 1),
            ("halloween", "🎃 Хэллоуин", "Зайти в бота 31 октября", "🎃", "rare", "holiday", "halloween", 1),
            ("birthday", "🎂 День рождения", "Зайти в бота в свой день рождения", "🎂", "epic", "holiday", "birthday", 1),
            
            # Турнирные значки
            ("tournament_winner", "🏆 Чемпион турнира", "Победить в еженедельном турнире", "🏆", "epic", "tournament", "tournament_wins", 1),
            ("tournament_participant", "🎯 Участник турнира", "Принять участие в турнире", "🎯", "common", "tournament", "tournament_participations", 1),
        ]
        
        cursor = self.conn.cursor()
        for badge in badges:
            cursor.execute('''
                INSERT OR IGNORE INTO badges 
                (badge_id, name, description, emoji, rarity, category, requirement_type, requirement_value)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', badge)
        
        self.conn.commit()
    
    def remove_purchases_badges(self):
        cursor = self.conn.cursor()
        
        purchase_badges = [
            "first_purchase",
            "vip_3", "vip_7", "vip_30", "vip_90",
            "vip_all",
            "stars_100", "stars_500", "stars_1000"
        ]
        
        for badge_id in purchase_badges:
            cursor.execute("DELETE FROM badges WHERE badge_id = ?", (badge_id,))
            cursor.execute("DELETE FROM user_badges WHERE badge_id = ?", (badge_id,))
        
        self.conn.commit()
        logger.info(f"✅ Удалены платные значки из базы данных")
    
    def add_message(self, user_id: int, username: str, full_name: str):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO users_stats (user_id, username, full_name, messages, dialogs)
            VALUES (?, ?, ?, 1, 0)
            ON CONFLICT(user_id) DO UPDATE SET
                messages = messages + 1,
                username = excluded.username,
                full_name = excluded.full_name
        ''', (user_id, username, full_name))
        self.conn.commit()
        
        # Обновляем турнирную статистику
        self.update_tournament_stats(user_id, messages_increment=1)
        self.check_new_badges(user_id)
    
    def add_dialog(self, user_id: int):
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE users_stats SET dialogs = dialogs + 1 WHERE user_id = ?
        ''', (user_id,))
        self.conn.commit()
        
        # Обновляем турнирную статистику
        self.update_tournament_stats(user_id, dialogs_increment=1)
        self.check_new_badges(user_id)
    
    def get_stats(self, user_id: int) -> dict:
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT messages, dialogs FROM users_stats WHERE user_id = ?
        ''', (user_id,))
        result = cursor.fetchone()
        if result:
            return {
                "messages": result[0] or 0,
                "dialogs": result[1] or 0
            }
        return {"messages": 0, "dialogs": 0}
    
    def get_user_badges(self, user_id: int) -> List[dict]:
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT b.badge_id, b.name, b.description, b.emoji, b.rarity, b.category
            FROM user_badges ub
            JOIN badges b ON ub.badge_id = b.badge_id
            WHERE ub.user_id = ?
            ORDER BY ub.earned_at DESC
        ''', (user_id,))
        
        badges = []
        for row in cursor.fetchall():
            badges.append({
                "id": row[0],
                "name": row[1],
                "description": row[2],
                "emoji": row[3],
                "rarity": row[4],
                "category": row[5]
            })
        return badges
    
    def get_all_badges(self) -> List[dict]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT badge_id, name, description, emoji, rarity, category FROM badges ORDER BY category, badge_id')
        badges = []
        for row in cursor.fetchall():
            badges.append({
                "id": row[0],
                "name": row[1],
                "description": row[2],
                "emoji": row[3],
                "rarity": row[4],
                "category": row[5]
            })
        return badges
    
    def check_badge(self, user_id: int, badge_id: str) -> bool:
        cursor = self.conn.cursor()
        cursor.execute('SELECT 1 FROM user_badges WHERE user_id = ? AND badge_id = ?', (user_id, badge_id))
        return cursor.fetchone() is not None
    
    def check_new_badges(self, user_id: int) -> List[dict]:
        cursor = self.conn.cursor()
        
        stats = self.get_stats(user_id)
        
        cursor.execute('SELECT * FROM badges')
        all_badges = cursor.fetchall()
        
        cursor.execute('SELECT badge_id FROM user_badges WHERE user_id = ?', (user_id,))
        earned = {row[0] for row in cursor.fetchall()}
        
        new_badges = []
        
        for badge in all_badges:
            badge_id = badge[1]
            if badge_id in earned:
                continue
            
            req_type = badge[7]
            req_value = badge[8]
            awarded = False
            
            if req_type == "messages" and stats["messages"] >= req_value:
                awarded = True
            elif req_type == "dialogs" and stats["dialogs"] >= req_value:
                awarded = True
            
            if awarded:
                cursor.execute('''
                    INSERT INTO user_badges (user_id, badge_id)
                    VALUES (?, ?)
                ''', (user_id, badge_id))
                
                new_badges.append({
                    "id": badge_id,
                    "name": badge[2],
                    "description": badge[3],
                    "emoji": badge[4],
                    "rarity": badge[5],
                    "category": badge[6]
                })
        
        self.conn.commit()
        return new_badges
    
    def add_payment(self, user_id: int, username: str, full_name: str, days: int, level: str,
                   amount: float, currency: str, method: str, payment_id: str, 
                   country: str = None, metadata: str = None):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO payments 
            (user_id, username, full_name, days, amount, currency, payment_method, 
             payment_id, status, country, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
        ''', (user_id, username, full_name, days, amount, currency, method, 
              payment_id, country, metadata))
        payment_db_id = cursor.lastrowid
        
        expires_at = datetime.now() + timedelta(days=days) if days < 999999 else None
        cursor.execute('''
            INSERT INTO vip_history (user_id, days, level, expires_at, payment_id)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, days, level, expires_at, payment_db_id))
        
        self.conn.commit()
        return payment_db_id
    
    def update_payment_status(self, payment_id: str, status: str):
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE payments 
            SET status = ?, completed_at = CURRENT_TIMESTAMP
            WHERE payment_id = ?
        ''', (status, payment_id))
        self.conn.commit()
    
    def add_vip_activation(self, user_id: int, days: int, level: str, payment_id: int = None):
        """Добавляет запись о активации VIP в историю"""
        cursor = self.conn.cursor()
        expires_at = datetime.now() + timedelta(days=days) if days < 999999 else None
        cursor.execute('''
            INSERT INTO vip_history (user_id, days, level, expires_at, payment_id)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, days, level, expires_at, payment_id))
        self.conn.commit()
    
    def get_vip_stats(self) -> dict:
        cursor = self.conn.cursor()
        
        cursor.execute("SELECT COUNT(DISTINCT user_id) FROM vip_history WHERE expires_at > CURRENT_TIMESTAMP OR expires_at IS NULL")
        total_vip = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT COUNT(*) FROM users_stats WHERE user_id IN (SELECT user_id FROM vip_history WHERE expires_at > CURRENT_TIMESTAMP OR expires_at IS NULL)")
        online_vip = cursor.fetchone()[0] or 0
        
        return {
            "total": total_vip,
            "online": online_vip,
        }
    
    def get_vip_history(self, user_id: int) -> List[tuple]:
        """Получает историю VIP статусов пользователя"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT days, level, activated_at, expires_at
            FROM vip_history 
            WHERE user_id = ?
            ORDER BY activated_at DESC
            LIMIT 10
        ''', (user_id,))
        return cursor.fetchall()
    
    # ================= НОВЫЕ МЕТОДЫ ДЛЯ ТУРНИРОВ =================
    
    def get_current_tournament_id(self) -> str:
        """Получает ID текущего турнира (недельный)"""
        now = datetime.now()
        # Турнир длится неделю, начинается в понедельник
        start_of_week = now - timedelta(days=now.weekday())
        return start_of_week.strftime("%Y-%m-%d")
    
    def update_tournament_stats(self, user_id: int, messages_increment: int = 0, dialogs_increment: int = 0):
        """Обновляет статистику пользователя в текущем турнире"""
        tournament_id = self.get_current_tournament_id()
        cursor = self.conn.cursor()
        
        # Проверяем, есть ли уже запись
        cursor.execute('''
            SELECT * FROM tournaments WHERE tournament_id = ? AND user_id = ?
        ''', (tournament_id, user_id))
        
        if cursor.fetchone():
            # Обновляем существующую запись
            if messages_increment > 0:
                cursor.execute('''
                    UPDATE tournaments SET messages = messages + ? 
                    WHERE tournament_id = ? AND user_id = ?
                ''', (messages_increment, tournament_id, user_id))
            if dialogs_increment > 0:
                cursor.execute('''
                    UPDATE tournaments SET dialogs = dialogs + ? 
                    WHERE tournament_id = ? AND user_id = ?
                ''', (dialogs_increment, tournament_id, user_id))
        else:
            # Создаем новую запись с анонимным именем
            anon_name = generate_tournament_name(user_id, tournament_id)
            cursor.execute('''
                INSERT INTO tournaments (tournament_id, user_id, anon_name, messages, dialogs)
                VALUES (?, ?, ?, ?, ?)
            ''', (tournament_id, user_id, anon_name, messages_increment, dialogs_increment))
        
        self.conn.commit()
    
    def get_tournament_leaderboard(self, tournament_id: str = None, limit: int = 10) -> List[dict]:
        """Получает таблицу лидеров турнира"""
        if tournament_id is None:
            tournament_id = self.get_current_tournament_id()
        
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT anon_name, messages, dialogs
            FROM tournaments
            WHERE tournament_id = ?
            ORDER BY messages DESC
            LIMIT ?
        ''', (tournament_id, limit))
        
        results = []
        for row in cursor.fetchall():
            results.append({
                "name": row[0],
                "messages": row[1],
                "dialogs": row[2]
            })
        return results
    
    def get_user_tournament_rank(self, user_id: int, tournament_id: str = None) -> Optional[dict]:
        """Получает позицию пользователя в турнире"""
        if tournament_id is None:
            tournament_id = self.get_current_tournament_id()
        
        cursor = self.conn.cursor()
        
        # Получаем статистику пользователя
        cursor.execute('''
            SELECT anon_name, messages, dialogs
            FROM tournaments
            WHERE tournament_id = ? AND user_id = ?
        ''', (tournament_id, user_id))
        user_stats = cursor.fetchone()
        
        if not user_stats:
            return None
        
        # Получаем ранг пользователя
        cursor.execute('''
            SELECT COUNT(*) + 1
            FROM tournaments
            WHERE tournament_id = ? AND messages > ?
        ''', (tournament_id, user_stats[1]))
        rank = cursor.fetchone()[0]
        
        # Получаем топ-10 для сравнения
        top10 = self.get_tournament_leaderboard(tournament_id, 10)
        
        return {
            "name": user_stats[0],
            "messages": user_stats[1],
            "dialogs": user_stats[2],
            "rank": rank,
            "top10": top10
        }
    
    # ================= НОВЫЕ МЕТОДЫ ДЛЯ ВОЗВРАТА СОБЕСЕДНИКА =================
    
    def add_dialog_rating(self, user_id: int, partner_id: int, rating: int):
        """Добавляет оценку диалога (1 = понравилось, 0 = не понравилось)"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO dialog_ratings (user_id, partner_id, rating)
            VALUES (?, ?, ?)
        ''', (user_id, partner_id, rating))
        self.conn.commit()
    
    def get_liked_partners(self, user_id: int) -> List[dict]:
        """Получает список понравившихся собеседников для VIP"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT partner_id, rating, dialog_date
            FROM dialog_ratings
            WHERE user_id = ? AND rating = 1
            ORDER BY dialog_date DESC
            LIMIT 50
        ''', (user_id,))
        
        results = []
        for row in cursor.fetchall():
            partner_id = row[0]
            
            # Получаем анонимное имя для турнира (или генерируем временное)
            tournament_id = self.get_current_tournament_id()
            cursor.execute('''
                SELECT anon_name FROM tournaments
                WHERE tournament_id = ? AND user_id = ?
            ''', (tournament_id, partner_id))
            anon_result = cursor.fetchone()
            
            if anon_result:
                anon_name = anon_result[0]
            else:
                anon_name = generate_tournament_name(partner_id, "temp")
            
            # Проверяем, не отправляли ли уже приглашение
            cursor.execute('''
                SELECT invite_sent, invite_expires FROM dialog_ratings
                WHERE user_id = ? AND partner_id = ? AND rating = 1
                ORDER BY dialog_date DESC LIMIT 1
            ''', (user_id, partner_id))
            invite_data = cursor.fetchone()
            
            can_invite = True
            if invite_data and invite_data[0] == 1:
                if invite_data[1] and datetime.strptime(invite_data[1], '%Y-%m-%d %H:%M:%S.%f') > datetime.now():
                    can_invite = False
            
            results.append({
                "partner_id": partner_id,
                "anon_name": anon_name,
                "dialog_date": row[2],
                "can_invite": can_invite
            })
        
        return results
    
    def send_invite(self, user_id: int, partner_id: int) -> bool:
        """Отправляет приглашение собеседнику (только 1 раз)"""
        cursor = self.conn.cursor()
        
        # Проверяем, можно ли отправить приглашение
        cursor.execute('''
            SELECT invite_sent, invite_expires FROM dialog_ratings
            WHERE user_id = ? AND partner_id = ? AND rating = 1
            ORDER BY dialog_date DESC LIMIT 1
        ''', (user_id, partner_id))
        invite_data = cursor.fetchone()
        
        if invite_data and invite_data[0] == 1:
            if invite_data[1] and datetime.strptime(invite_data[1], '%Y-%m-%d %H:%M:%S.%f') > datetime.now():
                return False
        
        # Отмечаем, что приглашение отправлено
        expires_at = datetime.now() + timedelta(hours=24)
        cursor.execute('''
            UPDATE dialog_ratings
            SET invite_sent = 1, invite_expires = ?
            WHERE user_id = ? AND partner_id = ? AND rating = 1
            ORDER BY dialog_date DESC LIMIT 1
        ''', (expires_at, user_id, partner_id))
        
        self.conn.commit()
        return True
    
    # ================= НОВЫЕ МЕТОДЫ ДЛЯ КАСТОМИЗАЦИИ VIP =================
    
    def set_vip_color(self, user_id: int, color: str):
        """Устанавливает цвет ника для VIP"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO vip_customization (user_id, nickname_color)
            VALUES (?, ?)
        ''', (user_id, color))
        self.conn.commit()
    
    def get_vip_color(self, user_id: int) -> str:
        """Получает цвет ника для VIP"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT nickname_color FROM vip_customization WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        return result[0] if result else "⚪️"
    
    def set_vip_frame(self, user_id: int, frame: str):
        """Устанавливает рамку для сообщений"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO vip_customization (user_id, message_frame)
            VALUES (?, ?)
        ''', (user_id, frame))
        self.conn.commit()
    
    def get_vip_frame(self, user_id: int) -> str:
        """Получает рамку для сообщений"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT message_frame FROM vip_customization WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        return result[0] if result else "none"
    
    def ban_user(self, user_id: int, username: str, full_name: str, 
                 reason: str, admin_id: int = None, duration_hours: int = 1):
        cursor = self.conn.cursor()
        unbanned_at = datetime.now() + timedelta(hours=duration_hours)
        
        cursor.execute('''
            INSERT OR REPLACE INTO bans 
            (user_id, username, full_name, reason, admin_id, unbanned_at, duration_hours)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (user_id, username, full_name, reason, admin_id, unbanned_at, duration_hours))
        self.conn.commit()
        logger.info(f"Пользователь {user_id} забанен по причине: {reason}")
    
    def unban_user(self, user_id: int):
        cursor = self.conn.cursor()
        cursor.execute('DELETE FROM bans WHERE user_id = ?', (user_id,))
        self.conn.commit()
        return cursor.rowcount > 0
    
    def is_banned(self, user_id: int) -> bool:
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT unbanned_at FROM bans 
            WHERE user_id = ? AND unbanned_at > CURRENT_TIMESTAMP
        ''', (user_id,))
        return cursor.fetchone() is not None
    
    def get_ban_info(self, user_id: int) -> Optional[tuple]:
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT user_id, username, reason, admin_id, banned_at, unbanned_at, duration_hours
            FROM bans WHERE user_id = ?
        ''', (user_id,))
        return cursor.fetchone()
    
    def add_violation(self, user_id: int, username: str, full_name: str, 
                     violation_type: str, message_text: str = None):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO violations (user_id, username, full_name, violation_type, message_text)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, username, full_name, violation_type, message_text))
        self.conn.commit()
        logger.info(f"Добавлено нарушение для пользователя {user_id}: {violation_type}")
    
    def get_violations(self, user_id: int, limit: int = 10):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT violation_type, message_text, created_at
            FROM violations 
            WHERE user_id = ?
            ORDER BY created_at DESC
            LIMIT ?
        ''', (user_id, limit))
        return cursor.fetchall()
    
    def get_all_banned_users(self):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT user_id, username, reason, banned_at, unbanned_at, duration_hours
            FROM bans 
            WHERE unbanned_at IS NULL OR unbanned_at > CURRENT_TIMESTAMP
            ORDER BY banned_at DESC
        ''')
        return cursor.fetchall()
    
    def get_payment_stats(self):
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT 
                payment_method,
                COUNT(*) as count,
                SUM(amount) as total,
                currency
            FROM payments 
            WHERE status = 'completed'
            GROUP BY payment_method, currency
        ''')
        
        stats = {"methods": {}, "total_completed": 0, "total_pending": 0}
        
        for row in cursor.fetchall():
            method, count, total, currency = row
            stats["methods"][method] = {
                "count": count,
                "total": total,
                "currency": currency
            }
            stats["total_completed"] += count
        
        cursor.execute('SELECT COUNT(*) FROM payments WHERE status = "pending"')
        result = cursor.fetchone()
        stats["total_pending"] = result[0] if result else 0
        
        return stats
    
    def get_user_payments(self, user_id: int):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT id, days, amount, currency, payment_method, status, created_at
            FROM payments 
            WHERE user_id = ?
            ORDER BY created_at DESC
            LIMIT 10
        ''', (user_id,))
        return cursor.fetchall()

db = Database()

# ================= FSM СОСТОЯНИЯ =================
class RegistrationStates(StatesGroup):
    waiting_gender = State()
    waiting_country = State()
    waiting_city = State()

class ProfileStates(StatesGroup):
    waiting_gender_change = State()
    waiting_country_change = State()
    waiting_city_change = State()
    waiting_birthday = State()

class PaymentStates(StatesGroup):
    waiting_yookassa_confirmation = State()

class AdminStates(StatesGroup):
    waiting_ban_user = State()
    waiting_ban_reason = State()
    waiting_unban_user = State()
    waiting_broadcast_message = State()
    waiting_give_forever_vip = State()

class VIPFilterStates(StatesGroup):
    waiting_gender_filter = State()
    waiting_country_filter = State()
    waiting_city_filter = State()
    waiting_vip_only = State()

# НОВЫЕ СОСТОЯНИЯ
class VIPCustomizationStates(StatesGroup):
    waiting_color = State()
    waiting_frame = State()

class DialogRatingStates(StatesGroup):
    waiting_rating = State()

# ================= ИНИЦИАЛИЗАЦИЯ =================
storage = MemoryStorage()
bot = Bot(token=TOKEN)
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

users: Dict[int, Dict[str, Any]] = {}
waiting: List[int] = []
pairs: Dict[int, int] = {}
# НОВАЯ ОЧЕРЕДЬ ДЛЯ VIP (приоритетный поиск)
vip_waiting: List[int] = []

# ================= СПИСКИ ГЕНДЕРОВ И СТРАН =================
GENDERS = {
    "male": "👨 Мужской",
    "female": "👩 Женский", 
    "not_specified": "🙈 Не указывать"
}

GENDERS_REVERSE = {
    "👨 Мужской": "male",
    "👩 Женский": "female", 
    "🙈 Не указывать": "not_specified"
}

SUPPORTED_COUNTRIES = {"ru", "ua", "by", "kz", "az", "am", "kg", "tj", "uz", "md", "ge", "tm"}

COUNTRIES = [
    {"code": "ru", "name": "🇷🇺 Россия"},
    {"code": "ua", "name": "🇺🇦 Украина"},
    {"code": "by", "name": "🇧🇾 Беларусь"},
    {"code": "kz", "name": "🇰🇿 Казахстан"},
    {"code": "az", "name": "🇦🇿 Азербайджан"},
    {"code": "am", "name": "🇦🇲 Армения"},
    {"code": "kg", "name": "🇰🇬 Кыргызстан"},
    {"code": "tj", "name": "🇹🇯 Таджикистан"},
    {"code": "uz", "name": "🇺🇿 Узбекистан"},
    {"code": "md", "name": "🇲🇩 Молдова"},
    {"code": "ge", "name": "🇬🇪 Грузия"},
    {"code": "tm", "name": "🇹🇲 Туркменистан"},
    {"code": "not_specified", "name": "🙈 Не указывать"},
    {"code": "other", "name": "🌍 Другая страна"}
]

COUNTRY_MAP = {
    "🇷🇺 Россия": "ru",
    "🇺🇦 Украина": "ua",
    "🇧🇾 Беларусь": "by",
    "🇰🇿 Казахстан": "kz",
    "🇦🇿 Азербайджан": "az",
    "🇦🇲 Армения": "am",
    "🇰🇬 Кыргызстан": "kg",
    "🇹🇯 Таджикистан": "tj",
    "🇺🇿 Узбекистан": "uz",
    "🇲🇩 Молдова": "md",
    "🇬🇪 Грузия": "ge",
    "🇹🇲 Туркменистан": "tm",
    "🙈 Не указывать": "not_specified",
    "🌍 Другая страна": "other"
}

PAYMENT_COUNTRIES = {
    "RU": "🇷🇺 Россия",
    "UA": "🇺🇦 Украина", 
    "BY": "🇧🇾 Беларусь",
    "KZ": "🇰🇿 Казахстан",
    "UZ": "🇺🇿 Узбекистан",
    "AZ": "🇦🇿 Азербайджан",
    "AM": "🇦🇲 Армения",
    "KG": "🇰🇬 Кыргызстан",
    "TJ": "🇹🇯 Таджикистан",
    "MD": "🇲🇩 Молдова",
    "GE": "🇬🇪 Грузия",
    "TM": "🇹🇲 Туркменистан",
    "INT": "🌍 Другие страны"
}

CITIES = {
    "ru": [
        {"code": "moscow", "name": "🇷🇺 Москва"},
        {"code": "spb", "name": "🇷🇺 Санкт-Петербург"},
        {"code": "ekb", "name": "🇷🇺 Екатеринбург"},
        {"code": "kazan", "name": "🇷🇺 Казань"},
        {"code": "nsk", "name": "🇷🇺 Новосибирск"},
        {"code": "sochi", "name": "🇷🇺 Сочи"},
        {"code": "other", "name": "🇷🇺 Другой город"}
    ],
    "ua": [
        {"code": "kyiv", "name": "🇺🇦 Киев"},
        {"code": "kharkiv", "name": "🇺🇦 Харьков"},
        {"code": "odesa", "name": "🇺🇦 Одесса"},
        {"code": "lviv", "name": "🇺🇦 Львов"},
        {"code": "other", "name": "🇺🇦 Другой город"}
    ],
    "by": [
        {"code": "minsk", "name": "🇧🇾 Минск"},
        {"code": "gomel", "name": "🇧🇾 Гомель"},
        {"code": "brest", "name": "🇧🇾 Брест"},
        {"code": "other", "name": "🇧🇾 Другой город"}
    ],
    "kz": [
        {"code": "almaty", "name": "🇰🇿 Алматы"},
        {"code": "nur_sultan", "name": "🇰🇿 Нур-Султан"},
        {"code": "shymkent", "name": "🇰🇿 Шымкент"},
        {"code": "other", "name": "🇰🇿 Другой город"}
    ],
    "az": [
        {"code": "baku", "name": "🇦🇿 Баку"},
        {"code": "ganja", "name": "🇦🇿 Гянджа"},
        {"code": "other", "name": "🇦🇿 Другой город"}
    ],
    "am": [
        {"code": "yerevan", "name": "🇦🇲 Ереван"},
        {"code": "gyumri", "name": "🇦🇲 Гюмри"},
        {"code": "other", "name": "🇦🇲 Другой город"}
    ],
    "kg": [
        {"code": "bishkek", "name": "🇰🇬 Бишкек"},
        {"code": "osh", "name": "🇰🇬 Ош"},
        {"code": "other", "name": "🇰🇬 Другой город"}
    ],
    "tj": [
        {"code": "dushanbe", "name": "🇹🇯 Душанбе"},
        {"code": "khujand", "name": "🇹🇯 Худжанд"},
        {"code": "other", "name": "🇹🇯 Другой город"}
    ],
    "uz": [
        {"code": "tashkent", "name": "🇺🇿 Ташкент"},
        {"code": "samarkand", "name": "🇺🇿 Самарканд"},
        {"code": "other", "name": "🇺🇿 Другой город"}
    ],
    "md": [
        {"code": "chisinau", "name": "🇲🇩 Кишинев"},
        {"code": "tiraspol", "name": "🇲🇩 Тирасполь"},
        {"code": "other", "name": "🇲🇩 Другой город"}
    ],
    "ge": [
        {"code": "tbilisi", "name": "🇬🇪 Тбилиси"},
        {"code": "batumi", "name": "🇬🇪 Батуми"},
        {"code": "other", "name": "🇬🇪 Другой город"}
    ],
    "tm": [
        {"code": "ashgabat", "name": "🇹🇲 Ашхабад"},
        {"code": "turkmenabat", "name": "🇹🇲 Туркменабад"},
        {"code": "other", "name": "🇹🇲 Другой город"}
    ]
}

CITY_MAP = {}
for country_code, cities in CITIES.items():
    for city in cities:
        full_name = city["name"]
        CITY_MAP[full_name] = {"country": country_code, "city": city["code"], "display": full_name}
        simple_name = full_name.split(' ', 1)[1] if ' ' in full_name else full_name
        CITY_MAP[simple_name] = {"country": country_code, "city": city["code"], "display": full_name}

def detect_user_country(user) -> str:
    lang = user.language_code or "en"
    
    country_map = {
        "ru": "RU", "uk": "UA", "be": "BY", "kk": "KZ", "uz": "UZ",
        "az": "AZ", "hy": "AM", "ky": "KG", "tg": "TJ", "ro": "MD",
        "ka": "GE", "tk": "TM", "en": "INT", "de": "INT", "tr": "INT",
        "es": "INT", "fr": "INT", "it": "INT",
    }
    
    return country_map.get(lang, "INT")

def is_cis_country(country_code: str) -> bool:
    return country_code in CIS_COUNTRIES

def load_data() -> None:
    global users, waiting, pairs, vip_waiting
    if DATA_FILE.exists():
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                users = {int(k): v for k, v in data.get("users", {}).items()}
                waiting = data.get("waiting", [])
                vip_waiting = data.get("vip_waiting", [])
                pairs = {int(k): int(v) for k, v in data.get("pairs", {}).items()}
            logger.info(f"✅ Данные загружены: {len(users)} пользователей")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки данных: {e}")
            users, waiting, pairs, vip_waiting = {}, [], [], {}
    else:
        logger.info("ℹ️ Файл данных не найден, создаем новый...")

def save_data() -> None:
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "users": users,
                "waiting": waiting,
                "vip_waiting": vip_waiting,
                "pairs": pairs
            }, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения данных: {e}")

def is_registered(uid: int) -> bool:
    return uid in users and "gender" in users[uid] and "country" in users[uid] and "city" in users[uid]

def get_vip_level_emoji(uid: int) -> str:
    """Получить эмодзи уровня VIP для пользователя"""
    if uid not in users or "vip_until" not in users[uid]:
        return ""
    
    vip_until = users[uid]["vip_until"]
    
    # Если вечный VIP
    if vip_until == float('inf') or (isinstance(vip_until, float) and vip_until > time.time() + 999999 * 86400):
        return VIP_LEVELS["forever"]["emoji"]
    
    # Если VIP активен
    if time.time() < vip_until:
        total_days = users[uid].get("vip_total_days", 0)
        level = get_vip_level(total_days)
        return level["emoji"]
    
    return ""

def is_vip(uid: int) -> bool:
    if uid not in users or "vip_until" not in users[uid]:
        return False
    if users[uid]["vip_until"] is None or users[uid]["vip_until"] == float('inf'):
        return True
    if time.time() > users[uid]["vip_until"]:
        users[uid].pop("vip_until", None)
        users[uid].pop("vip_level", None)
        if "search_prefs" in users[uid]:
            users[uid].pop("search_prefs", None)
        if "vip_only" in users[uid]:
            users[uid].pop("vip_only", None)
        save_data()
        return False
    return True

def give_vip(uid: int, seconds: int, level: str = "bronze") -> float:
    """Выдача VIP на указанное количество секунд"""
    users.setdefault(uid, {})
    
    current = users[uid].get("vip_until", time.time())
    current_total = users[uid].get("vip_total_days", 0)
    
    if current == float('inf'):
        return float('inf')
    
    days_to_add = seconds // 86400
    new_total = current_total + days_to_add
    
    if current > time.time():
        users[uid]["vip_until"] = current + seconds
    else:
        users[uid]["vip_until"] = time.time() + seconds
    
    users[uid]["vip_total_days"] = new_total
    users[uid]["vip_level"] = level
    
    save_data()
    
    days = seconds // 86400
    db.add_vip_activation(uid, days, level)
    
    return users[uid]["vip_until"]

# ================= НОВАЯ ФУНКЦИЯ ДЛЯ ОТБИРАНИЯ VIP =================
def remove_vip(uid: int) -> bool:
    """Отбирает VIP статус у пользователя"""
    if uid not in users or "vip_until" not in users[uid]:
        return False
    
    # Удаляем VIP данные
    if "vip_until" in users[uid]:
        del users[uid]["vip_until"]
    if "vip_total_days" in users[uid]:
        del users[uid]["vip_total_days"]
    if "vip_level" in users[uid]:
        del users[uid]["vip_level"]
    if "search_prefs" in users[uid]:
        del users[uid]["search_prefs"]
    if "vip_only" in users[uid]:
        del users[uid]["vip_only"]
    
    save_data()
    logger.info(f"VIP статус отобран у пользователя {uid}")
    return True

def give_forever_vip(uid: int) -> None:
    """Выдача вечного VIP"""
    users.setdefault(uid, {})
    users[uid]["vip_until"] = float('inf')
    users[uid]["vip_total_days"] = 999999
    users[uid]["vip_level"] = "forever"
    save_data()
    db.add_vip_activation(uid, 999999, "forever")

def get_country_name(country_code: str) -> str:
    for country in COUNTRIES:
        if country["code"] == country_code:
            return country["name"]
    return country_code

def get_city_kb(country_code: str) -> Optional[ReplyKeyboardMarkup]:
    if country_code not in CITIES or country_code == "other" or country_code == "not_specified":
        return None
    
    cities = CITIES.get(country_code, [])
    
    keyboard = []
    row = []
    
    for i, city in enumerate(cities):
        row.append(KeyboardButton(text=city["name"]))
        if len(row) == 2 or i == len(cities) - 1:
            keyboard.append(row)
            row = []
    
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=True)

def get_user_profile(uid: int) -> str:
    if uid not in users:
        return "❌ Пользователь не найден"
    
    user = users[uid]
    
    gender_code = user.get("gender", "not_specified")
    gender_text = GENDERS.get(gender_code, gender_code)
    
    country_code = user.get("country", "not_specified")
    if country_code == "not_specified":
        country_text = "🙈 Не указана"
    elif country_code == "other":
        country_text = "🌍 Другая страна"
    else:
        country_text = get_country_name(country_code)
    
    city_text = user.get("city_display", "Не указан")
    
    profile = "👤 <b>Ваш профиль:</b>\n"
    profile += f"• <b>Пол:</b> {gender_text}\n"
    profile += f"• <b>Страна:</b> {country_text}\n"
    profile += f"• <b>Город:</b> {city_text}\n"
    
    if "birthday" in user:
        profile += f"• <b>День рождения:</b> {user['birthday']}\n"
    
    if is_vip(uid):
        vip_emoji = get_vip_level_emoji(uid)
        if users[uid].get("vip_until") == float('inf'):
            profile += f"• <b>VIP:</b> {vip_emoji} КОРОЛЕВСКИЙ VIP (навсегда)\n"
        else:
            until = time.strftime("%d.%m.%Y %H:%M", time.localtime(user["vip_until"]))
            level = get_vip_level(user.get("vip_total_days", 0))
            profile += f"• <b>VIP:</b> {vip_emoji} {level['name']} (до {until})\n"
        
        if user.get("vip_only"):
            profile += f"• <b>Режим:</b> 💫 VIP-чат включен\n"
        
        # НОВОЕ: отображение кастомизации
        vip_color = db.get_vip_color(uid)
        if vip_color != "⚪️":
            profile += f"• <b>Цвет ника:</b> {vip_color}\n"
        
        prefs = user.get("search_prefs", {})
        if prefs:
            profile += "\n<b>⚙️ Ваши фильтры поиска:</b>\n"
            if "gender" in prefs:
                pref_gender = GENDERS.get(prefs["gender"], prefs["gender"])
                profile += f"• Пол: {pref_gender}\n"
            if "country" in prefs:
                if prefs["country"] == "not_specified":
                    pref_country = "Не важно"
                else:
                    pref_country = get_country_name(prefs["country"])
                profile += f"• Страна: {pref_country}\n"
            if "city" in prefs:
                city_display = prefs.get("city_display", prefs["city"])
                profile += f"• Город: {city_display}\n"
    else:
        profile += f"• <b>VIP:</b> ❌\n"
    
    stats = db.get_stats(uid)
    rank_data = get_rank(stats["messages"])
    
    profile += f"\n📊 <b>Ваша статистика:</b>\n"
    profile += f"• {rank_data['current']['emoji']} Ранг: {rank_data['current']['name']}\n"
    profile += f"• 💬 Сообщений: {stats['messages']}\n"
    profile += f"• 👥 Собеседников: {stats['dialogs']}\n"
    
    if rank_data['next']:
        remaining = rank_data['next']['min_messages'] - stats['messages']
        profile += f"• До следующего ранга: {remaining} сообщ.\n"
    
    badges = db.get_user_badges(uid)
    if badges:
        profile += f"\n🏅 <b>Значки ({len(badges)}):</b> "
        profile += " ".join([b['emoji'] for b in badges[:5]])
        if len(badges) > 5:
            profile += f" +{len(badges)-5}"
    
    return profile

async def notify_admin(message: str, parse_mode: str = ParseMode.HTML) -> None:
    if not ADMIN_IDS:
        return
    
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, message, parse_mode=parse_mode)
        except Exception as e:
            logger.error(f"Не удалось отправить уведомление администратору {admin_id}: {e}")

# ================= ФУНКЦИИ ДЛЯ КЛАВИАТУР =================
def get_vip_menu_kb(is_vip_user: bool, country: str = "RU") -> InlineKeyboardMarkup:
    if is_vip_user:
        buttons = [
            [InlineKeyboardButton(text="👤 Фильтр по полу", callback_data="vip_filter_gender")],
            [InlineKeyboardButton(text="🌍 Фильтр по стране", callback_data="vip_filter_country")],
            [InlineKeyboardButton(text="🌆 Фильтр по городу", callback_data="vip_filter_city")],
        ]
        
        # Кнопка VIP-чата (только для VIP)
        buttons.append([InlineKeyboardButton(text="💫 VIP-чат (только VIP)", callback_data="vip_toggle_vip_only")])
        
        # НОВЫЕ КНОПКИ
        buttons.append([InlineKeyboardButton(text="🎨 Кастомизация", callback_data="vip_customization")])
        buttons.append([InlineKeyboardButton(text="💫 Понравившиеся", callback_data="vip_liked_partners")])
        
        buttons.extend([
            [InlineKeyboardButton(text="📋 Мои фильтры", callback_data="vip_show_filters")],
            [InlineKeyboardButton(text="♻️ Сбросить фильтры", callback_data="vip_reset_filters")],
            [InlineKeyboardButton(text="💳 История платежей", callback_data="payment_history")],
            [InlineKeyboardButton(text="👑 Статистика VIP", callback_data="vip_stats")],
            [InlineKeyboardButton(text="🔙 Главное меню", callback_data="main_menu_back")]
        ])
        
        return InlineKeyboardMarkup(inline_keyboard=buttons)
    else:
        buttons = []
        
        user_cis = is_cis_country(country)
        
        if user_cis and YOOKASSA_AVAILABLE and YOOKASSA_SHOP_ID and YOOKASSA_SECRET_KEY:
            for days, plan in VIP_PLANS_RUB.items():
                if days <= 365:  # Не показываем вечный в продаже
                    buttons.append([
                        InlineKeyboardButton(
                            text=f"{plan['title']} — {plan['price']}₽ ({plan['level']})", 
                            callback_data=f"buy_vip_{days}"
                        )
                    ])
            buttons.append([
                InlineKeyboardButton(text="⭐️ Stars (для других стран)", callback_data="vip_stars_method")
            ])
        else:
            for days, plan in VIP_PLANS_STARS.items():
                if days <= 365:
                    buttons.append([
                        InlineKeyboardButton(
                            text=f"{plan['title']} — {plan['stars']}⭐️ ({plan['level']})", 
                            callback_data=f"buy_vip_stars_{days}"
                        )
                    ])
            if YOOKASSA_AVAILABLE and YOOKASSA_SHOP_ID and YOOKASSA_SECRET_KEY:
                buttons.append([
                    InlineKeyboardButton(text="💳 ЮKassa (для СНГ)", callback_data="vip_yookassa_method")
                ])
        
        buttons.append([InlineKeyboardButton(text="🔙 Главное меню", callback_data="main_menu_back")])
        return InlineKeyboardMarkup(inline_keyboard=buttons)

# ================= ФУНКЦИИ ПРОВЕРКИ =================
async def check_ban(user_id: int) -> bool:
    if user_id in ADMIN_IDS:
        db.unban_user(user_id)
        return False
    return db.is_banned(user_id)

async def check_violation(message: Message) -> bool:
    user_id = message.from_user.id
    
    if user_id in ADMIN_IDS:
        return False
    
    text = message.text or message.caption or ""
    
    level, reason, should_ban = check_content(text)
    
    if should_ban:
        return await handle_violation(user_id, message, level, reason)
    
    if message.text and "@" in text and len(text.strip()) < 30:
        db.add_violation(
            user_id=user_id,
            username=message.from_user.username or "",
            full_name=message.from_user.full_name,
            violation_type="contact_share",
            message_text=text[:100]
        )
        
        db.ban_user(
            user_id=user_id,
            username=message.from_user.username or "",
            full_name=message.from_user.full_name,
            reason="Попытка поделиться контактами (@)",
            admin_id=None,
            duration_hours=1
        )
        
        try:
            await message.answer(
                "🚫 <b>Вы забанены на 1 час!</b>\n\n"
                "Причина: попытка поделиться контактами (@)\n"
                "Через 1 час вы сможете продолжить пользоваться ботом.",
                parse_mode=ParseMode.HTML
            )
        except:
            pass
        
        await notify_admin(
            f"🚨 <b>Автоматический бан</b>\n\n"
            f"👤 Пользователь: {user_id}\n"
            f"📛 Имя: {message.from_user.full_name}\n"
            f"📌 Причина: Попытка поделиться контактами (@)",
            parse_mode=ParseMode.HTML
        )
        
        logger.info(f"Пользователь {user_id} забанен автоматически за попытку поделиться контактами")
        return True
    
    return False

# ================= ПРАВИЛА ЧАТА =================
RULES_TEXT = """
📜 <b>ПРАВИЛА ЧАТА</b> 📜

1️⃣ <b>Уважение к собеседникам</b>
   • Запрещены оскорбления, унижения, травля
   • Относитесь к другим так, как хотите, чтобы относились к вам

2️⃣ <b>Запрещенный контент</b>
   • ❌ Порнография, эротика, интим
   • ❌ Пропаганда насилия, экстремизма
   • ❌ Разжигание межнациональной розни
   • ❌ Детская порнография (немедленный бан)

3️⃣ <b>Личная информация</b>
   • Запрещено делиться контактами (@username, телефон, адрес)
   • Не просите личные данные у собеседников

4️⃣ <b>Медиафайлы</b>
   • В диалогах можно отправлять только ТЕКСТ
   • Фото, видео, стикеры, гифки запрещены

5️⃣ <b>Спам и реклама</b>
   • Запрещена любая реклама
   • Запрещен спам и флуд

⚠️ <b>СИСТЕМА НАКАЗАНИЙ:</b>
• 1-е нарушение: предупреждение
• 2-е нарушение: бан на 24 часа
• 3-е нарушение: бан на 7 дней
• Критические нарушения: бан на 7 дней сразу

🆘 <b>Поддержка:</b> {support}

<i>Администрация оставляет за собой право блокировать пользователей без объяснения причин в случае грубых нарушений.</i>
""".format(support=SUPPORT_USERNAME)

@router.message(F.text == "📜 Правила")
async def rules_cmd(m: Message) -> None:
    await m.answer(RULES_TEXT, parse_mode=ParseMode.HTML)

# ================= ПРЕДОПРЕДЕЛЕННЫЕ КЛАВИАТУРЫ =================
main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🔍 Найти собеседника"), KeyboardButton(text="⏹️ Остановить поиск")],
        [KeyboardButton(text="⭐️ VIP"), KeyboardButton(text="👤 Профиль")],
        [KeyboardButton(text="🏅 Мои значки"), KeyboardButton(text="📋 Все достижения")],
        [KeyboardButton(text="📜 Правила")]
    ],
    resize_keyboard=True
)

main_kb_with_stop = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🚫 Завершить диалог")],
        [KeyboardButton(text="⭐️ VIP"), KeyboardButton(text="👤 Профиль")],
        [KeyboardButton(text="🏅 Мои значки"), KeyboardButton(text="📋 Все достижения")],
        [KeyboardButton(text="📜 Правила")]
    ],
    resize_keyboard=True
)

gender_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="👨 Мужской")],
        [KeyboardButton(text="👩 Женский")],
        [KeyboardButton(text="🙈 Не указывать")]
    ],
    resize_keyboard=True,
    one_time_keyboard=True
)

country_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🇷🇺 Россия"), KeyboardButton(text="🇺🇦 Украина"), KeyboardButton(text="🇧🇾 Беларусь")],
        [KeyboardButton(text="🇰🇿 Казахстан"), KeyboardButton(text="🇦🇿 Азербайджан"), KeyboardButton(text="🇦🇲 Армения")],
        [KeyboardButton(text="🇰🇬 Кыргызстан"), KeyboardButton(text="🇹🇯 Таджикистан"), KeyboardButton(text="🇺🇿 Узбекистан")],
        [KeyboardButton(text="🇲🇩 Молдова"), KeyboardButton(text="🇬🇪 Грузия"), KeyboardButton(text="🇹🇲 Туркменистан")],
        [KeyboardButton(text="🙈 Не указывать"), KeyboardButton(text="🌍 Другая страна")]
    ],
    resize_keyboard=True,
    one_time_keyboard=True
)

# ================= МИДЛВАРЬ ДЛЯ ПРОВЕРКИ БАНОВ =================
@router.message.middleware()
async def ban_check_middleware(handler, event, data):
    user_id = event.from_user.id
    
    if user_id in ADMIN_IDS:
        return await handler(event, data)
    
    if await check_ban(user_id):
        return
    
    if isinstance(event, Message) and event.text:
        if await check_violation(event):
            return
    
    return await handler(event, data)

# ================= ТЕСТОВАЯ КОМАНДА =================
@router.message(Command("test"))
async def test_cmd(m: Message) -> None:
    await m.answer("✅ Бот работает! Команды загружаются.")

# ================= ОБРАБОТЧИК КОМАНДЫ /START =================
@router.message(Command("start"))
async def start_cmd(m: Message, state: FSMContext) -> None:
    uid = m.from_user.id
    
    if uid in ADMIN_IDS:
        if not is_registered(uid):
            users.setdefault(uid, {})
            users[uid]["gender"] = "not_specified"
            users[uid]["country"] = "not_specified"
            users[uid]["city"] = "other"
            users[uid]["city_display"] = "🌍 Другой город"
            save_data()
        
        if uid in pairs:
            await m.answer("👑 <b>Панель администратора</b>\n\nВы в диалоге.", reply_markup=main_kb_with_stop, parse_mode=ParseMode.HTML)
        else:
            await m.answer("👑 <b>Панель администратора</b>", reply_markup=main_kb, parse_mode=ParseMode.HTML)
        return
    
    if await check_ban(uid):
        return
    
    if is_registered(uid):
        await state.clear()
        
        if uid in pairs:
            await m.answer("С возвращением! Вы в диалоге.", reply_markup=main_kb_with_stop)
        else:
            await m.answer("С возвращением!", reply_markup=main_kb)
        return
    
    await m.answer(
        "👋 Привет! Выберите ваш пол:",
        reply_markup=gender_kb
    )
    await state.set_state(RegistrationStates.waiting_gender)

# ================= КОМАНДА /MYID =================
@router.message(Command("myid"))
async def my_id_cmd(m: Message) -> None:
    uid = m.from_user.id
    await m.answer(f"🆔 <b>Ваш ID:</b> <code>{uid}</code>", parse_mode=ParseMode.HTML)

# ================= АДМИН ПАНЕЛЬ И КОМАНДЫ =================
@router.message(Command("admin"))
async def admin_cmd(m: Message) -> None:
    uid = m.from_user.id
    
    if uid not in ADMIN_IDS:
        logger.warning(f"Пользователь {uid} попытался получить доступ к админ-панели")
        await m.answer("⛔️ У вас нет доступа к этой команде.")
        return
    
    admin_text = (
        "👑 <b>Панель администратора</b>\n\n"
        "Доступные команды:\n"
        "• /stats - статистика бота\n"
        "• /ban [ID] [причина] - забанить пользователя\n"
        "• /unban [ID] - разбанить пользователя\n"
        "• /bans - список забаненных\n"
        "• /givevip [ID] [дни] - выдать VIP статус\n"
        "• /giveforever [ID] - выдать КОРОЛЕВСКИЙ VIP навсегда\n"
        "• /removevip [ID] - отобрать VIP статус\n"
        "• /vipusers - список всех VIP пользователей\n"
        "• /broadcast - рассылка\n"
        "• /violations [ID] - история нарушений\n\n"
        f"<b>Всего пользователей:</b> {len(users)}\n"
        f"<b>В ожидании:</b> {len(waiting)}\n"
        f"<b>VIP в ожидании:</b> {len(vip_waiting)}\n"
        f"<b>Активных диалогов:</b> {len(pairs)//2}"
    )
    
    await m.answer(admin_text, parse_mode=ParseMode.HTML)
    logger.info(f"Админ-панель открыта для {uid}")

@router.message(Command("stats"))
async def stats_cmd(m: Message) -> None:
    uid = m.from_user.id
    
    if uid not in ADMIN_IDS:
        return
    
    stats = db.get_payment_stats()
    vip_stats = db.get_vip_stats()
    
    cursor = db.conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM users_stats")
    total_users_stats = cursor.fetchone()[0] or 0
    
    cursor.execute("SELECT SUM(messages) FROM users_stats")
    total_messages = cursor.fetchone()[0] or 0
    
    cursor.execute("SELECT COUNT(*) FROM bans WHERE unbanned_at > CURRENT_TIMESTAMP OR unbanned_at IS NULL")
    active_bans = cursor.fetchone()[0] or 0
    
    # Статистика турниров
    tournament_id = db.get_current_tournament_id()
    cursor.execute("SELECT COUNT(*) FROM tournaments WHERE tournament_id = ?", (tournament_id,))
    tournament_participants = cursor.fetchone()[0] or 0
    
    stats_text = (
        "📊 <b>Статистика бота</b>\n\n"
        f"<b>Всего пользователей (JSON):</b> {len(users)}\n"
        f"<b>Всего пользователей (БД):</b> {total_users_stats}\n"
        f"<b>Всего сообщений:</b> {total_messages}\n"
        f"<b>В ожидании:</b> {len(waiting)}\n"
        f"<b>VIP в ожидании:</b> {len(vip_waiting)}\n"
        f"<b>Активных диалогов:</b> {len(pairs)//2}\n"
        f"<b>Активных банов:</b> {active_bans}\n\n"
        f"<b>VIP пользователей:</b> {vip_stats['total']}\n"
        f"<b>VIP онлайн:</b> {vip_stats['online']}\n\n"
        f"<b>Участников турнира:</b> {tournament_participants}\n\n"
        f"<b>Завершенных платежей:</b> {stats['total_completed']}\n"
        f"<b>Ожидающих платежей:</b> {stats['total_pending']}\n\n"
    )
    
    if stats['methods']:
        stats_text += "<b>По методам оплаты:</b>\n"
        for method, data in stats['methods'].items():
            method_name = "Stars" if method == "stars" else "ЮKassa" if method == "yookassa" else method
            stats_text += f"• {method_name}: {data['count']} на сумму {data['total']} {data['currency']}\n"
    
    await m.answer(stats_text, parse_mode=ParseMode.HTML)

@router.message(Command("bans"))
async def bans_cmd(m: Message) -> None:
    uid = m.from_user.id
    
    if uid not in ADMIN_IDS:
        return
    
    banned_users = db.get_all_banned_users()
    
    if not banned_users:
        await m.answer("✅ Нет забаненных пользователей")
        return
    
    bans_text = "🚫 <b>Забаненные пользователи:</b>\n\n"
    for user in banned_users:
        user_id, username, reason, admin_id, banned_at, unbanned_at, duration = user
        
        if unbanned_at:
            try:
                unbanned_dt = datetime.strptime(unbanned_at, '%Y-%m-%d %H:%M:%S.%f')
                time_left = unbanned_dt - datetime.now()
                hours_left = max(0, int(time_left.total_seconds() // 3600))
                minutes_left = max(0, int((time_left.total_seconds() % 3600) // 60))
                ban_info = f"Разбан через: {hours_left}ч {minutes_left}мин"
            except:
                ban_info = f"Разбан: {unbanned_at[:16] if unbanned_at else 'Никогда'}"
        else:
            ban_info = "Перманентно"
        
        bans_text += (
            f"👤 <b>ID:</b> <code>{user_id}</code>\n"
            f"📛 <b>Username:</b> @{username or 'нет'}\n"
            f"📌 <b>Причина:</b> {reason or 'Не указана'}\n"
            f"⏰ <b>Забанен:</b> {banned_at[:16] if banned_at else 'Неизвестно'}\n"
            f"⏳ <b>Статус:</b> {ban_info}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
        )
    
    if len(bans_text) > 4000:
        for i in range(0, len(bans_text), 4000):
            await m.answer(bans_text[i:i+4000], parse_mode=ParseMode.HTML)
    else:
        await m.answer(bans_text, parse_mode=ParseMode.HTML)

@router.message(Command("unban"))
async def unban_cmd(m: Message, command: CommandObject) -> None:
    uid = m.from_user.id
    
    if uid not in ADMIN_IDS:
        return
    
    args = command.args
    if not args:
        await m.answer("❌ Использование: /unban [ID пользователя]\nПример: /unban 123456789")
        return
    
    try:
        user_id_to_unban = int(args.strip())
    except ValueError:
        await m.answer("❌ Неверный формат ID. ID должен быть числом.")
        return
    
    ban_info = db.get_ban_info(user_id_to_unban)
    if not ban_info:
        await m.answer(f"❌ Пользователь с ID {user_id_to_unban} не найден в бан-листе.")
        return
    
    if db.unban_user(user_id_to_unban):
        try:
            await bot.send_message(
                user_id_to_unban,
                "✅ <b>Вы были разбанены администратором!</b>\n\n"
                "Теперь вы можете снова пользоваться ботом.",
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Не удалось уведомить пользователя {user_id_to_unban} о разбане: {e}")
        
        await m.answer(
            f"✅ Пользователь <code>{user_id_to_unban}</code> успешно разбанен!",
            parse_mode=ParseMode.HTML
        )
        
        logger.info(f"Админ {uid} разбанил пользователя {user_id_to_unban}")
    else:
        await m.answer(f"❌ Не удалось разбанить пользователя {user_id_to_unban}")

@router.message(Command("ban"))
async def ban_cmd(m: Message, command: CommandObject) -> None:
    uid = m.from_user.id
    
    if uid not in ADMIN_IDS:
        return
    
    args = command.args
    if not args:
        await m.answer("❌ Использование: /ban [ID пользователя] [причина]\nПример: /ban 123456789 Спам")
        return
    
    args_parts = args.split(maxsplit=1)
    if len(args_parts) < 1:
        await m.answer("❌ Использование: /ban [ID пользователя] [причина]\nПример: /ban 123456789 Спам")
        return
    
    try:
        user_id_to_ban = int(args_parts[0].strip())
    except ValueError:
        await m.answer("❌ Неверный формат ID. ID должен быть числом.")
        return
    
    reason = args_parts[1] if len(args_parts) > 1 else "Без указания причины"
    
    if user_id_to_ban in ADMIN_IDS:
        await m.answer("❌ Невозможно забанить администратора!")
        logger.warning(f"Админ {uid} попытался забанить админа {user_id_to_ban}")
        return
    
    if user_id_to_ban == uid:
        await m.answer("❌ Нельзя забанить самого себя!")
        return
    
    try:
        user = await bot.get_chat(user_id_to_ban)
        username = user.username or ""
        full_name = user.full_name or ""
    except Exception as e:
        logger.error(f"Не удалось получить информацию о пользователе {user_id_to_ban}: {e}")
        username = ""
        full_name = "Неизвестный пользователь"
    
    db.ban_user(
        user_id=user_id_to_ban,
        username=username,
        full_name=full_name,
        reason=reason,
        admin_id=uid,
        duration_hours=24
    )
    
    if user_id_to_ban in pairs:
        partner_id = pairs[user_id_to_ban]
        
        if user_id_to_ban in pairs:
            del pairs[user_id_to_ban]
        if partner_id in pairs:
            del pairs[partner_id]
        
        save_data()
        
        try:
            await bot.send_message(
                partner_id,
                "❌ <b>Диалог завершен</b>\n\n"
                "Ваш собеседник был забанен администратором.",
                parse_mode=ParseMode.HTML,
                reply_markup=main_kb
            )
            logger.info(f"Диалог между {user_id_to_ban} и {partner_id} завершен из-за бана администратором")
        except Exception as e:
            logger.error(f"Не удалось уведомить собеседника {partner_id} о завершении диалога: {e}")
    
    try:
        await bot.send_message(
            user_id_to_ban,
            f"🚫 <b>Вы были забанены администратором!</b>\n\n"
            f"<b>Причина:</b> {reason}\n"
            f"<b>Длительность:</b> 24 часа\n\n"
            f"По истечении этого времени вы сможете снова пользоваться ботом.",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Не удалось уведомить пользователя {user_id_to_ban} о бане: {e}")
    
    await m.answer(
        f"✅ Пользователь <code>{user_id_to_ban}</code> успешно забанен на 24 часа!\n\n"
        f"📛 Username: @{username or 'нет'}\n"
        f"📝 Имя: {full_name}\n"
        f"📌 Причина: {reason}",
        parse_mode=ParseMode.HTML
    )
    
    logger.info(f"Админ {uid} забанил пользователя {user_id_to_ban} по причине: {reason}")

@router.message(Command("givevip"))
async def givevip_cmd(m: Message, command: CommandObject) -> None:
    uid = m.from_user.id
    
    if uid not in ADMIN_IDS:
        logger.warning(f"Пользователь {uid} попытался использовать /givevip без прав")
        await m.answer("⛔️ У вас нет доступа к этой команде.")
        return
    
    if not command.args:
        await m.answer(
            "❌ Использование: /givevip [ID пользователя] [количество дней]\n"
            "Пример: /givevip 123456789 7",
            parse_mode=ParseMode.HTML
        )
        return
    
    args = command.args.strip().split()
    if len(args) < 2:
        await m.answer(
            "❌ Использование: /givevip [ID пользователя] [количество дней]\n"
            "Пример: /givevip 123456789 7",
            parse_mode=ParseMode.HTML
        )
        return
    
    try:
        user_id_to_give = int(args[0])
        days = int(args[1])
    except ValueError:
        await m.answer("❌ ID и количество дней должны быть числами!")
        return
    
    if days <= 0 or days > 999999:
        await m.answer("❌ Количество дней должно быть от 1 до 999999!")
        return
    
    if user_id_to_give in ADMIN_IDS:
        await m.answer("❌ Нельзя выдать VIP администратору!")
        return
    
    if user_id_to_give == uid:
        await m.answer("❌ Нельзя выдать VIP самому себе!")
        return
    
    username = ""
    full_name = ""
    user = None
    
    try:
        user = await bot.get_chat(user_id_to_give)
        username = user.username or ""
        full_name = user.full_name or f"User_{user_id_to_give}"
        logger.info(f"Получена информация о пользователе {user_id_to_give}: @{username}, {full_name}")
    except Exception as e:
        logger.error(f"Не удалось получить информацию о пользователе {user_id_to_give}: {e}")
        full_name = f"User_{user_id_to_give}"
    
    if user_id_to_give not in users:
        users[user_id_to_give] = {}
        logger.info(f"✅ Создан новый пользователь {user_id_to_give}")
    
    if "gender" not in users[user_id_to_give]:
        users[user_id_to_give]["gender"] = "not_specified"
    if "country" not in users[user_id_to_give]:
        users[user_id_to_give]["country"] = "not_specified"
    if "city" not in users[user_id_to_give]:
        users[user_id_to_give]["city"] = "other"
        users[user_id_to_give]["city_display"] = "🌍 Другой город"
    
    if user and hasattr(user, 'language_code') and user.language_code:
        country_code = detect_user_country(user)
        if country_code != "INT":
            users[user_id_to_give]["country"] = country_code.lower()
    
    save_data()
    logger.info(f"✅ Пользователь {user_id_to_give} зарегистрирован: {users[user_id_to_give]}")
    
    # Определяем уровень VIP по количеству дней
    if days >= 999999:
        level = "forever"
    elif days >= 365:
        level = "platinum"
    elif days >= 180:
        level = "gold"
    elif days >= 90:
        level = "silver"
    else:
        level = "bronze"
    
    seconds = days * 86400
    until = give_vip(user_id_to_give, seconds, level)
    until_str = time.strftime("%d.%m.%Y %H:%M", time.localtime(until)) if until != float('inf') else "НАВСЕГДА"
    logger.info(f"✅ VIP выдан пользователю {user_id_to_give} до {until_str}")
    
    payment_id = f"admin_give_{user_id_to_give}_{int(time.time())}"
    
    try:
        user_country = "INT"
        if user and hasattr(user, 'language_code'):
            user_country = detect_user_country(user)
        
        payment_db_id = db.add_payment(
            user_id=user_id_to_give,
            username=username,
            full_name=full_name,
            days=days,
            level=level,
            amount=0,
            currency="ADMIN",
            method="admin_gift",
            payment_id=payment_id,
            country=user_country,
            metadata=json.dumps({
                "admin_id": uid,
                "admin_username": m.from_user.username,
                "admin_full_name": m.from_user.full_name,
                "type": "admin_gift",
                "days": days,
                "level": level,
                "issued_at": time.time()
            })
        )
        
        db.update_payment_status(payment_id, "completed")
        
        logger.info(f"✅ Данные сохранены в БД, payment_id: {payment_id}")
    except Exception as e:
        logger.error(f"❌ Ошибка при сохранении в БД: {e}")
    
    try:
        admin_mention = f"@{m.from_user.username}" if m.from_user.username else f"Администратор"
        level_name = get_vip_level(days)["name"]
        await bot.send_message(
            user_id_to_give,
            f"🎁 <b>Вам выдан VIP статус!</b>\n\n"
            f"👑 <b>Уровень:</b> {level_name}\n"
            f"⏰ <b>Срок:</b> {days} дней\n"
            f"📅 <b>Действует до:</b> {until_str}\n"
            f"👤 <b>Выдал:</b> {admin_mention}\n\n"
            f"Нажмите /start чтобы начать пользоваться ботом.",
            parse_mode=ParseMode.HTML
        )
        logger.info(f"✅ Уведомление отправлено пользователю {user_id_to_give}")
    except Exception as e:
        logger.error(f"❌ Не удалось отправить уведомление пользователю {user_id_to_give}: {e}")
    
    await m.answer(
        f"✅ <b>VIP статус успешно выдан!</b>\n\n"
        f"👤 <b>Пользователь:</b> <code>{user_id_to_give}</code>\n"
        f"📛 <b>Имя:</b> {full_name}\n"
        f"🔗 <b>Username:</b> @{username or 'нет'}\n"
        f"👑 <b>Уровень:</b> {level}\n"
        f"⏰ <b>Срок:</b> {days} дней\n"
        f"📅 <b>Действует до:</b> {until_str}",
        parse_mode=ParseMode.HTML
    )
    
    admin_mention = f"@{m.from_user.username}" if m.from_user.username else f"ID: {uid}"
    user_mention = f"@{username}" if username else f"ID: {user_id_to_give}"
    
    await notify_admin(
        f"🎁 <b>Административная выдача VIP</b>\n\n"
        f"👤 <b>Выдал:</b> {admin_mention}\n"
        f"👥 <b>Получил:</b> {full_name} ({user_mention})\n"
        f"👑 <b>Уровень:</b> {level}\n"
        f"⏰ <b>Срок:</b> {days} дней\n"
        f"📅 <b>Действует до:</b> {until_str}",
        parse_mode=ParseMode.HTML
    )
    
    logger.info(f"✅ Команда /givevip успешно выполнена: админ {uid} выдал VIP {user_id_to_give} на {days} дней")

@router.message(Command("giveforever"))
async def giveforever_cmd(m: Message, command: CommandObject) -> None:
    uid = m.from_user.id
    
    if uid not in ADMIN_IDS:
        logger.warning(f"Пользователь {uid} попытался использовать /giveforever без прав")
        await m.answer("⛔️ У вас нет доступа к этой команде.")
        return
    
    if not command.args:
        await m.answer(
            "❌ Использование: /giveforever [ID пользователя]\n"
            "Пример: /giveforever 123456789",
            parse_mode=ParseMode.HTML
        )
        return
    
    try:
        user_id_to_give = int(command.args.strip())
    except ValueError:
        await m.answer("❌ Неверный формат ID. ID должен быть числом.")
        return
    
    if user_id_to_give in ADMIN_IDS:
        await m.answer("❌ Нельзя выдать VIP администратору!")
        return
    
    if user_id_to_give == uid:
        await m.answer("❌ Нельзя выдать VIP самому себе!")
        return
    
    username = ""
    full_name = ""
    user = None
    
    try:
        user = await bot.get_chat(user_id_to_give)
        username = user.username or ""
        full_name = user.full_name or f"User_{user_id_to_give}"
        logger.info(f"Получена информация о пользователе {user_id_to_give}: @{username}, {full_name}")
    except Exception as e:
        logger.error(f"Не удалось получить информацию о пользователе {user_id_to_give}: {e}")
        full_name = f"User_{user_id_to_give}"
    
    if user_id_to_give not in users:
        users[user_id_to_give] = {}
        logger.info(f"✅ Создан новый пользователь {user_id_to_give}")
    
    if "gender" not in users[user_id_to_give]:
        users[user_id_to_give]["gender"] = "not_specified"
    if "country" not in users[user_id_to_give]:
        users[user_id_to_give]["country"] = "not_specified"
    if "city" not in users[user_id_to_give]:
        users[user_id_to_give]["city"] = "other"
        users[user_id_to_give]["city_display"] = "🌍 Другой город"
    
    if user and hasattr(user, 'language_code') and user.language_code:
        country_code = detect_user_country(user)
        if country_code != "INT":
            users[user_id_to_give]["country"] = country_code.lower()
    
    save_data()
    logger.info(f"✅ Пользователь {user_id_to_give} зарегистрирован: {users[user_id_to_give]}")
    
    # Выдаем вечный VIP
    give_forever_vip(user_id_to_give)
    logger.info(f"✅ Королевский VIP выдан пользователю {user_id_to_give} НАВСЕГДА")
    
    payment_id = f"admin_forever_{user_id_to_give}_{int(time.time())}"
    
    try:
        user_country = "INT"
        if user and hasattr(user, 'language_code'):
            user_country = detect_user_country(user)
        
        payment_db_id = db.add_payment(
            user_id=user_id_to_give,
            username=username,
            full_name=full_name,
            days=999999,
            level="forever",
            amount=0,
            currency="ADMIN",
            method="admin_gift",
            payment_id=payment_id,
            country=user_country,
            metadata=json.dumps({
                "admin_id": uid,
                "admin_username": m.from_user.username,
                "admin_full_name": m.from_user.full_name,
                "type": "admin_gift_forever",
                "issued_at": time.time()
            })
        )
        
        db.update_payment_status(payment_id, "completed")
        
        logger.info(f"✅ Данные сохранены в БД, payment_id: {payment_id}")
    except Exception as e:
        logger.error(f"❌ Ошибка при сохранении в БД: {e}")
    
    try:
        admin_mention = f"@{m.from_user.username}" if m.from_user.username else f"Администратор"
        await bot.send_message(
            user_id_to_give,
            f"👑⚡️ <b>Вам выдан КОРОЛЕВСКИЙ VIP статус!</b> 👑⚡️\n\n"
            f"✨ <b>Это навсегда!</b> ✨\n"
            f"Ваш статус никогда не сгорит.\n\n"
            f"👤 <b>Выдал:</b> {admin_mention}\n\n"
            f"Нажмите /start чтобы начать пользоваться ботом.",
            parse_mode=ParseMode.HTML
        )
        logger.info(f"✅ Уведомление отправлено пользователю {user_id_to_give}")
    except Exception as e:
        logger.error(f"❌ Не удалось отправить уведомление пользователю {user_id_to_give}: {e}")
    
    await m.answer(
        f"✅ <b>Королевский VIP статус успешно выдан!</b>\n\n"
        f"👤 <b>Пользователь:</b> <code>{user_id_to_give}</code>\n"
        f"📛 <b>Имя:</b> {full_name}\n"
        f"🔗 <b>Username:</b> @{username or 'нет'}\n"
        f"👑⚡️ <b>Статус:</b> Королевский VIP НАВСЕГДА",
        parse_mode=ParseMode.HTML
    )
    
    admin_mention = f"@{m.from_user.username}" if m.from_user.username else f"ID: {uid}"
    user_mention = f"@{username}" if username else f"ID: {user_id_to_give}"
    
    await notify_admin(
        f"👑⚡️ <b>Административная выдача КОРОЛЕВСКОГО VIP</b> 👑⚡️\n\n"
        f"👤 <b>Выдал:</b> {admin_mention}\n"
        f"👥 <b>Получил:</b> {full_name} ({user_mention})\n"
        f"✨ <b>Статус:</b> НАВСЕГДА",
        parse_mode=ParseMode.HTML
    )
    
    logger.info(f"✅ Команда /giveforever успешно выполнена: админ {uid} выдал вечный VIP {user_id_to_give}")

# ================= НОВАЯ КОМАНДА ДЛЯ ОТБИРАНИЯ VIP =================
@router.message(Command("removevip"))
async def removevip_cmd(m: Message, command: CommandObject) -> None:
    uid = m.from_user.id
    
    if uid not in ADMIN_IDS:
        logger.warning(f"Пользователь {uid} попытался использовать /removevip без прав")
        await m.answer("⛔️ У вас нет доступа к этой команде.")
        return
    
    if not command.args:
        await m.answer(
            "❌ Использование: /removevip [ID пользователя]\n"
            "Пример: /removevip 123456789",
            parse_mode=ParseMode.HTML
        )
        return
    
    try:
        user_id_to_remove = int(command.args.strip())
    except ValueError:
        await m.answer("❌ Неверный формат ID. ID должен быть числом.")
        return
    
    if user_id_to_remove in ADMIN_IDS:
        await m.answer("❌ Нельзя отобрать VIP у администратора!")
        return
    
    if user_id_to_remove == uid:
        await m.answer("❌ Нельзя отобрать VIP у самого себя!")
        return
    
    # Проверяем, есть ли у пользователя VIP
    if not is_vip(user_id_to_remove):
        await m.answer(f"❌ У пользователя {user_id_to_remove} нет VIP статуса.")
        return
    
    # Получаем информацию о пользователе
    username = ""
    full_name = ""
    try:
        user = await bot.get_chat(user_id_to_remove)
        username = user.username or ""
        full_name = user.full_name or f"User_{user_id_to_remove}"
    except Exception as e:
        logger.error(f"Не удалось получить информацию о пользователе {user_id_to_remove}: {e}")
        full_name = f"User_{user_id_to_remove}"
    
    # Отбираем VIP
    if remove_vip(user_id_to_remove):
        # Записываем в базу данных как отзыв VIP
        try:
            payment_id = f"admin_remove_{user_id_to_remove}_{int(time.time())}"
            user_country = "INT"
            if user and hasattr(user, 'language_code'):
                user_country = detect_user_country(user)
            
            db.add_payment(
                user_id=user_id_to_remove,
                username=username,
                full_name=full_name,
                days=0,
                level="none",
                amount=0,
                currency="ADMIN",
                method="admin_remove",
                payment_id=payment_id,
                country=user_country,
                metadata=json.dumps({
                    "admin_id": uid,
                    "admin_username": m.from_user.username,
                    "admin_full_name": m.from_user.full_name,
                    "type": "admin_remove",
                    "removed_at": time.time()
                })
            )
        except Exception as e:
            logger.error(f"❌ Ошибка при сохранении в БД: {e}")
        
        # Уведомляем пользователя
        try:
            admin_mention = f"@{m.from_user.username}" if m.from_user.username else f"Администратор"
            await bot.send_message(
                user_id_to_remove,
                f"❌ <b>Ваш VIP статус был отобран администратором!</b>\n\n"
                f"👤 <b>Администратор:</b> {admin_mention}\n\n"
                f"Если у вас есть вопросы, обратитесь в поддержку: {SUPPORT_USERNAME}",
                parse_mode=ParseMode.HTML
            )
            logger.info(f"✅ Уведомление отправлено пользователю {user_id_to_remove}")
        except Exception as e:
            logger.error(f"❌ Не удалось отправить уведомление пользователю {user_id_to_remove}: {e}")
        
        # Уведомляем админа об успехе
        await m.answer(
            f"✅ <b>VIP статус успешно отобран!</b>\n\n"
            f"👤 <b>Пользователь:</b> <code>{user_id_to_remove}</code>\n"
            f"📛 <b>Имя:</b> {full_name}\n"
            f"🔗 <b>Username:</b> @{username or 'нет'}\n"
            f"❌ <b>Статус:</b> VIP отобран",
            parse_mode=ParseMode.HTML
        )
        
        # Отправляем уведомление всем админам
        admin_mention = f"@{m.from_user.username}" if m.from_user.username else f"ID: {uid}"
        user_mention = f"@{username}" if username else f"ID: {user_id_to_remove}"
        
        await notify_admin(
            f"❌ <b>Административный отзыв VIP</b>\n\n"
            f"👤 <b>Администратор:</b> {admin_mention}\n"
            f"👥 <b>Пользователь:</b> {full_name} ({user_mention})\n"
            f"❌ <b>Действие:</b> VIP статус отобран",
            parse_mode=ParseMode.HTML
        )
        
        logger.info(f"✅ Команда /removevip успешно выполнена: админ {uid} отобрал VIP у {user_id_to_remove}")
    else:
        await m.answer(f"❌ Не удалось отобрать VIP у пользователя {user_id_to_remove}")

# ================= НОВАЯ КОМАНДА ДЛЯ ПРОСМОТРА ВСЕХ VIP =================
@router.message(Command("vipusers"))
async def vipusers_cmd(m: Message) -> None:
    uid = m.from_user.id
    
    if uid not in ADMIN_IDS:
        return
    
    # Собираем всех VIP пользователей
    vip_users = []
    for user_id, user_data in users.items():
        if is_vip(user_id):
            vip_until = user_data.get("vip_until")
            if vip_until == float('inf'):
                expires = "НАВСЕГДА"
            else:
                expires = time.strftime("%d.%m.%Y %H:%M", time.localtime(vip_until))
            
            level = user_data.get("vip_level", "bronze")
            level_emoji = VIP_LEVELS.get(level, VIP_LEVELS["bronze"])["emoji"]
            
            vip_users.append({
                "id": user_id,
                "name": user_data.get("city_display", f"User_{user_id}"),
                "level": level,
                "emoji": level_emoji,
                "expires": expires
            })
    
    if not vip_users:
        await m.answer("📊 <b>VIP пользователи:</b>\n\n❌ Нет активных VIP пользователей", parse_mode=ParseMode.HTML)
        return
    
    # Сортируем: сначала вечные, потом по дате окончания
    vip_users.sort(key=lambda x: (x["expires"] != "НАВСЕГДА", x["expires"]))
    
    text = f"📊 <b>VIP пользователи ({len(vip_users)}):</b>\n\n"
    
    for i, vip in enumerate(vip_users[:20], 1):  # Показываем первые 20
        text += f"{i}. {vip['emoji']} <code>{vip['id']}</code>\n"
        text += f"   Уровень: {vip['level']}\n"
        text += f"   Действует до: {vip['expires']}\n\n"
    
    if len(vip_users) > 20:
        text += f"\n... и еще {len(vip_users) - 20} пользователей"
    
    await m.answer(text, parse_mode=ParseMode.HTML)

# ================= НОВАЯ КОМАНДА ДЛЯ ПРОСМОТРА ИНФОРМАЦИИ О ПОЛЬЗОВАТЕЛЕ =================
@router.message(Command("userinfo"))
async def userinfo_cmd(m: Message, command: CommandObject) -> None:
    uid = m.from_user.id
    
    if uid not in ADMIN_IDS:
        return
    
    args = command.args
    if not args:
        await m.answer("❌ Использование: /userinfo [ID пользователя]\nПример: /userinfo 123456789")
        return
    
    try:
        user_id = int(args.strip())
    except ValueError:
        await m.answer("❌ Неверный формат ID. ID должен быть числом.")
        return
    
    # Получаем информацию о пользователе
    user_data = users.get(user_id, {})
    stats = db.get_stats(user_id)
    vip_status = is_vip(user_id)
    
    text = f"📋 <b>Информация о пользователе {user_id}</b>\n\n"
    
    # Основная информация
    text += f"👤 <b>Профиль:</b>\n"
    text += f"• Пол: {GENDERS.get(user_data.get('gender', 'not_specified'), 'Не указан')}\n"
    text += f"• Страна: {get_country_name(user_data.get('country', 'not_specified'))}\n"
    text += f"• Город: {user_data.get('city_display', 'Не указан')}\n"
    
    if "birthday" in user_data:
        text += f"• ДР: {user_data['birthday']}\n"
    
    text += f"\n📊 <b>Статистика:</b>\n"
    text += f"• Сообщений: {stats['messages']}\n"
    text += f"• Диалогов: {stats['dialogs']}\n"
    
    # VIP информация
    text += f"\n👑 <b>VIP статус:</b> "
    if vip_status:
        if user_data.get("vip_until") == float('inf'):
            text += f"✅ Королевский VIP (НАВСЕГДА)\n"
        else:
            until = time.strftime("%d.%m.%Y %H:%M", time.localtime(user_data["vip_until"]))
            level = get_vip_level(user_data.get("vip_total_days", 0))
            text += f"✅ Активен\n"
            text += f"• Уровень: {level['name']}\n"
            text += f"• Действует до: {until}\n"
            text += f"• Всего дней: {user_data.get('vip_total_days', 0)}\n"
    else:
        text += f"❌ Нет\n"
    
    # Проверка на бан
    ban_info = db.get_ban_info(user_id)
    if ban_info:
        text += f"\n🚫 <b>В бане:</b> ДА\n"
        text += f"• Причина: {ban_info[2]}\n"
        text += f"• До: {ban_info[5][:16] if ban_info[5] else 'НАВСЕГДА'}\n"
    
    # История нарушений
    violations = db.get_violations(user_id, limit=3)
    if violations:
        text += f"\n⚠️ <b>Последние нарушения:</b>\n"
        for v_type, v_text, v_date in violations[:3]:
            text += f"• {v_type}: {v_date[:16] if v_date else 'Неизвестно'}\n"
    
    # Кнопки действий
    buttons = []
    if vip_status:
        buttons.append([InlineKeyboardButton(text="❌ Отобрать VIP", callback_data=f"admin_removevip_{user_id}")])
    else:
        buttons.append([InlineKeyboardButton(text="✅ Выдать VIP на 30 дней", callback_data=f"admin_givevip_30_{user_id}")])
        buttons.append([InlineKeyboardButton(text="👑 Выдать вечный VIP", callback_data=f"admin_giveforever_{user_id}")])
    
    if not ban_info:
        buttons.append([InlineKeyboardButton(text="🚫 Забанить на 24ч", callback_data=f"admin_ban_{user_id}")])
    else:
        buttons.append([InlineKeyboardButton(text="✅ Разбанить", callback_data=f"admin_unban_{user_id}")])
    
    buttons.append([InlineKeyboardButton(text="🔙 Закрыть", callback_data="close_profile")])
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await m.answer(text, reply_markup=kb, parse_mode=ParseMode.HTML)

# ================= НОВЫЕ ОБРАБОТЧИКИ ДЛЯ АДМИНСКИХ ДЕЙСТВИЙ =================
@router.callback_query(F.data.startswith("admin_removevip_"))
async def admin_callback_removevip(cb: CallbackQuery) -> None:
    admin_id = cb.from_user.id
    
    if admin_id not in ADMIN_IDS:
        await cb.answer("⛔️ Нет доступа!", show_alert=True)
        return
    
    user_id = int(cb.data.replace("admin_removevip_", ""))
    
    if remove_vip(user_id):
        await cb.message.edit_text(f"✅ VIP статус пользователя {user_id} успешно отобран!")
        
        # Уведомляем пользователя
        try:
            await bot.send_message(
                user_id,
                f"❌ Ваш VIP статус был отобран администратором.",
                parse_mode=ParseMode.HTML
            )
        except:
            pass
    else:
        await cb.message.edit_text(f"❌ Не удалось отобрать VIP у пользователя {user_id}")
    
    await cb.answer()

@router.callback_query(F.data.startswith("admin_givevip_"))
async def admin_callback_givevip(cb: CallbackQuery) -> None:
    admin_id = cb.from_user.id
    
    if admin_id not in ADMIN_IDS:
        await cb.answer("⛔️ Нет доступа!", show_alert=True)
        return
    
    parts = cb.data.split("_")
    days = int(parts[2])
    user_id = int(parts[3])
    
    seconds = days * 86400
    level = "bronze" if days < 90 else "silver" if days < 180 else "gold" if days < 365 else "platinum"
    
    give_vip(user_id, seconds, level)
    
    await cb.message.edit_text(f"✅ VIP статус на {days} дней выдан пользователю {user_id}!")
    
    # Уведомляем пользователя
    try:
        await bot.send_message(
            user_id,
            f"🎁 Вам выдан VIP статус на {days} дней!",
            parse_mode=ParseMode.HTML
        )
    except:
        pass
    
    await cb.answer()

@router.callback_query(F.data.startswith("admin_giveforever_"))
async def admin_callback_giveforever(cb: CallbackQuery) -> None:
    admin_id = cb.from_user.id
    
    if admin_id not in ADMIN_IDS:
        await cb.answer("⛔️ Нет доступа!", show_alert=True)
        return
    
    user_id = int(cb.data.replace("admin_giveforever_", ""))
    
    give_forever_vip(user_id)
    
    await cb.message.edit_text(f"✅ Королевский VIP навсегда выдан пользователю {user_id}!")
    
    # Уведомляем пользователя
    try:
        await bot.send_message(
            user_id,
            f"👑⚡️ Вам выдан Королевский VIP навсегда!",
            parse_mode=ParseMode.HTML
        )
    except:
        pass
    
    await cb.answer()

@router.callback_query(F.data.startswith("admin_ban_"))
async def admin_callback_ban(cb: CallbackQuery) -> None:
    admin_id = cb.from_user.id
    
    if admin_id not in ADMIN_IDS:
        await cb.answer("⛔️ Нет доступа!", show_alert=True)
        return
    
    user_id = int(cb.data.replace("admin_ban_", ""))
    
    # Получаем информацию о пользователе
    username = ""
    full_name = ""
    try:
        user = await bot.get_chat(user_id)
        username = user.username or ""
        full_name = user.full_name or ""
    except:
        pass
    
    db.ban_user(
        user_id=user_id,
        username=username,
        full_name=full_name,
        reason="Бан администратором через панель",
        admin_id=admin_id,
        duration_hours=24
    )
    
    await cb.message.edit_text(f"✅ Пользователь {user_id} забанен на 24 часа!")
    
    # Уведомляем пользователя
    try:
        await bot.send_message(
            user_id,
            f"🚫 Вы забанены администратором на 24 часа!",
            parse_mode=ParseMode.HTML
        )
    except:
        pass
    
    await cb.answer()

@router.callback_query(F.data.startswith("admin_unban_"))
async def admin_callback_unban(cb: CallbackQuery) -> None:
    admin_id = cb.from_user.id
    
    if admin_id not in ADMIN_IDS:
        await cb.answer("⛔️ Нет доступа!", show_alert=True)
        return
    
    user_id = int(cb.data.replace("admin_unban_", ""))
    
    if db.unban_user(user_id):
        await cb.message.edit_text(f"✅ Пользователь {user_id} разбанен!")
        
        # Уведомляем пользователя
        try:
            await bot.send_message(
                user_id,
                f"✅ Вы разбанены администратором!",
                parse_mode=ParseMode.HTML
            )
        except:
            pass
    else:
        await cb.message.edit_text(f"❌ Не удалось разбанить пользователя {user_id}")
    
    await cb.answer()

@router.message(Command("violations"))
async def violations_cmd(m: Message, command: CommandObject) -> None:
    uid = m.from_user.id
    
    if uid not in ADMIN_IDS:
        return
    
    args = command.args
    if not args:
        await m.answer("❌ Использование: /violations [ID пользователя]\nПример: /violations 123456789")
        return
    
    try:
        user_id = int(args.strip())
    except ValueError:
        await m.answer("❌ Неверный формат ID. ID должен быть числом.")
        return
    
    violations = db.get_violations(user_id, limit=20)
    
    if not violations:
        await m.answer(f"ℹ️ У пользователя {user_id} нет нарушений.")
        return
    
    violations_text = f"⚠️ <b>История нарушений пользователя {user_id}:</b>\n\n"
    
    for i, (violation_type, message_text, created_at) in enumerate(violations, 1):
        violations_text += (
            f"<b>{i}. Тип:</b> {violation_type}\n"
            f"<b>Сообщение:</b> {message_text[:50] if message_text else 'Нет текста'}...\n"
            f"<b>Время:</b> {created_at[:16] if created_at else 'Неизвестно'}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
        )
    
    if len(violations_text) > 4000:
        for i in range(0, len(violations_text), 4000):
            await m.answer(violations_text[i:i+4000], parse_mode=ParseMode.HTML)
    else:
        await m.answer(violations_text, parse_mode=ParseMode.HTML)

@router.message(Command("broadcast"))
async def broadcast_cmd(m: Message, state: FSMContext) -> None:
    uid = m.from_user.id
    
    if uid not in ADMIN_IDS:
        return
    
    await m.answer(
        "📢 <b>Режим рассылки</b>\n\n"
        "Отправьте сообщение, которое хотите разослать всем пользователям.\n"
        "Для отмены отправьте /cancel",
        parse_mode=ParseMode.HTML
    )
    await state.set_state(AdminStates.waiting_broadcast_message)

@router.message(AdminStates.waiting_broadcast_message)
async def process_broadcast(m: Message, state: FSMContext) -> None:
    uid = m.from_user.id
    
    if uid not in ADMIN_IDS:
        await state.clear()
        return
    
    await m.answer("⏳ Начинаю рассылку...")
    
    sent = 0
    failed = 0
    
    for user_id in users.keys():
        try:
            await bot.send_message(user_id, m.text, parse_mode=ParseMode.HTML)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
            logger.error(f"Не удалось отправить сообщение пользователю {user_id}: {e}")
    
    await m.answer(
        f"📊 <b>Рассылка завершена</b>\n\n"
        f"✅ Успешно: {sent}\n"
        f"❌ Не удалось: {failed}",
        parse_mode=ParseMode.HTML
    )
    await state.clear()

# ================= РЕГИСТРАЦИЯ =================
@router.message(RegistrationStates.waiting_gender)
async def process_gender(m: Message, state: FSMContext) -> None:
    uid = m.from_user.id
    
    if m.text in GENDERS_REVERSE:
        gender = GENDERS_REVERSE[m.text]
        users.setdefault(uid, {})
        users[uid]["gender"] = gender
        save_data()
        
        await m.answer(
            "🌍 Отлично! Теперь выберите вашу страну:",
            reply_markup=country_kb
        )
        await state.set_state(RegistrationStates.waiting_country)
    else:
        await m.answer("Пожалуйста, выберите пол из кнопок:", reply_markup=gender_kb)

@router.message(RegistrationStates.waiting_country)
async def process_country(m: Message, state: FSMContext) -> None:
    uid = m.from_user.id
    
    if m.text in COUNTRY_MAP:
        country = COUNTRY_MAP[m.text]
        users.setdefault(uid, {})
        users[uid]["country"] = country
        save_data()
        
        await state.update_data(selected_country=country)
        
        if country in CITIES:
            city_kb = get_city_kb(country)
            if city_kb:
                await m.answer(
                    f"🌆 Отлично! Теперь выберите ваш город:",
                    reply_markup=city_kb
                )
                await state.set_state(RegistrationStates.waiting_city)
                return
        
        users[uid]["city"] = "other"
        users[uid]["city_display"] = "🌍 Другой город"
        save_data()
        
        await m.answer(
            f"✅ Регистрация завершена!\n\n"
            f"Ваш город: Не указан",
            reply_markup=main_kb
        )
        await state.clear()
        
        new_badges = db.check_new_badges(uid)
        if new_badges:
            badge_text = "🎉 <b>Новые значки!</b>\n\n"
            for badge in new_badges[:3]:
                badge_text += f"{badge['emoji']} <b>{badge['name']}</b>\n{badge['description']}\n\n"
            await m.answer(badge_text, parse_mode=ParseMode.HTML)
    else:
        await m.answer("Пожалуйста, выберите страну из кнопок:", reply_markup=country_kb)

@router.message(RegistrationStates.waiting_city)
async def process_city(m: Message, state: FSMContext) -> None:
    uid = m.from_user.id
    
    text = m.text.strip()
    
    if text in CITY_MAP:
        city_data = CITY_MAP[text]
        country = city_data["country"]
        city = city_data["city"]
        display_name = city_data["display"]
        
        state_data = await state.get_data()
        selected_country = state_data.get("selected_country")
        
        if selected_country and selected_country != country and selected_country != "not_specified":
            await m.answer(
                "❌ Этот город не соответствует выбранной стране",
                reply_markup=get_city_kb(selected_country)
            )
            return
        
        users[uid]["city"] = city
        users[uid]["city_display"] = display_name
        save_data()
        
        await m.answer(
            f"✅ Регистрация завершена!\n\n"
            f"Ваш город: {display_name}",
            reply_markup=main_kb
        )
        await state.clear()
        
        new_badges = db.check_new_badges(uid)
        if new_badges:
            badge_text = "🎉 <b>Новые значки!</b>\n\n"
            for badge in new_badges[:3]:
                badge_text += f"{badge['emoji']} <b>{badge['name']}</b>\n{badge['description']}\n\n"
            await m.answer(badge_text, parse_mode=ParseMode.HTML)
    else:
        state_data = await state.get_data()
        selected_country = state_data.get("selected_country", "not_specified")
        await m.answer(
            "❌ Пожалуйста, выберите город из списка",
            reply_markup=get_city_kb(selected_country)
        )

# ================= ПРОФИЛЬ =================
@router.message(F.text == "👤 Профиль")
async def profile_cmd(m: Message) -> None:
    uid = m.from_user.id
    
    if not is_registered(uid):
        await m.answer("❌ Сначала зарегистрируйтесь через /start!")
        return
    
    profile_text = get_user_profile(uid)
    
    buttons = [
        [InlineKeyboardButton(text="✏️ Изменить пол", callback_data="edit_gender"),
         InlineKeyboardButton(text="🌍 Изменить страну", callback_data="edit_country")],
        [InlineKeyboardButton(text="🌆 Изменить город", callback_data="edit_city"),
         InlineKeyboardButton(text="📅 Указать ДР", callback_data="edit_birthday")],
    ]
    
    if is_vip(uid):
        buttons.append([InlineKeyboardButton(text="👑 История VIP", callback_data="vip_history")])
        buttons.append([InlineKeyboardButton(text="💫 Понравившиеся", callback_data="vip_liked_partners")])
    
    buttons.append([InlineKeyboardButton(text="🔙 Закрыть", callback_data="close_profile")])
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await m.answer(profile_text, reply_markup=kb, parse_mode=ParseMode.HTML)

# ================= МОИ ЗНАЧКИ =================
@router.message(F.text == "🏅 Мои значки")
async def my_badges_cmd(m: Message) -> None:
    uid = m.from_user.id
    
    if not is_registered(uid):
        await m.answer("❌ Сначала зарегистрируйтесь через /start!")
        return
    
    stats = db.get_stats(uid)
    badges = db.get_user_badges(uid)
    
    text = "🏅 <b>Мои значки</b>\n\n"
    text += f"📊 <b>Статистика:</b>\n"
    text += f"• Сообщений: {stats['messages']}\n"
    text += f"• Диалогов: {stats['dialogs']}\n\n"
    
    if not badges:
        text += "❌ У вас пока нет значков"
    else:
        text += f"✅ <b>Получено значков: {len(badges)}</b>\n\n"
        for badge in badges:
            text += f"{badge['emoji']} <b>{badge['name']}</b>\n"
            text += f"  {badge['description']}\n\n"
    
    await m.answer(text, parse_mode=ParseMode.HTML)

# ================= ВСЕ ДОСТИЖЕНИЯ =================
@router.message(F.text == "📋 Все достижения")
async def all_badges_cmd(m: Message) -> None:
    uid = m.from_user.id
    
    if not is_registered(uid):
        await m.answer("❌ Сначала зарегистрируйтесь через /start!")
        return
    
    all_badges = db.get_all_badges()
    
    text = "📋 <b>Все достижения в игре</b>\n\n"
    
    categories = {}
    for badge in all_badges:
        cat = badge['category']
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(badge)
    
    for cat_name, cat_badges in categories.items():
        cat_display = {
            "messages": "🗣 СООБЩЕНИЯ",
            "dialogs": "👥 ДИАЛОГИ",
            "time": "⏱️ ВРЕМЯ В ЧАТЕ",
            "time_of_day": "🌙 ВРЕМЯ СУТОК",
            "streak": "🔥 СЕРИИ",
            "daily": "⚡ ДНЕВНАЯ АКТИВНОСТЬ",
            "special": "🎯 СПЕЦИАЛЬНЫЕ",
            "holiday": "🎉 ПРАЗДНИЧНЫЕ",
            "tournament": "🏆 ТУРНИРНЫЕ",
        }.get(cat_name, cat_name.upper())
        
        text += f"\n<b>{cat_display}</b>\n"
        for badge in cat_badges:
            status = "✅" if db.check_badge(uid, badge['id']) else "❌"
            text += f"{status} {badge['emoji']} <b>{badge['name']}</b>\n"
            text += f"  {badge['description']}\n"
    
    await m.answer(text, parse_mode=ParseMode.HTML)

# ================= ЮРИДИЧЕСКАЯ ИНФОРМАЦИЯ УДАЛЕНА =================

@router.callback_query(F.data == "edit_gender")
async def edit_gender(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.message.answer("Выберите ваш пол:", reply_markup=gender_kb)
    await state.set_state(ProfileStates.waiting_gender_change)
    await cb.answer()

@router.message(ProfileStates.waiting_gender_change)
async def process_gender_change(m: Message, state: FSMContext) -> None:
    uid = m.from_user.id
    if m.text in GENDERS_REVERSE:
        users[uid]["gender"] = GENDERS_REVERSE[m.text]
        save_data()
        await m.answer(f"✅ Пол изменен", reply_markup=main_kb)
        await state.clear()
    else:
        await m.answer("Пожалуйста, выберите пол из кнопок:", reply_markup=gender_kb)

@router.callback_query(F.data == "edit_country")
async def edit_country(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.message.answer("Выберите вашу страну:", reply_markup=country_kb)
    await state.set_state(ProfileStates.waiting_country_change)
    await cb.answer()

@router.message(ProfileStates.waiting_country_change)
async def process_country_change(m: Message, state: FSMContext) -> None:
    uid = m.from_user.id
    if m.text in COUNTRY_MAP:
        country = COUNTRY_MAP[m.text]
        users[uid]["country"] = country
        save_data()
        await state.update_data(selected_country=country)
        
        if country in CITIES:
            await m.answer("Выберите ваш город:", reply_markup=get_city_kb(country))
            await state.set_state(ProfileStates.waiting_city_change)
        else:
            users[uid]["city"] = "other"
            users[uid]["city_display"] = "🌍 Другой город"
            save_data()
            await m.answer(f"✅ Страна изменена", reply_markup=main_kb)
            await state.clear()
    else:
        await m.answer("Пожалуйста, выберите страну из кнопок:", reply_markup=country_kb)

@router.callback_query(F.data == "edit_city")
async def edit_city(cb: CallbackQuery, state: FSMContext) -> None:
    uid = cb.from_user.id
    user_country = users[uid].get("country", "not_specified")
    
    if user_country not in CITIES:
        await cb.answer("❌ Для вашей страны нет списка городов", show_alert=True)
        return
    
    await cb.message.answer("Выберите ваш город:", reply_markup=get_city_kb(user_country))
    await state.set_state(ProfileStates.waiting_city_change)
    await cb.answer()

@router.message(ProfileStates.waiting_city_change)
async def process_city_change(m: Message, state: FSMContext) -> None:
    uid = m.from_user.id
    text = m.text.strip()
    
    if text in CITY_MAP:
        city_data = CITY_MAP[text]
        country = city_data["country"]
        city = city_data["city"]
        display_name = city_data["display"]
        
        user_country = users[uid].get("country", "not_specified")
        
        if user_country != "not_specified" and user_country != country:
            await m.answer(
                "❌ Город не соответствует стране",
                reply_markup=get_city_kb(user_country)
            )
            return
        
        users[uid]["city"] = city
        users[uid]["city_display"] = display_name
        save_data()
        
        await m.answer(f"✅ Город изменен", reply_markup=main_kb)
        await state.clear()
    else:
        user_country = users[uid].get("country", "not_specified")
        if user_country in CITIES:
            await m.answer(
                "❌ Выберите город из списка",
                reply_markup=get_city_kb(user_country)
            )
        else:
            await state.clear()
            await m.answer("❌ Город не выбран", reply_markup=main_kb)

@router.callback_query(F.data == "edit_birthday")
async def edit_birthday(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.message.answer(
        "📅 Введите дату рождения в формате ДД.ММ\nНапример: 15.05",
        parse_mode=ParseMode.HTML
    )
    await state.set_state(ProfileStates.waiting_birthday)
    await cb.answer()

@router.message(ProfileStates.waiting_birthday)
async def process_birthday(m: Message, state: FSMContext) -> None:
    uid = m.from_user.id
    text = m.text.strip()
    
    birthday_pattern = re.compile(r'^(\d{1,2})\.(\d{1,2})$')
    match = birthday_pattern.match(text)
    
    if match:
        day, month = match.groups()
        day, month = int(day), int(month)
        
        if 1 <= day <= 31 and 1 <= month <= 12:
            users[uid]["birthday"] = text
            save_data()
            await m.answer(f"✅ Дата рождения сохранена", reply_markup=main_kb)
            await state.clear()
        else:
            await m.answer("❌ Неверный формат. Попробуйте снова:")
    else:
        await m.answer("❌ Используйте формат ДД.ММ\nНапример: 15.05")

@router.callback_query(F.data == "vip_history")
async def vip_history(cb: CallbackQuery) -> None:
    uid = cb.from_user.id
    
    if not is_vip(uid):
        await cb.answer("❌ У вас нет VIP статуса", show_alert=True)
        return
    
    history = db.get_vip_history(uid)
    
    text = "👑 <b>История VIP статусов</b>\n\n"
    
    if not history:
        text += "Нет записей"
    else:
        for days, level, activated, expires in history[:5]:
            activated_str = activated[:16] if activated else "Неизвестно"
            expires_str = expires[:16] if expires else "НАВСЕГДА"
            level_emoji = VIP_LEVELS.get(level, VIP_LEVELS["bronze"])["emoji"]
            text += f"{level_emoji} <b>{days} дней</b>\n"
            text += f"   Активирован: {activated_str}\n"
            text += f"   Истекает: {expires_str}\n\n"
    
    await cb.message.answer(text, parse_mode=ParseMode.HTML)
    await cb.answer()

@router.callback_query(F.data == "close_profile")
async def close_profile(cb: CallbackQuery) -> None:
    try:
        await cb.message.delete()
    except:
        pass
    await cb.answer()

# ================= VIP СИСТЕМА =================
@router.message(F.text == "⭐️ VIP")
async def vip_menu(m: Message) -> None:
    uid = m.from_user.id
    
    if not is_registered(uid):
        await m.answer("❌ Сначала зарегистрируйтесь через /start!")
        return
    
    user_country = detect_user_country(m.from_user)
    vip_user = is_vip(uid)
    
    if vip_user:
        vip_emoji = get_vip_level_emoji(uid)
        if users[uid].get("vip_until") == float('inf'):
            text = f"{vip_emoji} <b>Королевский VIP активен НАВСЕГДА</b>"
        else:
            until = time.strftime("%d.%m.%Y %H:%M", time.localtime(users[uid]["vip_until"]))
            level = get_vip_level(users[uid].get("vip_total_days", 0))
            text = f"{vip_emoji} <b>VIP активен до {until}</b>\n"
            text += f"Ваш уровень: {level['name']}"
        
        if users[uid].get("vip_only"):
            text += "\n\n💫 <b>Режим VIP-чата включен</b>"
    else:
        country_name = PAYMENT_COUNTRIES.get(user_country, "🌍 Другие страны")
        text = f"⭐️ <b>VIP не активен</b>\n📍 Регион: {country_name}"
    
    kb = get_vip_menu_kb(vip_user, user_country)
    
    await m.answer(text, reply_markup=kb, parse_mode=ParseMode.HTML)

@router.callback_query(F.data == "vip_stats")
async def vip_stats(cb: CallbackQuery) -> None:
    uid = cb.from_user.id
    
    if not is_vip(uid):
        await cb.answer("❌ VIP статус не активен!", show_alert=True)
        return
    
    vip_stats = db.get_vip_stats()
    
    text = (
        "👑 <b>СТАТИСТИКА VIP-КЛУБА</b> 👑\n\n"
        f"👥 <b>Всего VIP:</b> {vip_stats['total']}\n"
        f"🟢 <b>Сейчас онлайн:</b> {vip_stats['online']}\n\n"
        f"💫 <b>VIP-чат:</b> {'✅ Доступен' if users[uid].get('vip_only') else '❌ Отключен'}\n\n"
        f"✨ Элитное общение только для избранных!"
    )
    
    await cb.message.answer(text, parse_mode=ParseMode.HTML)
    await cb.answer()

@router.callback_query(F.data == "vip_toggle_vip_only")
async def vip_toggle_vip_only(cb: CallbackQuery) -> None:
    uid = cb.from_user.id
    
    if not is_vip(uid):
        await cb.answer("❌ VIP статус не активен!", show_alert=True)
        return
    
    # Переключаем режим VIP-чата
    if users[uid].get("vip_only"):
        users[uid]["vip_only"] = False
        status = "❌ ОТКЛЮЧЕН"
    else:
        users[uid]["vip_only"] = True
        status = "✅ ВКЛЮЧЕН"
    
    save_data()
    
    await cb.message.answer(
        f"💫 <b>VIP-чат</b>\n\n"
        f"Режим поиска только среди VIP: {status}\n\n"
        f"Теперь вы будете искать собеседников только с VIP статусом.",
        parse_mode=ParseMode.HTML
    )
    await cb.answer()

# ================= НОВЫЙ ОБРАБОТЧИК: КАСТОМИЗАЦИЯ =================
@router.callback_query(F.data == "vip_customization")
async def vip_customization(cb: CallbackQuery, state: FSMContext) -> None:
    uid = cb.from_user.id
    
    if not is_vip(uid):
        await cb.answer("❌ Только для VIP!", show_alert=True)
        return
    
    current_color = db.get_vip_color(uid)
    
    text = (
        "🎨 <b>Кастомизация профиля</b>\n\n"
        f"Текущий цвет ника: {current_color}\n\n"
        "Выберите цвет для вашего ника в диалогах:"
    )
    
    buttons = []
    for color_code, color_name in VIP_COLORS.items():
        buttons.append([InlineKeyboardButton(
            text=f"{color_name}", 
            callback_data=f"vip_set_color_{color_code}"
        )])
    
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="vip_menu_back")])
    
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode=ParseMode.HTML)
    await cb.answer()

@router.callback_query(F.data.startswith("vip_set_color_"))
async def vip_set_color(cb: CallbackQuery) -> None:
    uid = cb.from_user.id
    color_code = cb.data.replace("vip_set_color_", "")
    
    if not is_vip(uid):
        await cb.answer("❌ Только для VIP!", show_alert=True)
        return
    
    color_emoji = {
        "red": "🔴",
        "blue": "🔵",
        "green": "🟢",
        "yellow": "🟡",
        "purple": "🟣",
        "orange": "🟠"
    }.get(color_code, "⚪️")
    
    db.set_vip_color(uid, color_emoji)
    
    await cb.message.answer(
        f"✅ Цвет ника изменен на {color_emoji}\n\n"
        f"Теперь в диалогах ваш ник будет выделяться!",
        parse_mode=ParseMode.HTML
    )
    await cb.answer()

# ================= НОВЫЙ ОБРАБОТЧИК: ПОНРАВИВШИЕСЯ СОБЕСЕДНИКИ =================
@router.callback_query(F.data == "vip_liked_partners")
async def vip_liked_partners(cb: CallbackQuery) -> None:
    uid = cb.from_user.id
    
    if not is_vip(uid):
        await cb.answer("❌ Только для VIP!", show_alert=True)
        return
    
    liked = db.get_liked_partners(uid)
    
    if not liked:
        await cb.message.answer(
            "💫 <b>Понравившиеся собеседники</b>\n\n"
            "У вас пока нет понравившихся собеседников.\n\n"
            "После завершения диалога вы можете оценить собеседника, и он появится здесь!",
            parse_mode=ParseMode.HTML
        )
        await cb.answer()
        return
    
    text = "💫 <b>Понравившиеся собеседники</b>\n\n"
    
    for item in liked[:10]:
        date_str = item["dialog_date"][:16] if item["dialog_date"] else "Недавно"
        invite_status = "✅ Можно пригласить" if item["can_invite"] else "⏳ Приглашение отправлено"
        
        text += f"• {item['anon_name']}\n"
        text += f"  🕒 {date_str} | {invite_status}\n\n"
    
    text += "\n<i>Выберите собеседника для приглашения:</i>"
    
    buttons = []
    for item in liked[:5]:
        if item["can_invite"]:
            buttons.append([InlineKeyboardButton(
                text=f"💌 {item['anon_name']}",
                callback_data=f"vip_invite_{item['partner_id']}"
            )])
    
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="vip_menu_back")])
    
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode=ParseMode.HTML)
    await cb.answer()

@router.callback_query(F.data.startswith("vip_invite_"))
async def vip_send_invite(cb: CallbackQuery) -> None:
    uid = cb.from_user.id
    partner_id = int(cb.data.replace("vip_invite_", ""))
    
    if not is_vip(uid):
        await cb.answer("❌ Только для VIP!", show_alert=True)
        return
    
    if db.send_invite(uid, partner_id):
        # Получаем анонимное имя партнера
        tournament_id = db.get_current_tournament_id()
        cursor = db.conn.cursor()
        cursor.execute('''
            SELECT anon_name FROM tournaments
            WHERE tournament_id = ? AND user_id = ?
        ''', (tournament_id, partner_id))
        result = cursor.fetchone()
        
        if result:
            anon_name = result[0]
        else:
            anon_name = generate_tournament_name(partner_id, "temp")
        
        # Отправляем приглашение партнеру
        try:
            accept_kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Принять", callback_data=f"invite_accept_{uid}"),
                 InlineKeyboardButton(text="❌ Отклонить", callback_data=f"invite_decline_{uid}")]
            ])
            
            await bot.send_message(
                partner_id,
                f"💌 <b>Вам пришло приглашение!</b>\n\n"
                f"Ваш прошлый собеседник ({anon_name}) хочет пообщаться снова.\n\n"
                f"У вас есть 24 часа, чтобы ответить.",
                reply_markup=accept_kb,
                parse_mode=ParseMode.HTML
            )
            
            await cb.message.answer(
                f"✅ Приглашение отправлено!\n\n"
                f"Если {anon_name} примет его, вы сразу начнете диалог.",
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Не удалось отправить приглашение {partner_id}: {e}")
            await cb.message.answer("❌ Не удалось отправить приглашение. Возможно, пользователь заблокировал бота.")
    else:
        await cb.message.answer("❌ Вы уже отправляли приглашение этому собеседнику. Попробуйте позже.")
    
    await cb.answer()

@router.callback_query(F.data.startswith("invite_accept_"))
async def invite_accept(cb: CallbackQuery) -> None:
    uid = cb.from_user.id
    inviter_id = int(cb.data.replace("invite_accept_", ""))
    
    # Проверяем, не в диалоге ли уже
    if uid in pairs or inviter_id in pairs:
        await cb.message.edit_text("❌ Один из вас уже в диалоге. Попробуйте позже.")
        await cb.answer()
        return
    
    # Создаем диалог
    pairs[uid] = inviter_id
    pairs[inviter_id] = uid
    save_data()
    
    # Уведомляем обоих
    await bot.send_message(uid, "✅ Собеседник найден! Общайтесь!", reply_markup=main_kb_with_stop, parse_mode=ParseMode.HTML)
    await bot.send_message(inviter_id, "✅ Собеседник найден! Общайтесь!", reply_markup=main_kb_with_stop, parse_mode=ParseMode.HTML)
    
    await cb.message.edit_text("✅ Приглашение принято! Диалог начат.")
    await cb.answer()

@router.callback_query(F.data.startswith("invite_decline_"))
async def invite_decline(cb: CallbackQuery) -> None:
    inviter_id = int(cb.data.replace("invite_decline_", ""))
    
    await cb.message.edit_text("❌ Приглашение отклонено.")
    await cb.answer()
    
    # Уведомляем пригласившего
    try:
        await bot.send_message(inviter_id, "😔 Ваш прошлый собеседник отклонил приглашение.")
    except:
        pass

# ================= ОБРАБОТЧИКИ VIP ФИЛЬТРОВ =================
@router.callback_query(F.data == "vip_filter_gender")
async def vip_filter_gender(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_vip(cb.from_user.id):
        await cb.answer("❌ VIP статус не активен!", show_alert=True)
        return
    
    await cb.message.answer("Выберите пол собеседника:", reply_markup=gender_kb)
    await state.set_state(VIPFilterStates.waiting_gender_filter)
    await cb.answer()

@router.message(VIPFilterStates.waiting_gender_filter)
async def process_gender_filter(m: Message, state: FSMContext) -> None:
    uid = m.from_user.id
    
    if not is_vip(uid):
        await state.clear()
        await m.answer("❌ VIP статус не активен!")
        return
    
    if m.text in GENDERS_REVERSE:
        gender = GENDERS_REVERSE[m.text]
        users[uid].setdefault("search_prefs", {})["gender"] = gender
        save_data()
        await m.answer(f"✅ Фильтр по полу установлен", reply_markup=main_kb)
        await state.clear()
    else:
        await m.answer("Пожалуйста, выберите пол из кнопок:", reply_markup=gender_kb)

@router.callback_query(F.data == "vip_filter_country")
async def vip_filter_country(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_vip(cb.from_user.id):
        await cb.answer("❌ VIP статус не активен!", show_alert=True)
        return
    
    await cb.message.answer("Выберите страну собеседника:", reply_markup=country_kb)
    await state.set_state(VIPFilterStates.waiting_country_filter)
    await cb.answer()

@router.message(VIPFilterStates.waiting_country_filter)
async def process_country_filter(m: Message, state: FSMContext) -> None:
    uid = m.from_user.id
    
    if not is_vip(uid):
        await state.clear()
        await m.answer("❌ VIP статус не активен!")
        return
    
    if m.text in COUNTRY_MAP:
        country = COUNTRY_MAP[m.text]
        users[uid].setdefault("search_prefs", {})["country"] = country
        save_data()
        await m.answer(f"✅ Фильтр по стране установлен", reply_markup=main_kb)
        await state.clear()
    else:
        await m.answer("Пожалуйста, выберите страну из кнопок:", reply_markup=country_kb)

@router.callback_query(F.data == "vip_filter_city")
async def vip_filter_city(cb: CallbackQuery, state: FSMContext) -> None:
    uid = cb.from_user.id
    
    if not is_vip(uid):
        await cb.answer("❌ VIP статус не активен!", show_alert=True)
        return
    
    user_country = users[uid].get("country", "not_specified")
    
    if user_country not in CITIES:
        await cb.answer("❌ Для этой страны нет списка городов", show_alert=True)
        return
    
    await cb.message.answer("Выберите город собеседника:", reply_markup=get_city_kb(user_country))
    await state.set_state(VIPFilterStates.waiting_city_filter)
    await cb.answer()

@router.message(VIPFilterStates.waiting_city_filter)
async def process_city_filter(m: Message, state: FSMContext) -> None:
    uid = m.from_user.id
    
    if not is_vip(uid):
        await state.clear()
        await m.answer("❌ VIP статус не активен!")
        return
    
    text = m.text.strip()
    
    if text in CITY_MAP:
        city_data = CITY_MAP[text]
        city = city_data["city"]
        display_name = city_data["display"]
        
        users[uid].setdefault("search_prefs", {})["city"] = city
        users[uid]["search_prefs"]["city_display"] = display_name
        save_data()
        
        await m.answer(f"✅ Фильтр по городу установлен", reply_markup=main_kb)
        await state.clear()
    else:
        user_country = users[uid].get("country", "not_specified")
        if user_country in CITIES:
            await m.answer("❌ Выберите город из списка", reply_markup=get_city_kb(user_country))
        else:
            await state.clear()
            await m.answer("❌ Город не выбран", reply_markup=main_kb)

@router.callback_query(F.data == "vip_show_filters")
async def vip_show_filters(cb: CallbackQuery) -> None:
    uid = cb.from_user.id
    
    if not is_vip(uid):
        await cb.answer("❌ VIP статус не активен!", show_alert=True)
        return
    
    prefs = users[uid].get("search_prefs", {})
    vip_only = users[uid].get("vip_only", False)
    
    if not prefs and not vip_only:
        await cb.message.answer("📋 Фильтры не установлены")
        await cb.answer()
        return
    
    text = "📋 <b>Ваши фильтры:</b>\n\n"
    
    if vip_only:
        text += f"💫 <b>VIP-чат:</b> ВКЛЮЧЕН (только VIP)\n"
    
    if "gender" in prefs:
        gender_text = GENDERS.get(prefs["gender"], prefs["gender"])
        text += f"👤 Пол: {gender_text}\n"
    if "country" in prefs:
        if prefs["country"] == "not_specified":
            text += f"🌍 Страна: Любая\n"
        else:
            country_name = get_country_name(prefs["country"])
            text += f"🌍 Страна: {country_name}\n"
    if "city" in prefs:
        text += f"🌆 Город: {prefs.get('city_display', prefs['city'])}\n"
    
    await cb.message.answer(text, parse_mode=ParseMode.HTML)
    await cb.answer()

@router.callback_query(F.data == "vip_reset_filters")
async def vip_reset_filters(cb: CallbackQuery) -> None:
    uid = cb.from_user.id
    
    if not is_vip(uid):
        await cb.answer("❌ VIP статус не активен!", show_alert=True)
        return
    
    if "search_prefs" in users[uid]:
        del users[uid]["search_prefs"]
    # Не сбрасываем vip_only
    
    save_data()
    await cb.message.answer("✅ Фильтры поиска сброшены (кроме VIP-чата)")
    await cb.answer()

@router.callback_query(F.data == "payment_history")
async def payment_history(cb: CallbackQuery) -> None:
    uid = cb.from_user.id
    
    payments = db.get_user_payments(uid)
    
    if not payments:
        await cb.message.answer("ℹ️ У вас пока нет платежей")
        await cb.answer()
        return
    
    text = "💳 <b>История платежей:</b>\n\n"
    for payment in payments:
        _, days, amount, currency, method, status, date = payment
        status_emoji = "✅" if status == "completed" else "⏳"
        method_name = "Stars" if method == "stars" else "ЮKassa" if method == "yookassa" else method
        text += f"{status_emoji} {days} дней - {amount} {currency} ({method_name})\n"
    
    await cb.message.answer(text, parse_mode=ParseMode.HTML)
    await cb.answer()

# ================= ОПЛАТА TELEGRAM STARS =================
@router.callback_query(F.data == "vip_stars_method")
async def vip_stars_method(cb: CallbackQuery) -> None:
    text = "⭐️ <b>Оплата Stars</b>\n\n"
    text += "<b>Доступные тарифы:</b>\n\n"
    
    for days, plan in VIP_PLANS_STARS.items():
        if days <= 365:
            level_emoji = VIP_LEVELS[plan["level"]]["emoji"]
            text += f"• {level_emoji} {plan['title']} — {plan['stars']}⭐️\n"
    
    text += "\n<i>Королевский VIP (навсегда) не продается</i>"
    
    buttons = []
    for days, plan in VIP_PLANS_STARS.items():
        if days <= 365:
            level_emoji = VIP_LEVELS[plan["level"]]["emoji"]
            buttons.append([InlineKeyboardButton(
                text=f"{level_emoji} {plan['title']} — {plan['stars']}⭐️",
                callback_data=f"buy_vip_stars_{days}"
            )])
    
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="vip_menu_back")])
    
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode=ParseMode.HTML)
    await cb.answer()

@router.callback_query(F.data.startswith("buy_vip_stars_"))
async def buy_vip_stars(cb: CallbackQuery) -> None:
    try:
        days = int(cb.data.split("_")[-1])
    except:
        await cb.answer("❌ Ошибка", show_alert=True)
        return
    
    if days not in VIP_PLANS_STARS or days > 365:
        await cb.answer("❌ Тариф не найден", show_alert=True)
        return
    
    plan = VIP_PLANS_STARS[days]
    uid = cb.from_user.id
    stars_amount = plan["stars"]
    
    try:
        await bot.send_invoice(
            chat_id=uid,
            title=f"{plan['title']}",
            description=f"Покупка VIP статуса на {days} дней. Уровень: {plan['level']}",
            payload=f"vip_stars_{days}_{uid}",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice(label=plan["title"], amount=stars_amount)],
            start_parameter=f"vip_stars_{days}"
        )
        await cb.answer("✅ Счет отправлен")
    except Exception as e:
        logger.error(f"Ошибка создания счета: {e}")
        await cb.answer("❌ Ошибка", show_alert=True)

@router.pre_checkout_query()
async def pre_checkout_query_handler(pre_checkout_q: PreCheckoutQuery) -> None:
    await bot.answer_pre_checkout_query(pre_checkout_q.id, ok=True)

@router.message(F.successful_payment)
async def successful_payment_handler(message: Message) -> None:
    uid = message.from_user.id
    payment = message.successful_payment
    total_amount = payment.total_amount
    payload = payment.invoice_payload
    
    try:
        days = int(payload.split("_")[2])
        
        if days not in VIP_PLANS_STARS:
            logger.error(f"Неизвестный тариф VIP: {days} дней")
            return
        
        plan = VIP_PLANS_STARS[days]
        
        until = give_vip(uid, plan["seconds"], plan["level"])
        until_str = time.strftime("%d.%m.%Y %H:%M", time.localtime(until))
        level_emoji = VIP_LEVELS[plan["level"]]["emoji"]
        
        payment_db_id = db.add_payment(
            user_id=uid,
            username=message.from_user.username or "",
            full_name=message.from_user.full_name,
            days=days,
            level=plan["level"],
            amount=total_amount,
            currency="XTR",
            method="stars",
            payment_id=f"stars_{int(time.time())}_{uid}",
            country=detect_user_country(message.from_user)
        )
        
        db.update_payment_status(f"stars_{int(time.time())}_{uid}", "completed")
        
        await message.answer(
            f"🎉 <b>VIP активирован!</b>\n\n"
            f"{level_emoji} <b>Уровень:</b> {plan['level']}\n"
            f"✅ <b>Срок:</b> {days} дней\n"
            f"📅 <b>Действует до:</b> {until_str}",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Ошибка активации VIP: {e}")
        await message.answer("❌ Ошибка активации VIP")

# ================= БЛОКИРОВКА МЕДИАФАЙЛОВ =================
@router.message(F.photo | F.video | F.audio | F.voice | F.document | F.sticker | F.animation)
async def block_media_in_dialogs(m: Message) -> None:
    uid = m.from_user.id
    
    if uid in pairs:
        await m.answer("🚫 В этом чате можно отправлять только текстовые сообщения. Фото, видео, стикеры и другие медиафайлы отправлять нельзя.\n\n<i>💫 VIP могут отправлять медиафайлы</i>", parse_mode=ParseMode.HTML)
        logger.info(f"Пользователь {uid} попытался отправить медиафайл в диалоге")
        return
    
    if not is_registered(uid):
        await m.answer("❌ Сначала зарегистрируйтесь через /start!")

# ================= ПОИСК СОБЕСЕДНИКА =================
@router.message(F.text == "🔍 Найти собеседника")
async def search_companion(m: Message) -> None:
    uid = m.from_user.id
    
    if not is_registered(uid):
        await m.answer("❌ Сначала зарегистрируйтесь через /start!")
        return
    
    if uid in pairs:
        await m.answer("❌ Вы уже в диалоге!")
        return
    
    if uid in waiting or uid in vip_waiting:
        await m.answer("⏳ Вы уже ищете собеседника...")
        return
    
    vip_user = is_vip(uid)
    
    # НОВОЕ: приоритетный поиск для VIP
    if vip_user and users[uid].get("priority_search", True):
        vip_waiting.append(uid)
        queue_type = "VIP"
    else:
        waiting.append(uid)
        queue_type = "обычную"
    
    save_data()
    await m.answer(f"🔍 Ищем собеседника в {queue_type} очереди...")
    
    vip_only = users[uid].get("vip_only", False) if vip_user else False
    search_prefs = users[uid].get("search_prefs", {}) if vip_user else {}
    
    # Сначала проверяем VIP очередь
    if vip_user:
        for partner in vip_waiting[:]:
            if partner == uid:
                continue
            
            partner_vip = is_vip(partner)
            
            if vip_only and not partner_vip:
                continue
            
            if partner_vip and users[partner].get("vip_only", False) and not vip_user:
                continue
            
            if search_prefs:
                partner_data = users.get(partner, {})
                
                if "gender" in search_prefs and search_prefs["gender"] != "not_specified":
                    if partner_data.get("gender") != search_prefs["gender"]:
                        continue
                
                if "country" in search_prefs and search_prefs["country"] != "not_specified":
                    if partner_data.get("country") != search_prefs["country"]:
                        continue
                
                if "city" in search_prefs and search_prefs["city"] != "other":
                    if partner_data.get("city") != search_prefs["city"]:
                        continue
            
            if partner_vip and not vip_user:
                partner_prefs = users[partner].get("search_prefs", {})
                user_data = users.get(uid, {})
                
                if "gender" in partner_prefs and partner_prefs["gender"] != "not_specified":
                    if user_data.get("gender") != partner_prefs["gender"]:
                        continue
                
                if "country" in partner_prefs and partner_prefs["country"] != "not_specified":
                    if user_data.get("country") != partner_prefs["country"]:
                        continue
                
                if "city" in partner_prefs and partner_prefs["city"] != "other":
                    if user_data.get("city") != partner_prefs["city"]:
                        continue
            
            if uid in vip_waiting:
                vip_waiting.remove(uid)
            if partner in vip_waiting:
                vip_waiting.remove(partner)
            
            pairs[uid] = partner
            pairs[partner] = uid
            save_data()
            
            user_country = users[uid].get("country", "not_specified")
            partner_country = users[partner].get("country", "not_specified")
            
            db.add_dialog(uid)
            db.add_dialog(partner)
            
            # Формируем приветственные сообщения
            user_greeting = "✅ Собеседник найден! Общайтесь!\n\n"
            partner_greeting = "✅ Собеседник найден! Общайтесь!\n\n"
            
            user_greeting += "⚠️ <b>Важно:</b> В этом чате можно отправлять только текстовые сообщения.\n"
            user_greeting += "Фото, видео, стикеры и другие медиафайлы отправлять нельзя.\n\n"
            user_greeting += "Чтобы завершить диалог, нажмите '🚫 Завершить диалог'"
            
            partner_greeting += "⚠️ <b>Важно:</b> В этом чате можно отправлять только текстовые сообщения.\n"
            partner_greeting += "Фото, видео, стикеры и другие медиафайлы отправлять нельзя.\n\n"
            partner_greeting += "Чтобы завершить диалог, нажмите '🚫 Завершить диалог'"
            
            await bot.send_message(uid, user_greeting, reply_markup=main_kb_with_stop, parse_mode=ParseMode.HTML)
            await bot.send_message(partner, partner_greeting, reply_markup=main_kb_with_stop, parse_mode=ParseMode.HTML)
            
            logger.info(f"Создан диалог между {uid} (VIP: {vip_user}) и {partner} (VIP: {partner_vip})")
            return
    
    # Если не нашли в VIP очереди, ищем в обычной
    for partner in waiting[:]:
        if partner == uid:
            continue
        
        partner_vip = is_vip(partner)
        
        if vip_only and not partner_vip:
            continue
        
        if partner_vip and users[partner].get("vip_only", False) and not vip_user:
            continue
        
        if vip_user and search_prefs:
            partner_data = users.get(partner, {})
            
            if "gender" in search_prefs and search_prefs["gender"] != "not_specified":
                if partner_data.get("gender") != search_prefs["gender"]:
                    continue
            
            if "country" in search_prefs and search_prefs["country"] != "not_specified":
                if partner_data.get("country") != search_prefs["country"]:
                    continue
            
            if "city" in search_prefs and search_prefs["city"] != "other":
                if partner_data.get("city") != search_prefs["city"]:
                    continue
        
        if partner_vip and not vip_user:
            partner_prefs = users[partner].get("search_prefs", {})
            user_data = users.get(uid, {})
            
            if "gender" in partner_prefs and partner_prefs["gender"] != "not_specified":
                if user_data.get("gender") != partner_prefs["gender"]:
                    continue
            
            if "country" in partner_prefs and partner_prefs["country"] != "not_specified":
                if user_data.get("country") != partner_prefs["country"]:
                    continue
            
            if "city" in partner_prefs and partner_prefs["city"] != "other":
                if user_data.get("city") != partner_prefs["city"]:
                    continue
        
        if uid in waiting:
            waiting.remove(uid)
        if uid in vip_waiting:
            vip_waiting.remove(uid)
        if partner in waiting:
            waiting.remove(partner)
        
        pairs[uid] = partner
        pairs[partner] = uid
        save_data()
        
        user_country = users[uid].get("country", "not_specified")
        partner_country = users[partner].get("country", "not_specified")
        
        db.add_dialog(uid)
        db.add_dialog(partner)
        
        # Формируем приветственные сообщения
        user_greeting = "✅ Собеседник найден! Общайтесь!\n\n"
        partner_greeting = "✅ Собеседник найден! Общайтесь!\n\n"
        
        user_greeting += "⚠️ <b>Важно:</b> В этом чате можно отправлять только текстовые сообщения.\n"
        user_greeting += "Фото, видео, стикеры и другие медиафайлы отправлять нельзя.\n\n"
        user_greeting += "Чтобы завершить диалог, нажмите '🚫 Завершить диалог'"
        
        partner_greeting += "⚠️ <b>Важно:</b> В этом чате можно отправлять только текстовые сообщения.\n"
        partner_greeting += "Фото, видео, стикеры и другие медиафайлы отправлять нельзя.\n\n"
        partner_greeting += "Чтобы завершить диалог, нажмите '🚫 Завершить диалог'"
        
        await bot.send_message(uid, user_greeting, reply_markup=main_kb_with_stop, parse_mode=ParseMode.HTML)
        await bot.send_message(partner, partner_greeting, reply_markup=main_kb_with_stop, parse_mode=ParseMode.HTML)
        
        logger.info(f"Создан диалог между {uid} (VIP: {vip_user}) и {partner} (VIP: {partner_vip})")
        return

@router.message(F.text == "🚫 Завершить диалог")
@router.message(F.text == "⏹️ Остановить поиск")
async def stop_dialog(m: Message, state: FSMContext) -> None:
    uid = m.from_user.id
    
    if uid in pairs:
        partner = pairs[uid]
        
        # НОВОЕ: запрос оценки для VIP
        if is_vip(uid):
            rating_kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="👍 Понравилось", callback_data=f"rate_like_{partner}"),
                 InlineKeyboardButton(text="👎 Не понравилось", callback_data=f"rate_dislike_{partner}")],
                [InlineKeyboardButton(text="⏭ Пропустить", callback_data=f"rate_skip_{partner}")]
            ])
            
            await m.answer(
                "❌ Диалог завершен\n\n"
                "💫 Оцените собеседника (только для VIP):",
                reply_markup=rating_kb
            )
        else:
            await m.answer("❌ Диалог завершен", reply_markup=main_kb)
        
        # Удаляем пару
        if uid in pairs:
            del pairs[uid]
        if partner in pairs:
            del pairs[partner]
        save_data()
        
        try:
            await bot.send_message(partner, "❌ Собеседник завершил диалог", reply_markup=main_kb)
        except:
            pass
    
    elif uid in waiting:
        waiting.remove(uid)
        save_data()
        await m.answer("❌ Поиск остановлен", reply_markup=main_kb)
    
    elif uid in vip_waiting:
        vip_waiting.remove(uid)
        save_data()
        await m.answer("❌ Поиск остановлен", reply_markup=main_kb)
    
    else:
        await m.answer("❌ Вы не в диалоге и не ищете собеседника", reply_markup=main_kb)

# ================= НОВЫЙ ОБРАБОТЧИК: ОЦЕНКА ДИАЛОГА =================
@router.callback_query(F.data.startswith("rate_"))
async def rate_dialog(cb: CallbackQuery) -> None:
    uid = cb.from_user.id
    data = cb.data.split("_")
    action = data[1]
    partner_id = int(data[2])
    
    if action == "like":
        db.add_dialog_rating(uid, partner_id, 1)
        await cb.message.edit_text("✅ Собеседник отмечен как понравившийся!\n\nОн появится в разделе '💫 Понравившиеся' вашего профиля.")
    elif action == "dislike":
        db.add_dialog_rating(uid, partner_id, 0)
        await cb.message.edit_text("✅ Спасибо за оценку!")
    else:
        await cb.message.edit_text("❌ Диалог завершен")
    
    await cb.answer()

# ================= ТУРНИРНАЯ СИСТЕМА =================
@router.message(Command("tournament"))
async def tournament_cmd(m: Message) -> None:
    uid = m.from_user.id
    
    if not is_registered(uid):
        await m.answer("❌ Сначала зарегистрируйтесь через /start!")
        return
    
    tournament_id = db.get_current_tournament_id()
    end_date = datetime.strptime(tournament_id, "%Y-%m-%d") + timedelta(days=7)
    days_left = (end_date - datetime.now()).days
    
    user_rank = db.get_user_tournament_rank(uid, tournament_id)
    
    text = (
        "🏆 <b>ТУРНИР НЕДЕЛИ</b> 🏆\n\n"
        f"📅 До конца: {days_left} дней\n\n"
        "<b>ТОП-10 ЛИДЕРОВ:</b>\n"
    )
    
    if user_rank:
        for i, player in enumerate(user_rank["top10"], 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "▫️"
            text += f"{medal} {player['name']} — {player['messages']} сообщ.\n"
        
        text += f"\n📊 <b>ВАША ПОЗИЦИЯ:</b> #{user_rank['rank']} ({user_rank['messages']} сообщ.)\n"
        
        if user_rank['rank'] <= 10:
            text += f"🔥 Вы в топ-10! Держитесь!"
        elif user_rank['top10']:
            diff = user_rank['top10'][-1]['messages'] - user_rank['messages']
            text += f"🎯 До топ-10: {diff} сообщ."
    else:
        text += "Участников пока нет. Начните общаться, чтобы попасть в турнир!"
    
    await m.answer(text, parse_mode=ParseMode.HTML)

# ================= ОБРАБОТКА ТЕКСТОВЫХ СООБЩЕНИЙ =================
@router.message(F.text)
async def handle_message(m: Message) -> None:
    uid = m.from_user.id
    
    db.add_message(uid, m.from_user.username or "", m.from_user.full_name)
    
    new_badges = db.check_new_badges(uid)
    if new_badges:
        badge_text = "🎉 <b>Новые значки!</b>\n\n"
        for badge in new_badges:
            badge_text += f"{badge['emoji']} <b>{badge['name']}</b>\n{badge['description']}\n\n"
        await m.answer(badge_text, parse_mode=ParseMode.HTML)
    
    if uid in pairs:
        partner = pairs[uid]
        
        # НОВОЕ: добавляем цвет для VIP
        if is_vip(uid):
            vip_color = db.get_vip_color(uid)
            if vip_color != "⚪️":
                # Здесь можно добавить форматирование с цветом
                await bot.send_message(partner, f"{vip_color} {m.text}")
            else:
                await bot.send_message(partner, m.text)
        else:
            await bot.send_message(partner, m.text)
    
    elif not is_registered(uid):
        await m.answer("❌ Сначала зарегистрируйтесь через /start!")
    else:
        if m.text not in ["🔍 Найти собеседника", "⏹️ Остановить поиск", "🚫 Завершить диалог", 
                         "⭐️ VIP", "👤 Профиль", "🏅 Мои значки", "📋 Все достижения", "📜 Правила"]:
            await m.answer("ℹ️ Сначала найдите собеседника с помощью кнопки '🔍 Найти собеседника'")

# ================= ОБРАБОТКА CALLBACK-ЗАПРОСОВ =================
@router.callback_query(F.data == "main_menu_back")
async def main_menu_back(cb: CallbackQuery) -> None:
    try:
        await cb.message.delete()
    except:
        pass
    
    if cb.from_user.id in pairs:
        await cb.message.answer("🏠 Главное меню", reply_markup=main_kb_with_stop)
    else:
        await cb.message.answer("🏠 Главное меню", reply_markup=main_kb)
    
    await cb.answer()

@router.callback_query(F.data == "vip_menu_back")
async def vip_menu_back(cb: CallbackQuery) -> None:
    await vip_menu(cb.message)
    await cb.answer()

# ================= ЗАПУСК БОТА =================
async def main():
    logger.info("🤖 Бот запускается...")
    load_data()
    await notify_admin("✅ Бот запущен")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())