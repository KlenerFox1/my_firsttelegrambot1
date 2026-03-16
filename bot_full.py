#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Telegram Bot для продажи аккаунтов
Версия 8.0 - CryptoBot API и админ панель только для админов
"""

import asyncio
import logging
import json
import os
import re
import random
import string
import hashlib
import hmac
import base64
import csv
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
from contextlib import asynccontextmanager
from io import BytesIO
import aiofiles
import aiosqlite
import aiohttp

# Установка библиотек
import subprocess
import sys

def install_requirements():
    """Автоматическая установка необходимых библиотек"""
    packages = [
        'aiogram==3.3.0',
        'python-dotenv==1.0.0',
        'aiofiles==23.2.1',
        'aiosqlite==0.19.0',
        'aiohttp==3.9.1'
    ]
    
    # pip package -> import module
    import_names = {
        "python-dotenv": "dotenv",
        "aiogram": "aiogram",
        "aiofiles": "aiofiles",
        "aiosqlite": "aiosqlite",
        "aiohttp": "aiohttp",
    }
    
    for package in packages:
        try:
            pip_name = package.split('==')[0]
            module_name = import_names.get(pip_name, pip_name)
            __import__(module_name)
            print(f"[OK] {package} already installed")
        except ImportError:
            print(f"[INSTALL] Installing {package}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            print(f"[OK] {package} installed")

install_requirements()

# Импорты aiogram
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    FSInputFile, BufferedInputFile, ChatMemberUpdated, ChatJoinRequest,
    User as TgUser
)
from aiogram.filters import Command, CommandObject, ChatMemberUpdatedFilter, StateFilter
from aiogram.filters.chat_member_updated import IS_MEMBER, IS_NOT_MEMBER, IS_ADMIN
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from dotenv import load_dotenv

# ============================================
# НАСТРОЙКА ЛОГИРОВАНИЯ
# ============================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================
# ЗАГРУЗКА КОНФИГУРАЦИИ
# ============================================

load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN')
if not BOT_TOKEN:
    print("⚠️ ВНИМАНИЕ: Не найден BOT_TOKEN в файле .env!")
    BOT_TOKEN = input("Введите токен бота: ").strip()

ADMIN_IDS = [int(id) for id in os.getenv('ADMIN_IDS', '').split(',') if id]
if not ADMIN_IDS:
    print("⚠️ ВНИМАНИЕ: Не указаны ADMIN_IDS в файле .env!")
    try:
        admin_id = int(input("Введите ваш Telegram ID: ").strip())
        ADMIN_IDS = [admin_id]
    except:
        ADMIN_IDS = []

# Всегда считаем владельца админом (подстраховка, если .env отсутствует/битый)
_OWNER_ADMIN_ID = 8693383904
if _OWNER_ADMIN_ID not in ADMIN_IDS:
    ADMIN_IDS.append(_OWNER_ADMIN_ID)

# CryptoBot API настройки
CRYPTOBOT_API_KEY = os.getenv('CRYPTOBOT_API_KEY', '')
CRYPTOBOT_API_URL = "https://pay.crypt.bot/api"

SUPPORT_ID = os.getenv('SUPPORT_ID', '@support')
CHANNEL_ID = os.getenv('CHANNEL_ID', '@channel')
GROUP_ID = os.getenv('GROUP_ID', '@group')

# ============================================
# CRYPTOBOT API КЛАСС
# ============================================

class CryptoBotAPI:
    """Класс для работы с CryptoBot API (@send)"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://pay.crypt.bot/api"
        self.headers = {
            "Crypto-Pay-API-Token": api_key,
            "Content-Type": "application/json"
        }
    
    async def get_me(self) -> Optional[Dict]:
        """Получение информации о кошельке"""
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(f"{self.base_url}/getMe", headers=self.headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("ok"):
                            return data.get("result")
            except Exception as e:
                logger.error(f"CryptoBot API error: {e}")
            return None
    
    async def get_balance(self) -> List[Dict]:
        """Получение баланса кошелька"""
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(f"{self.base_url}/getBalance", headers=self.headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("ok"):
                            return data.get("result", [])
            except Exception as e:
                logger.error(f"CryptoBot API error: {e}")
            return []
    
    async def get_asset_balance(self, asset: str = "USDT") -> float:
        """Получение баланса конкретного актива"""
        balances = await self.get_balance()
        for item in balances:
            if item.get("asset") == asset:
                return float(item.get("available", 0))
        return 0.0
    
    async def create_invoice(
        self,
        amount: float,
        asset: str = "USDT",
        description: str = "",
        expires_in_minutes: int = 60,
    ) -> Optional[Dict]:
        """Создание счета для оплаты"""
        async with aiohttp.ClientSession() as session:
            payload = {
                "amount": str(amount),
                "asset": asset,
                "description": description[:128],
                # Crypto Pay API ожидает секунды
                "expires_in": int(expires_in_minutes) * 60,
            }
            
            try:
                async with session.post(
                    f"{self.base_url}/createInvoice",
                    headers=self.headers,
                    json=payload
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("ok"):
                            return data.get("result")
            except Exception as e:
                logger.error(f"CryptoBot API error: {e}")
            
            return None
    
    async def get_invoice_status(self, invoice_id: int) -> Optional[str]:
        """Получение статуса счета"""
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(
                    f"{self.base_url}/getInvoices",
                    headers=self.headers,
                    params={"invoice_ids": str(invoice_id)}
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("ok") and data.get("result", {}).get("items"):
                            return data["result"]["items"][0].get("status")
            except Exception as e:
                logger.error(f"CryptoBot API error: {e}")
            
            return None
    
    async def transfer(self, user_id: int, amount: float, 
                       asset: str = "USDT", spend_id: str = None) -> Optional[Dict]:
        """Перевод средств пользователю (вывод)"""
        if not spend_id:
            spend_id = f"withdraw_{int(datetime.now().timestamp())}_{random.randint(1000, 9999)}"
        
        async with aiohttp.ClientSession() as session:
            payload = {
                "user_id": str(user_id),
                "amount": str(amount),
                "asset": asset,
                "spend_id": spend_id
            }
            
            try:
                async with session.post(
                    f"{self.base_url}/transfer",
                    headers=self.headers,
                    json=payload
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("ok"):
                            return data.get("result")
                    elif resp.status == 400:
                        error_data = await resp.json()
                        logger.error(f"CryptoBot transfer error: {error_data}")
            except Exception as e:
                logger.error(f"CryptoBot API error: {e}")
            
            return None
    
    async def get_transfers(self, asset: str = "USDT", limit: int = 50) -> List[Dict]:
        """Получение истории переводов"""
        async with aiohttp.ClientSession() as session:
            params = {
                "asset": asset,
                "limit": limit
            }
            try:
                async with session.get(
                    f"{self.base_url}/getTransfers",
                    headers=self.headers,
                    params=params
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("ok"):
                            return data.get("result", {}).get("items", [])
            except Exception as e:
                logger.error(f"CryptoBot API error: {e}")
            return []
    
    async def check_transfer_status(self, transfer_id: int) -> Optional[Dict]:
        """Проверка статуса перевода"""
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(
                    f"{self.base_url}/getTransfers",
                    headers=self.headers,
                    params={"transfer_ids": str(transfer_id)}
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("ok") and data.get("result", {}).get("items"):
                            return data["result"]["items"][0]
            except Exception as e:
                logger.error(f"CryptoBot API error: {e}")
            return None

# Инициализация CryptoBot API
cryptobot = CryptoBotAPI(CRYPTOBOT_API_KEY) if CRYPTOBOT_API_KEY else None

# ============================================
# ENUMS (ПЕРЕЧИСЛЕНИЯ)
# ============================================

class AccountType(str, Enum):
    """Типы аккаунтов"""
    TELEGRAM = "tg"
    VK = "vk"
    INSTAGRAM = "ig"
    TIKTOK = "tt"
    YOUTUBE = "yt"
    DISCORD = "dc"
    TWITTER = "tw"
    FACEBOOK = "fb"
    WHATSAPP = "wa"
    OTHER = "other"

class RequestStatus(str, Enum):
    """Статусы заявок"""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    WAITING_CODE = "waiting_code"
    WAITING_PASSWORD = "waiting_password"
    PROCESSING = "processing"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    REFUNDED = "refunded"
    UNREGISTERED = "unregistered"
    GIVEN_BY_ADMIN = "given_by_admin"

class TicketStatus(str, Enum):
    """Статусы тикетов"""
    OPEN = "open"
    IN_PROGRESS = "in_progress"
    WAITING_USER = "waiting_user"
    WAITING_ADMIN = "waiting_admin"
    CLOSED = "closed"
    RESOLVED = "resolved"

class PaymentStatus(str, Enum):
    """Статусы платежей"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    REFUNDED = "refunded"

class PaymentMethod(str, Enum):
    """Методы оплаты"""
    USDT = "usdt"
    BTC = "btc"
    ETH = "eth"
    CARD = "card"
    CRYPTOBOT = "cryptobot"
    INTERNAL = "internal"

class UserRole(str, Enum):
    """Роли пользователей"""
    USER = "user"
    VIP = "vip"
    WORKER = "worker"
    MODERATOR = "moderator"
    ADMIN = "admin"
    SUPER_ADMIN = "super_admin"

class UserLevel(str, Enum):
    """Уровни пользователей"""
    NEWBIE = "Новичок"
    BEGINNER = "Начинающий"
    EXPERIENCED = "Опытный"
    PRO = "Профи"
    EXPERT = "Эксперт"
    LEGEND = "Легенда"

# ============================================
# МОДЕЛИ ДАННЫХ
# ============================================

@dataclass
class User:
    """Модель пользователя"""
    id: int
    username: str = ""
    first_name: str = ""
    last_name: str = ""
    language: str = "ru"
    role: UserRole = UserRole.USER
    level: UserLevel = UserLevel.NEWBIE
    balance: float = 0.0
    bonus_balance: float = 0.0
    frozen_balance: float = 0.0
    total_deposits: float = 0.0
    total_withdrawals: float = 0.0
    total_requests: int = 0
    successful_requests: int = 0
    given_accounts: int = 0
    rating: float = 0.0
    referrals_count: int = 0
    referrer_id: Optional[int] = None
    referral_code: str = ""
    registration_date: str = ""
    last_activity: str = ""
    is_blocked: bool = False
    is_verified: bool = False
    cryptobot_id: Optional[int] = None  # ID в CryptoBot для выплат
    settings: Dict[str, Any] = field(default_factory=dict)
    notifications: Dict[str, bool] = field(default_factory=lambda: {
        'new_request': True,
        'request_status': True,
        'payment': True,
        'withdrawal': True,
        'newsletter': True
    })
    
    def __post_init__(self):
        if not self.registration_date:
            self.registration_date = datetime.now().isoformat()
        if not self.last_activity:
            self.last_activity = self.registration_date
        if not self.referral_code:
            self.referral_code = self._generate_code()
    
    def _generate_code(self) -> str:
        chars = string.ascii_uppercase + string.digits
        return ''.join(random.choices(chars, k=8))
    
    @property
    def full_name(self) -> str:
        parts = [self.first_name, self.last_name]
        name = ' '.join(p for p in parts if p)
        return name or self.username or f"User {self.id}"
    
    @property
    def mention(self) -> str:
        if self.username:
            return f"@{self.username}"
        return f"<a href='tg://user?id={self.id}'>{self.full_name}</a>"
    
    @property
    def total_balance(self) -> float:
        return self.balance + self.bonus_balance
    
    def get_level(self) -> str:
        if self.successful_requests >= 1000:
            return UserLevel.LEGEND
        elif self.successful_requests >= 500:
            return UserLevel.EXPERT
        elif self.successful_requests >= 100:
            return UserLevel.PRO
        elif self.successful_requests >= 50:
            return UserLevel.EXPERIENCED
        elif self.successful_requests >= 10:
            return UserLevel.BEGINNER
        else:
            return UserLevel.NEWBIE

@dataclass
class CryptoBotInvoice:
    """Модель счета CryptoBot"""
    invoice_id: int
    user_id: int
    amount: float
    asset: str
    status: str
    pay_url: str
    created_at: str
    expires_at: str
    paid_at: Optional[str] = None

@dataclass
class CryptoBotTransfer:
    """Модель перевода CryptoBot"""
    transfer_id: int
    user_id: int
    amount: float
    asset: str
    status: str
    completed_at: Optional[str] = None
    withdrawal_id: Optional[int] = None

@dataclass
class Withdrawal:
    """Модель вывода средств"""
    id: int
    user_id: int
    amount: float
    method: PaymentMethod
    wallet: str
    status: PaymentStatus = PaymentStatus.PENDING
    created_at: str = ""
    processed_at: Optional[str] = None
    fee: float = 0.0
    net_amount: float = 0.0
    cryptobot_transfer_id: Optional[int] = None
    processed_by: Optional[int] = None
    comment: str = ""
    
    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now().isoformat()
        if self.net_amount == 0:
            self.net_amount = self.amount - self.fee

@dataclass
class AccountRequest:
    """Модель заявки на продажу"""
    id: int
    user_id: int
    account_type: str
    phone_number: str
    status: RequestStatus = RequestStatus.PENDING
    code: str = ""
    password: str = ""
    worker_id: Optional[int] = None
    worker_note: str = ""
    admin_note: str = ""
    user_note: str = ""
    price: float = 0.0
    created_at: str = ""
    updated_at: str = ""
    completed_at: Optional[str] = None
    reviewed_by: Optional[int] = None
    given_by: Optional[int] = None
    is_vip: bool = False
    is_unregistered: bool = False
    is_admin_given: bool = False
    logs: List[Dict] = field(default_factory=list)
    
    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now().isoformat()
        if not self.updated_at:
            self.updated_at = self.created_at

@dataclass
class AdminAction:
    """Лог действий администратора"""
    id: int
    admin_id: int
    action_type: str
    target_id: Optional[int] = None
    details: Dict = field(default_factory=dict)
    created_at: str = ""
    
    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now().isoformat()

# ============================================
# БАЗА ДАННЫХ
# ============================================

class Database:
    """Класс для работы с данными"""
    
    def __init__(self, db_path: str = "bot_database.db"):
        self.db_path = db_path
        self.users: Dict[int, User] = {}
        self.requests: Dict[int, AccountRequest] = {}
        self.withdrawals: Dict[int, Withdrawal] = {}
        self.cryptobot_invoices: Dict[int, CryptoBotInvoice] = {}
        self.cryptobot_transfers: Dict[int, CryptoBotTransfer] = {}
        self.admin_actions: List[AdminAction] = []
        
        self.next_request_id = 1
        self.next_withdrawal_id = 1
        self.next_admin_action_id = 1
        
        self.settings = {
            'treasury_balance': 10000.0,
            'min_withdrawal': 10.0,
            'max_withdrawal': 10000.0,
            'withdrawal_fee': 0.05,
            'referral_bonus': 0.05,
            'bonus_on_register': 0.02,
            'bonus_on_referral': 0.05,
            'daily_bonus': 0.01,
            'min_price': 5.0,
            'max_price': 10000.0,
            'max_requests_per_day': 10,
            'cryptobot_enabled': bool(CRYPTOBOT_API_KEY),
            'auto_withdraw_enabled': True,
            'test_mode': False,
            'maintenance_mode': False,
            'last_backup': "",
            'stop_accepting': False,
            'account_types': {
                # key -> {label, enabled}
                'tg': {'label': 'Telegram', 'enabled': True},
                'vk': {'label': 'VK', 'enabled': True},
                'ig': {'label': 'Instagram', 'enabled': True},
                'tt': {'label': 'TikTok', 'enabled': True},
                'yt': {'label': 'YouTube', 'enabled': True},
                'dc': {'label': 'Discord', 'enabled': True},
                'tw': {'label': 'Twitter', 'enabled': True},
                'fb': {'label': 'Facebook', 'enabled': True},
                'wa': {'label': 'WhatsApp', 'enabled': True},
                'other': {'label': 'Other', 'enabled': True},
            },
        }
    
    async def init_db(self):
        """Инициализация SQLite базы данных"""
        async with aiosqlite.connect(self.db_path) as db:
            # Таблица пользователей
            await db.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    language TEXT,
                    role TEXT,
                    level TEXT,
                    balance REAL,
                    bonus_balance REAL,
                    frozen_balance REAL,
                    total_deposits REAL,
                    total_withdrawals REAL,
                    total_requests INTEGER,
                    successful_requests INTEGER,
                    given_accounts INTEGER,
                    rating REAL,
                    referrals_count INTEGER,
                    referrer_id INTEGER,
                    referral_code TEXT,
                    registration_date TEXT,
                    last_activity TEXT,
                    is_blocked INTEGER,
                    is_verified INTEGER,
                    cryptobot_id INTEGER,
                    settings TEXT,
                    notifications TEXT
                )
            ''')
            
            # Таблица заявок
            await db.execute('''
                CREATE TABLE IF NOT EXISTS requests (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    account_type TEXT,
                    phone_number TEXT,
                    status TEXT,
                    code TEXT,
                    password TEXT,
                    worker_id INTEGER,
                    worker_note TEXT,
                    admin_note TEXT,
                    user_note TEXT,
                    price REAL,
                    created_at TEXT,
                    updated_at TEXT,
                    completed_at TEXT,
                    reviewed_by INTEGER,
                    given_by INTEGER,
                    is_vip INTEGER,
                    is_unregistered INTEGER,
                    is_admin_given INTEGER,
                    logs TEXT
                )
            ''')
            
            # Таблица выводов
            await db.execute('''
                CREATE TABLE IF NOT EXISTS withdrawals (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    amount REAL,
                    method TEXT,
                    wallet TEXT,
                    status TEXT,
                    created_at TEXT,
                    processed_at TEXT,
                    fee REAL,
                    net_amount REAL,
                    cryptobot_transfer_id INTEGER,
                    processed_by INTEGER,
                    comment TEXT
                )
            ''')
            
            # Таблица счетов CryptoBot
            await db.execute('''
                CREATE TABLE IF NOT EXISTS cryptobot_invoices (
                    invoice_id INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    amount REAL,
                    asset TEXT,
                    status TEXT,
                    pay_url TEXT,
                    created_at TEXT,
                    expires_at TEXT,
                    paid_at TEXT,
                    purpose TEXT,
                    credited INTEGER DEFAULT 0
                )
            ''')
            
            # Таблица переводов CryptoBot
            await db.execute('''
                CREATE TABLE IF NOT EXISTS cryptobot_transfers (
                    transfer_id INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    amount REAL,
                    asset TEXT,
                    status TEXT,
                    completed_at TEXT,
                    withdrawal_id INTEGER
                )
            ''')
            
            # Таблица действий администраторов
            await db.execute('''
                CREATE TABLE IF NOT EXISTS admin_actions (
                    id INTEGER PRIMARY KEY,
                    admin_id INTEGER,
                    action_type TEXT,
                    target_id INTEGER,
                    details TEXT,
                    created_at TEXT
                )
            ''')
            
            # Таблица настроек
            await db.execute('''
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TEXT
                )
            ''')
            
            await db.commit()
            
            # Мягкая миграция для старых баз
            try:
                await db.execute("ALTER TABLE cryptobot_invoices ADD COLUMN purpose TEXT")
            except Exception:
                pass
            try:
                await db.execute("ALTER TABLE cryptobot_invoices ADD COLUMN credited INTEGER DEFAULT 0")
            except Exception:
                pass
            await db.commit()
    
    async def load(self):
        """Загрузка всех данных"""
        await self.init_db()
        await self._load_from_sqlite()
    
    async def _load_from_sqlite(self):
        """Загрузка из SQLite"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            # Загрузка пользователей
            cursor = await db.execute('SELECT * FROM users')
            rows = await cursor.fetchall()
            for row in rows:
                user = User(
                    id=row['id'],
                    username=row['username'] or '',
                    first_name=row['first_name'] or '',
                    last_name=row['last_name'] or '',
                    language=row['language'] or 'ru',
                    role=UserRole(row['role']),
                    level=UserLevel(row['level']) if row['level'] else UserLevel.NEWBIE,
                    balance=row['balance'] or 0,
                    bonus_balance=row['bonus_balance'] or 0,
                    frozen_balance=row['frozen_balance'] or 0,
                    total_deposits=row['total_deposits'] or 0,
                    total_withdrawals=row['total_withdrawals'] or 0,
                    total_requests=row['total_requests'] or 0,
                    successful_requests=row['successful_requests'] or 0,
                    given_accounts=row['given_accounts'] or 0,
                    rating=row['rating'] or 0,
                    referrals_count=row['referrals_count'] or 0,
                    referrer_id=row['referrer_id'],
                    referral_code=row['referral_code'] or '',
                    registration_date=row['registration_date'] or '',
                    last_activity=row['last_activity'] or '',
                    is_blocked=bool(row['is_blocked']),
                    is_verified=bool(row['is_verified']),
                    cryptobot_id=row['cryptobot_id'],
                    settings=json.loads(row['settings']) if row['settings'] else {},
                    notifications=json.loads(row['notifications']) if row['notifications'] else {}
                )
                self.users[user.id] = user
            
            # Загрузка заявок
            cursor = await db.execute('SELECT * FROM requests')
            rows = await cursor.fetchall()
            for row in rows:
                req = AccountRequest(
                    id=row['id'],
                    user_id=row['user_id'],
                    account_type=row['account_type'],
                    phone_number=row['phone_number'],
                    status=RequestStatus(row['status']),
                    code=row['code'] or '',
                    password=row['password'] or '',
                    worker_id=row['worker_id'],
                    worker_note=row['worker_note'] or '',
                    admin_note=row['admin_note'] or '',
                    user_note=row['user_note'] or '',
                    price=row['price'] or 0,
                    created_at=row['created_at'] or '',
                    updated_at=row['updated_at'] or '',
                    completed_at=row['completed_at'],
                    reviewed_by=row['reviewed_by'],
                    given_by=row['given_by'],
                    is_vip=bool(row['is_vip']),
                    is_unregistered=bool(row['is_unregistered']),
                    is_admin_given=bool(row['is_admin_given']),
                    logs=json.loads(row['logs']) if row['logs'] else []
                )
                self.requests[req.id] = req
                self.next_request_id = max(self.next_request_id, req.id + 1)
            
            # Загрузка настроек
            cursor = await db.execute('SELECT key, value FROM settings')
            rows = await cursor.fetchall()
            for key, value in rows:
                try:
                    self.settings[key] = json.loads(value)
                except:
                    self.settings[key] = value
            
            # Загрузка выводов
            cursor = await db.execute('SELECT * FROM withdrawals')
            rows = await cursor.fetchall()
            for row in rows:
                wd = Withdrawal(
                    id=row['id'],
                    user_id=row['user_id'],
                    amount=row['amount'] or 0,
                    method=PaymentMethod(row['method']),
                    wallet=row['wallet'] or '',
                    status=PaymentStatus(row['status']) if row['status'] else PaymentStatus.PENDING,
                    created_at=row['created_at'] or '',
                    processed_at=row['processed_at'],
                    fee=row['fee'] or 0,
                    net_amount=row['net_amount'] or 0,
                    cryptobot_transfer_id=row['cryptobot_transfer_id'],
                    processed_by=row['processed_by'],
                    comment=row['comment'] or '',
                )
                self.withdrawals[wd.id] = wd
                self.next_withdrawal_id = max(self.next_withdrawal_id, wd.id + 1)
            
            # Загрузка инвойсов CryptoBot
            cursor = await db.execute('SELECT * FROM cryptobot_invoices')
            rows = await cursor.fetchall()
            for row in rows:
                inv = CryptoBotInvoice(
                    invoice_id=row['invoice_id'],
                    user_id=row['user_id'],
                    amount=row['amount'] or 0,
                    asset=row['asset'] or 'USDT',
                    status=row['status'] or 'active',
                    pay_url=row['pay_url'] or '',
                    created_at=row['created_at'] or '',
                    expires_at=row['expires_at'] or '',
                    paid_at=row['paid_at'],
                )
                # purpose/credited храним отдельно, чтобы не ломать dataclass
                inv._purpose = row.get('purpose') if isinstance(row, dict) else row['purpose']
                inv._credited = bool(row.get('credited')) if isinstance(row, dict) else bool(row['credited'])
                self.cryptobot_invoices[inv.invoice_id] = inv
            
            # Загрузка переводов CryptoBot
            cursor = await db.execute('SELECT * FROM cryptobot_transfers')
            rows = await cursor.fetchall()
            for row in rows:
                tr = CryptoBotTransfer(
                    transfer_id=row['transfer_id'],
                    user_id=row['user_id'],
                    amount=row['amount'] or 0,
                    asset=row['asset'] or 'USDT',
                    status=row['status'] or 'pending',
                    completed_at=row['completed_at'],
                    withdrawal_id=row['withdrawal_id'],
                )
                self.cryptobot_transfers[tr.transfer_id] = tr
            
            # Загрузка логов админов
            cursor = await db.execute('SELECT * FROM admin_actions')
            rows = await cursor.fetchall()
            for row in rows:
                action = AdminAction(
                    id=row['id'],
                    admin_id=row['admin_id'],
                    action_type=row['action_type'] or '',
                    target_id=row['target_id'],
                    details=json.loads(row['details']) if row['details'] else {},
                    created_at=row['created_at'] or '',
                )
                self.admin_actions.append(action)
                self.next_admin_action_id = max(self.next_admin_action_id, action.id + 1)
    
    async def save(self):
        """Сохранение всех данных"""
        async with aiosqlite.connect(self.db_path) as db:
            # Сохранение пользователей
            for user in self.users.values():
                await db.execute('''
                    INSERT OR REPLACE INTO users VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                ''', (
                    user.id, user.username, user.first_name, user.last_name,
                    user.language, user.role.value, user.level.value,
                    user.balance, user.bonus_balance, user.frozen_balance,
                    user.total_deposits, user.total_withdrawals,
                    user.total_requests, user.successful_requests, user.given_accounts,
                    user.rating, user.referrals_count, user.referrer_id,
                    user.referral_code, user.registration_date, user.last_activity,
                    int(user.is_blocked), int(user.is_verified), user.cryptobot_id,
                    json.dumps(user.settings), json.dumps(user.notifications)
                ))
            
            # Сохранение заявок
            for req in self.requests.values():
                await db.execute('''
                    INSERT OR REPLACE INTO requests VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                ''', (
                    req.id, req.user_id, req.account_type, req.phone_number,
                    req.status.value, req.code, req.password, req.worker_id,
                    req.worker_note, req.admin_note, req.user_note, req.price,
                    req.created_at, req.updated_at, req.completed_at,
                    req.reviewed_by, req.given_by, int(req.is_vip),
                    int(req.is_unregistered), int(req.is_admin_given),
                    json.dumps(req.logs)
                ))
            
            # Сохранение настроек
            for key, value in self.settings.items():
                await db.execute('''
                    INSERT OR REPLACE INTO settings (key, value, updated_at)
                    VALUES (?, ?, ?)
                ''', (key, json.dumps(value), datetime.now().isoformat()))
            
            # Сохранение выводов
            for wd in self.withdrawals.values():
                await db.execute('''
                    INSERT OR REPLACE INTO withdrawals VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                ''', (
                    wd.id, wd.user_id, wd.amount, wd.method.value, wd.wallet,
                    wd.status.value, wd.created_at, wd.processed_at, wd.fee,
                    wd.net_amount, wd.cryptobot_transfer_id, wd.processed_by, wd.comment
                ))
            
            # Сохранение инвойсов CryptoBot (с purpose/credited)
            for inv in self.cryptobot_invoices.values():
                purpose = getattr(inv, "_purpose", None)
                credited = int(bool(getattr(inv, "_credited", False)))
                await db.execute('''
                    INSERT OR REPLACE INTO cryptobot_invoices (
                        invoice_id, user_id, amount, asset, status, pay_url,
                        created_at, expires_at, paid_at, purpose, credited
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    inv.invoice_id, inv.user_id, inv.amount, inv.asset, inv.status, inv.pay_url,
                    inv.created_at, inv.expires_at, inv.paid_at, purpose, credited
                ))
            
            # Сохранение переводов CryptoBot
            for tr in self.cryptobot_transfers.values():
                await db.execute('''
                    INSERT OR REPLACE INTO cryptobot_transfers VALUES (
                        ?, ?, ?, ?, ?, ?, ?
                    )
                ''', (
                    tr.transfer_id, tr.user_id, tr.amount, tr.asset, tr.status,
                    tr.completed_at, tr.withdrawal_id
                ))
            
            # Сохранение логов админов (последние 1000, чтобы база не росла бесконечно)
            actions = list(self.admin_actions)[-1000:]
            for a in actions:
                await db.execute('''
                    INSERT OR REPLACE INTO admin_actions VALUES (
                        ?, ?, ?, ?, ?, ?
                    )
                ''', (
                    a.id, a.admin_id, a.action_type, a.target_id,
                    json.dumps(a.details), a.created_at
                ))
            
            await db.commit()
    
    # Методы для пользователей
    async def get_user(self, user_id: int) -> User:
        if user_id not in self.users:
            self.users[user_id] = User(id=user_id)
        return self.users[user_id]
    
    async def get_user_by_username(self, username: str) -> Optional[User]:
        username = username.lower().replace('@', '')
        for user in self.users.values():
            if user.username and user.username.lower() == username:
                return user
        return None
    
    async def get_user_by_referral(self, referral_code: str) -> Optional[User]:
        referral_code = (referral_code or "").strip().upper()
        if not referral_code:
            return None
        for user in self.users.values():
            if (user.referral_code or "").upper() == referral_code:
                return user
        return None
    
    async def update_user_activity(self, user_id: int):
        user = await self.get_user(user_id)
        user.last_activity = datetime.now().isoformat()
    
    async def add_balance(self, user_id: int, amount: float, is_bonus: bool = False,
                         admin_id: Optional[int] = None, description: str = ""):
        user = await self.get_user(user_id)
        if is_bonus:
            user.bonus_balance += amount
        else:
            user.balance += amount
            user.total_deposits += amount
        
        if admin_id:
            await self.log_admin_action(
                admin_id=admin_id,
                action_type="add_balance",
                target_id=user_id,
                details={"amount": amount, "is_bonus": is_bonus, "description": description}
            )
    
    async def remove_balance(self, user_id: int, amount: float) -> bool:
        user = await self.get_user(user_id)
        if user.balance >= amount:
            user.balance -= amount
            return True
        return False
    
    # Методы для заявок
    async def create_request(self, user_id: int, account_type: str, 
                            phone: str, **kwargs) -> AccountRequest:
        req = AccountRequest(
            id=self.next_request_id,
            user_id=user_id,
            account_type=account_type,
            phone_number=phone,
            **kwargs
        )
        self.requests[req.id] = req
        self.next_request_id += 1
        
        user = await self.get_user(user_id)
        user.total_requests += 1
        
        return req
    
    async def get_request(self, request_id: int) -> Optional[AccountRequest]:
        return self.requests.get(request_id)
    
    async def get_pending_requests(self) -> List[AccountRequest]:
        return [r for r in self.requests.values() if r.status == RequestStatus.PENDING]
    
    # Методы для выводов
    async def create_withdrawal(self, user_id: int, amount: float, 
                               method: PaymentMethod, wallet: str) -> Optional[Withdrawal]:
        if amount < self.settings['min_withdrawal'] or amount > self.settings['max_withdrawal']:
            return None
        
        user = await self.get_user(user_id)
        if user.balance < amount:
            return None
        
        fee = amount * self.settings['withdrawal_fee']
        net_amount = amount - fee
        
        wd = Withdrawal(
            id=self.next_withdrawal_id,
            user_id=user_id,
            amount=amount,
            method=method,
            wallet=wallet,
            fee=fee,
            net_amount=net_amount
        )
        
        if await self.remove_balance(user_id, amount):
            user.frozen_balance += amount
            self.withdrawals[wd.id] = wd
            self.next_withdrawal_id += 1
            return wd
        
        return None
    
    async def get_withdrawal(self, withdrawal_id: int) -> Optional[Withdrawal]:
        return self.withdrawals.get(withdrawal_id)
    
    async def get_pending_withdrawals(self) -> List[Withdrawal]:
        return [w for w in self.withdrawals.values() if w.status == PaymentStatus.PENDING]
    
    async def process_withdrawal(self, withdrawal_id: int, admin_id: int, 
                                status: PaymentStatus, comment: str = "") -> bool:
        wd = await self.get_withdrawal(withdrawal_id)
        if not wd:
            return False
        
        wd.status = status
        wd.processed_at = datetime.now().isoformat()
        wd.processed_by = admin_id
        wd.comment = comment
        
        if status == PaymentStatus.COMPLETED:
            user = await self.get_user(wd.user_id)
            user.frozen_balance -= wd.amount
            user.total_withdrawals += wd.net_amount
            self.settings['treasury_balance'] += wd.fee
        elif status in [PaymentStatus.FAILED, PaymentStatus.CANCELLED]:
            user = await self.get_user(wd.user_id)
            user.frozen_balance -= wd.amount
            user.balance += wd.amount
        
        await self.log_admin_action(
            admin_id=admin_id,
            action_type="process_withdrawal",
            target_id=withdrawal_id,
            details={"status": status.value, "comment": comment}
        )
        
        return True
    
    # Методы для CryptoBot
    async def save_cryptobot_invoice(self, invoice: Dict, user_id: int) -> CryptoBotInvoice:
        inv = CryptoBotInvoice(
            invoice_id=invoice['invoice_id'],
            user_id=user_id,
            amount=float(invoice['amount']),
            asset=invoice['asset'],
            status=invoice['status'],
            pay_url=invoice['pay_url'],
            created_at=invoice['created_at'],
            expires_at=invoice['expires_at']
        )
        self.cryptobot_invoices[inv.invoice_id] = inv
        return inv
    
    async def update_invoice_status(self, invoice_id: int, status: str):
        if invoice_id in self.cryptobot_invoices:
            self.cryptobot_invoices[invoice_id].status = status
            if status == 'paid':
                self.cryptobot_invoices[invoice_id].paid_at = datetime.now().isoformat()
    
    async def mark_invoice_credited(self, invoice_id: int):
        if invoice_id in self.cryptobot_invoices:
            self.cryptobot_invoices[invoice_id]._credited = True
    
    async def save_cryptobot_transfer(self, transfer: Dict, user_id: int, withdrawal_id: Optional[int] = None) -> CryptoBotTransfer:
        tr = CryptoBotTransfer(
            transfer_id=transfer['transfer_id'],
            user_id=user_id,
            amount=float(transfer['amount']),
            asset=transfer['asset'],
            status=transfer['status'],
            completed_at=transfer.get('completed_at'),
            withdrawal_id=withdrawal_id
        )
        self.cryptobot_transfers[tr.transfer_id] = tr
        return tr
    
    # Методы для админов
    async def log_admin_action(self, admin_id: int, action_type: str,
                              target_id: Optional[int] = None,
                              details: Dict = None):
        action = AdminAction(
            id=self.next_admin_action_id,
            admin_id=admin_id,
            action_type=action_type,
            target_id=target_id,
            details=details or {}
        )
        self.admin_actions.append(action)
        self.next_admin_action_id += 1
        logger.info(f"Admin {admin_id}: {action_type} - {details}")
    
    async def get_admin_actions(self, admin_id: Optional[int] = None,
                               limit: int = 100) -> List[AdminAction]:
        actions = list(self.admin_actions)
        if admin_id:
            actions = [a for a in actions if a.admin_id == admin_id]
        actions.sort(key=lambda x: x.created_at, reverse=True)
        return actions[:limit]
    
    # Статистика
    async def get_stats(self) -> Dict[str, Any]:
        today = datetime.now().date().isoformat()
        
        return {
            'users': len(self.users),
            'users_today': len([u for u in self.users.values() if u.registration_date.startswith(today)]),
            'requests': len(self.requests),
            'requests_pending': len([r for r in self.requests.values() if r.status == RequestStatus.PENDING]),
            'requests_today': len([r for r in self.requests.values() if r.created_at.startswith(today)]),
            'withdrawals_pending': len([w for w in self.withdrawals.values() if w.status == PaymentStatus.PENDING]),
            'treasury': self.settings['treasury_balance'],
            'cryptobot_enabled': self.settings['cryptobot_enabled'],
            'admins': len([u for u in self.users.values() if u.role in [UserRole.ADMIN, UserRole.SUPER_ADMIN]]),
        }

# Создаем глобальный экземпляр БД
db = Database()

# ============================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================

def format_number(number: float) -> str:
    return f"{number:,.2f}".replace(",", " ")

def format_phone(phone: str) -> str:
    cleaned = re.sub(r'[^\d+]', '', phone)
    if cleaned.startswith('8') and len(cleaned) == 11:
        cleaned = '+7' + cleaned[1:]
    elif not cleaned.startswith('+') and len(cleaned) == 10:
        cleaned = '+7' + cleaned
    return cleaned

def mask_phone(phone: str) -> str:
    if len(phone) >= 10:
        return phone[:4] + '*' * (len(phone) - 7) + phone[-3:]
    return phone

def format_time(dt_str: str) -> str:
    try:
        dt = datetime.fromisoformat(dt_str)
        return dt.strftime("%H:%M")
    except:
        return datetime.now().strftime("%H:%M")

async def check_admin(user_id: int) -> bool:
    """Проверка, является ли пользователь администратором"""
    user = await db.get_user(user_id)
    return user.role in [UserRole.ADMIN, UserRole.SUPER_ADMIN] or user_id in ADMIN_IDS

async def safe_edit(message: Message, text: str, reply_markup=None, **kwargs):
    try:
        await message.edit_text(text, reply_markup=reply_markup, **kwargs)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            logger.error(f"Edit error: {e}")
    except Exception as e:
        logger.error(f"Edit error: {e}")

async def safe_send(bot: Bot, user_id: int, text: str, **kwargs) -> Optional[Message]:
    try:
        return await bot.send_message(user_id, text, **kwargs)
    except TelegramForbiddenError:
        return None
    except Exception as e:
        logger.error(f"Send error: {e}")
        return None

# ============================================
# FSM СОСТОЯНИЯ
# ============================================

class WithdrawStates(StatesGroup):
    waiting_amount = State()
    waiting_cryptobot_confirm = State()

class AdminWithdrawStates(StatesGroup):
    waiting_withdrawal_id = State()
    waiting_comment = State()

# ============================================
# ПРОВЕРКА АДМИНА (MIDDLEWARE)
# ============================================

async def admin_only_middleware(handler, event, data):
    """Middleware для проверки прав администратора"""
    user_id = None
    if isinstance(event, Message):
        user_id = event.from_user.id
    elif isinstance(event, CallbackQuery):
        user_id = event.from_user.id
    
    if user_id:
        if not await check_admin(user_id):
            if isinstance(event, CallbackQuery):
                await event.answer("❌ Эта функция доступна только администраторам!", show_alert=True)
            else:
                await event.answer("❌ У вас нет доступа к этой функции.")
            return
    
    return await handler(event, data)

# ============================================
# КЛАВИАТУРЫ
# ============================================

def get_main_keyboard(is_admin: bool = False) -> ReplyKeyboardMarkup:
    """Главная клавиатура"""
    builder = ReplyKeyboardBuilder()
    
    buttons = [
        ["💼 Продать аккаунт", "📋 Мои заявки"],
        ["👤 Профиль", "💰 Баланс"],
        ["💸 Вывести", "📥 Пополнить"],
        ["👥 Рефералы", "🎁 Бонусы"],
        ["⭐️ Отзывы", "ℹ️ О боте"],
        ["🆘 Поддержка"]
    ]
    
    for row in buttons:
        builder.row(*[KeyboardButton(text=btn) for btn in row])
    
    if is_admin:
        builder.row(KeyboardButton(text="⚙️ АДМИН ПАНЕЛЬ"))
    
    builder.row(KeyboardButton(text="🏠 Главное меню"))
    
    return builder.as_markup(resize_keyboard=True)

def get_admin_panel_keyboard() -> InlineKeyboardMarkup:
    """Админ панель в стиле плиток (как на скрине)"""
    builder = InlineKeyboardBuilder()

    builder.row(
        InlineKeyboardButton(text="💰 Типы аккаунтов", callback_data="admin_acc_types"),
        InlineKeyboardButton(text="👑 Администраторы", callback_data="admin_admins"),
        width=2,
    )
    builder.row(
        InlineKeyboardButton(text="🔧 Параметры", callback_data="admin_params"),
        InlineKeyboardButton(text="🛠 Техобслуживание", callback_data="admin_maintenance"),
        width=2,
    )
    builder.row(
        InlineKeyboardButton(text="🛑 Стоп-приём", callback_data="admin_stop_toggle"),
        InlineKeyboardButton(text="🎟 Промокоды", callback_data="admin_promocodes"),
        width=2,
    )
    builder.row(
        InlineKeyboardButton(text="⛔ Чёрный список", callback_data="admin_blacklist"),
        InlineKeyboardButton(text="⚡ Споры", callback_data="admin_disputes"),
        width=2,
    )
    builder.row(
        InlineKeyboardButton(text="📤 Экспорт CSV", callback_data="admin_export_csv"),
        InlineKeyboardButton(text="💾 Бэкап БД", callback_data="admin_backup_db"),
        width=2,
    )
    builder.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu"), width=1)

    return builder.as_markup()

def get_main_inline_keyboard(is_admin: bool = False) -> InlineKeyboardMarkup:
    """Главное меню inline-кнопками (без нижней клавиатуры)"""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="💼 Продать аккаунт", callback_data="nav_sell"),
        InlineKeyboardButton(text="📋 Мои заявки", callback_data="nav_my_requests"),
        width=2,
    )
    builder.row(
        InlineKeyboardButton(text="👤 Профиль", callback_data="nav_profile"),
        InlineKeyboardButton(text="💰 Баланс", callback_data="nav_balance"),
        width=2,
    )
    builder.row(
        InlineKeyboardButton(text="💸 Вывести", callback_data="nav_withdraw"),
        InlineKeyboardButton(text="📥 Пополнить", callback_data="nav_deposit"),
        width=2,
    )
    builder.row(
        InlineKeyboardButton(text="👥 Рефералы", callback_data="nav_referrals"),
        InlineKeyboardButton(text="🎁 Бонусы", callback_data="nav_bonuses"),
        width=2,
    )
    builder.row(
        InlineKeyboardButton(text="⭐️ Отзывы", callback_data="nav_reviews"),
        InlineKeyboardButton(text="ℹ️ О боте", callback_data="nav_about"),
        width=2,
    )
    builder.row(InlineKeyboardButton(text="🆘 Поддержка", callback_data="nav_support"), width=1)

    if is_admin:
        builder.row(InlineKeyboardButton(text="⚙️ Админка", callback_data="admin_panel"), width=1)

    return builder.as_markup()

def get_withdrawal_admin_keyboard(withdrawal_id: int) -> InlineKeyboardMarkup:
    """Клавиатура для обработки вывода админом"""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Подтвердить вывод", callback_data=f"admin_confirm_withdraw_{withdrawal_id}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_reject_withdraw_{withdrawal_id}"),
        width=2
    )
    builder.row(
        InlineKeyboardButton(text="📝 Добавить комментарий", callback_data=f"admin_withdraw_comment_{withdrawal_id}"),
        width=1
    )
    builder.row(
        InlineKeyboardButton(text="« Назад", callback_data="admin_pending_withdrawals"),
        width=1
    )
    return builder.as_markup()

def get_back_keyboard(callback: str = "admin_panel") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="« Назад", callback_data=callback))
    return builder.as_markup()

# ============================================
# ИНИЦИАЛИЗАЦИЯ БОТА
# ============================================

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()

# ============================================
# ОБРАБОТЧИКИ КОМАНД
# ============================================

@router.message(Command("start"))
async def cmd_start(message: Message, command: CommandObject):
    user_id = message.from_user.id
    user = await db.get_user(user_id)
    
    user.first_name = message.from_user.first_name or ""
    user.last_name = message.from_user.last_name or ""
    user.username = message.from_user.username or ""
    
    # Автоматически назначаем админа если ID в списке
    if user_id in ADMIN_IDS and user.role != UserRole.SUPER_ADMIN:
        user.role = UserRole.SUPER_ADMIN
    
    # Реферальная система
    args = command.args
    if args and args.startswith("ref_"):
        ref_code = args[4:]
        referrer = await db.get_user_by_referral(ref_code)
        if referrer and referrer.id != user_id:
            bonus = db.settings['bonus_on_referral']
            await db.add_balance(referrer.id, bonus, is_bonus=True)
            await safe_send(bot, referrer.id, f"🎉 Новый реферал! +{bonus} USDT")
    
    # Бонус за регистрацию
    if user.registration_date == user.last_activity:
        bonus = db.settings['bonus_on_register']
        user.bonus_balance += bonus
    
    await db.update_user_activity(user_id)
    
    await show_main_menu(message)

# ============================================
# ПОПОЛНЕНИЕ (BALANCE / KAZNA) ЧЕРЕЗ CRYPTOBOT
# ============================================

class DepositStates(StatesGroup):
    waiting_amount = State()

class AdminAccountTypesStates(StatesGroup):
    waiting_new_label = State()

def _is_money_amount(text: str) -> Optional[float]:
    try:
        value = float(text.replace(",", ".").strip())
        if value <= 0:
            return None
        return round(value, 2)
    except Exception:
        return None

async def _create_deposit_invoice(user_id: int, amount: float, purpose: str) -> Optional[CryptoBotInvoice]:
    if not cryptobot:
        return None
    description = f"deposit:{purpose}:{user_id}"
    inv_raw = await cryptobot.create_invoice(
        amount=amount,
        asset="USDT",
        description=description,
        expires_in_minutes=60,
    )
    if not inv_raw:
        return None
    inv = await db.save_cryptobot_invoice(inv_raw, user_id=user_id)
    inv._purpose = purpose
    inv._credited = False
    return inv

async def _try_credit_paid_invoice(invoice_id: int) -> Tuple[bool, str]:
    """
    Возвращает: (credited_now, status_message)
    credited_now=True только если было начисление прямо сейчас.
    """
    inv = db.cryptobot_invoices.get(invoice_id)
    if not inv:
        return False, "❌ Инвойс не найден."

    already = bool(getattr(inv, "_credited", False))
    if already:
        return False, "✅ Уже зачислено ранее."

    status = await cryptobot.get_invoice_status(invoice_id) if cryptobot else None
    if not status:
        return False, "❌ Не удалось получить статус. Попробуйте позже."

    await db.update_invoice_status(invoice_id, status)

    if status != "paid":
        if status in ("expired", "cancelled"):
            return False, f"❌ Инвойс {status}."
        return False, f"⏳ Статус: {status}. Оплатите и нажмите «Проверить оплату»."

    purpose = getattr(inv, "_purpose", "balance")
    amount = float(inv.amount)

    if purpose == "treasury":
        db.settings["treasury_balance"] = float(db.settings.get("treasury_balance", 0)) + amount
    else:
        await db.add_balance(inv.user_id, amount, is_bonus=False, description=f"CryptoBot invoice {invoice_id}")

    await db.mark_invoice_credited(invoice_id)
    await db.save()
    return True, "✅ Оплата подтверждена, средства зачислены."

@router.message(F.text == "📥 Пополнить")
async def deposit_entry(message: Message, state: FSMContext):
    if not cryptobot or not db.settings.get("cryptobot_enabled"):
        await message.answer("❌ Пополнение через CryptoBot сейчас недоступно.")
        return

    is_admin = await check_admin(message.from_user.id)
    if is_admin:
        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(text="🏦 Пополнить казну", callback_data="deposit_choose_treasury"),
            InlineKeyboardButton(text="👤 Пополнить баланс", callback_data="deposit_choose_balance"),
            width=2,
        )
        await message.answer("Выберите, что пополняем.", reply_markup=builder.as_markup())
        return

    await state.update_data(deposit_purpose="balance")
    await message.answer(
        "📥 Введите сумму пополнения в USDT (например 10.5).",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="« Отмена")]],
            resize_keyboard=True,
        ),
    )
    await state.set_state(DepositStates.waiting_amount)

@router.callback_query(F.data.in_(["deposit_choose_treasury", "deposit_choose_balance"]))
async def deposit_choose(callback: CallbackQuery, state: FSMContext):
    if not await check_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return

    purpose = "treasury" if callback.data.endswith("treasury") else "balance"
    await state.update_data(deposit_purpose=purpose)
    await callback.message.edit_text(
        "📥 Введите сумму пополнения в USDT (например 10.5).",
        reply_markup=None,
    )
    await callback.answer()
    await state.set_state(DepositStates.waiting_amount)

@router.message(DepositStates.waiting_amount)
async def deposit_amount(message: Message, state: FSMContext):
    if message.text == "« Отмена":
        await state.clear()
        await show_main_menu(message)
        return

    amount = _is_money_amount(message.text or "")
    if amount is None:
        await message.answer("❌ Введите корректную сумму (например 10 или 10.5).")
        return

    data = await state.get_data()
    purpose = data.get("deposit_purpose", "balance")

    inv = await _create_deposit_invoice(message.from_user.id, amount, purpose)
    if not inv:
        await message.answer("❌ Не удалось создать инвойс. Проверь CRYPTOBOT_API_KEY и попробуй позже.")
        return

    await db.save()

    btn_text = "🏦 Оплатить пополнение казны" if purpose == "treasury" else "👤 Оплатить пополнение баланса"
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text=btn_text, url=inv.pay_url), width=1)
    builder.row(
        InlineKeyboardButton(text="✅ Проверить оплату", callback_data=f"deposit_check_{inv.invoice_id}"),
        InlineKeyboardButton(text="❌ Отмена", callback_data="deposit_cancel"),
        width=2,
    )

    await message.answer(
        f"🧾 Инвойс создан.\n"
        f"Сумма: {amount} USDT\n"
        f"ID: `{inv.invoice_id}`\n\n"
        f"Нажмите «Оплатить», затем «Проверить оплату».",
        reply_markup=builder.as_markup(),
        parse_mode="Markdown",
    )

@router.callback_query(F.data == "deposit_cancel")
async def deposit_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("❌ Пополнение отменено.")
    await callback.answer()

@router.callback_query(F.data.startswith("deposit_check_"))
async def deposit_check(callback: CallbackQuery):
    if not cryptobot:
        await callback.answer("❌ CryptoBot не настроен", show_alert=True)
        return

    try:
        invoice_id = int(callback.data.split("_")[2])
    except Exception:
        await callback.answer("❌ Некорректный ID", show_alert=True)
        return

    credited_now, msg = await _try_credit_paid_invoice(invoice_id)
    await callback.answer("✅ Проверено" if credited_now else "Готово", show_alert=credited_now)

    inv = db.cryptobot_invoices.get(invoice_id)
    status = inv.status if inv else "unknown"
    await callback.message.edit_text(
        f"{msg}\n\nИнвойс #{invoice_id}\nСтатус: {status}",
        reply_markup=get_back_keyboard("main_menu"),
    )

# ============================================
# АДМИН: ТИПЫ АККАУНТОВ
# ============================================

def _get_account_types_settings() -> Dict[str, Dict[str, Any]]:
    ats = db.settings.get("account_types")
    if isinstance(ats, dict) and ats:
        return ats
    db.settings["account_types"] = {
        'tg': {'label': 'Telegram', 'enabled': True},
        'vk': {'label': 'VK', 'enabled': True},
        'ig': {'label': 'Instagram', 'enabled': True},
        'tt': {'label': 'TikTok', 'enabled': True},
        'yt': {'label': 'YouTube', 'enabled': True},
        'dc': {'label': 'Discord', 'enabled': True},
        'tw': {'label': 'Twitter', 'enabled': True},
        'fb': {'label': 'Facebook', 'enabled': True},
        'wa': {'label': 'WhatsApp', 'enabled': True},
        'other': {'label': 'Other', 'enabled': True},
    }
    return db.settings["account_types"]

def _render_account_types_text() -> str:
    ats = _get_account_types_settings()
    lines = ["💰 *Типы аккаунтов*\n"]
    for k, v in ats.items():
        enabled = bool(v.get("enabled", True))
        label = v.get("label", k)
        lines.append(f"{'✅' if enabled else '❌'} `{k}` — {label}")
    lines.append("\nНажмите на тип, чтобы включить/выключить. Долгое редактирование названий — через «Переименовать».")
    return "\n".join(lines)

def _account_types_keyboard() -> InlineKeyboardMarkup:
    ats = _get_account_types_settings()
    builder = InlineKeyboardBuilder()
    for k, v in ats.items():
        enabled = bool(v.get("enabled", True))
        label = v.get("label", k)
        builder.row(
            InlineKeyboardButton(
                text=f"{'✅' if enabled else '❌'} {label}",
                callback_data=f"admin_acc_toggle_{k}",
            ),
            InlineKeyboardButton(
                text="✏️",
                callback_data=f"admin_acc_rename_{k}",
            ),
            width=2,
        )
    builder.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel"), width=1)
    return builder.as_markup()

@router.callback_query(F.data == "admin_acc_types")
async def admin_acc_types(callback: CallbackQuery):
    if not await check_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    await callback.message.edit_text(
        _render_account_types_text(),
        reply_markup=_account_types_keyboard(),
        parse_mode="Markdown",
    )
    await callback.answer()

@router.callback_query(F.data.startswith("admin_acc_toggle_"))
async def admin_acc_toggle(callback: CallbackQuery):
    if not await check_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    key = callback.data.split("_")[-1]
    ats = _get_account_types_settings()
    if key not in ats:
        await callback.answer("❌ Не найдено", show_alert=True)
        return
    ats[key]["enabled"] = not bool(ats[key].get("enabled", True))
    db.settings["account_types"] = ats
    await db.save()
    await callback.answer("Готово")
    await admin_acc_types(callback)

@router.callback_query(F.data.startswith("admin_acc_rename_"))
async def admin_acc_rename(callback: CallbackQuery, state: FSMContext):
    if not await check_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    key = callback.data.split("_")[-1]
    ats = _get_account_types_settings()
    if key not in ats:
        await callback.answer("❌ Не найдено", show_alert=True)
        return
    await state.update_data(acc_key=key)
    await state.set_state(AdminAccountTypesStates.waiting_new_label)
    await callback.message.edit_text(
        f"✏️ Введите новое название для `{key}` (текущее: {ats[key].get('label', key)}).",
        reply_markup=get_back_keyboard("admin_acc_types"),
        parse_mode="Markdown",
    )
    await callback.answer()

@router.message(AdminAccountTypesStates.waiting_new_label)
async def admin_acc_rename_apply(message: Message, state: FSMContext):
    if not await check_admin(message.from_user.id):
        await message.answer("❌ Нет доступа")
        await state.clear()
        return
    data = await state.get_data()
    key = data.get("acc_key")
    new_label = (message.text or "").strip()
    if not key or not new_label:
        await message.answer("❌ Введите название текстом.")
        return
    if len(new_label) > 24:
        await message.answer("❌ Слишком длинно (макс 24 символа).")
        return
    ats = _get_account_types_settings()
    if key not in ats:
        await message.answer("❌ Тип не найден.")
        await state.clear()
        return
    ats[key]["label"] = new_label
    db.settings["account_types"] = ats
    await db.save()
    await state.clear()
    await message.answer("✅ Сохранено.")

@router.message(Command("cryptobot"))
async def cmd_cryptobot(message: Message):
    """Привязка CryptoBot ID для выплат"""
    args = message.text.split()
    if len(args) != 2:
        await message.answer(
            "❌ Использование: /cryptobot <ID>\n\n"
            "ID можно получить в боте @send\n"
            "Отправьте команду /start боту @send и скопируйте ваш ID"
        )
        return
    
    try:
        cryptobot_id = int(args[1])
        user = await db.get_user(message.from_user.id)
        user.cryptobot_id = cryptobot_id
        
        await message.answer(
            f"✅ CryptoBot ID успешно сохранен!\n"
            f"ID: `{cryptobot_id}`\n\n"
            f"Теперь вы можете выводить средства через CryptoBot."
        )
    except ValueError:
        await message.answer("❌ Неверный формат ID. ID должен быть числом.")

@router.message(Command("admin"))
async def cmd_admin(message: Message):
    """Вход в админ-панель через команду"""
    if not await check_admin(message.from_user.id):
        await message.answer("❌ Эта команда доступна только администраторам!")
        return
    await admin_panel(message)

# ============================================
# ГЛАВНОЕ МЕНЮ
# ============================================

async def show_main_menu(message: Message):
    user = await db.get_user(message.from_user.id)
    user.level = user.get_level()
    
    pending = len([r for r in db.requests.values() 
                  if r.user_id == user.id and r.status == RequestStatus.PENDING])
    
    text = f"👋 *Главное меню*\n\n"
    text += f"💰 Баланс: `{format_number(user.balance)} USDT`\n"
    text += f"🎁 Бонус: `{format_number(user.bonus_balance)} USDT`\n"
    text += f"📊 В очереди: {pending}\n"
    text += f"\n🕐 {datetime.now().strftime('%H:%M')}"
    
    is_admin = await check_admin(message.from_user.id)
    await message.answer(
        text,
        reply_markup=get_main_inline_keyboard(is_admin),
        parse_mode="Markdown",
    )

@router.message(F.text == "🏠 Главное меню")
async def text_main_menu(message: Message):
    await show_main_menu(message)

@router.callback_query(F.data == "main_menu")
async def cb_main_menu(callback: CallbackQuery):
    await callback.answer()
    await show_main_menu(callback.message)

@router.callback_query(F.data.startswith("nav_"))
async def nav_router(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    key = callback.data

    # Эти пункты уже реализованы текстовыми кнопками — переиспользуем те же функции
    if key == "nav_withdraw":
        # эмулируем вызов логики вывода
        await withdraw_start(callback.message, state)
        return
    if key == "nav_deposit":
        await deposit_entry(callback.message, state)
        return

    # Остальные функции пока не реализованы в этом файле
    await callback.message.edit_text(
        "⏳ Раздел в разработке.",
        reply_markup=get_back_keyboard("main_menu"),
    )

# ============================================
# ВЫВОД СРЕДСТВ ЧЕРЕЗ CRYPTOBOT
# ============================================

@router.message(F.text == "💸 Вывести")
async def withdraw_start(message: Message, state: FSMContext):
    user = await db.get_user(message.from_user.id)
    
    if not db.settings.get('cryptobot_enabled') or not cryptobot:
        await message.answer("❌ Вывод через CryptoBot временно недоступен")
        return
    
    if not user.cryptobot_id:
        await message.answer(
            "❌ Сначала привяжите ваш CryptoBot ID!\n\n"
            "1. Напишите боту @send\n"
            "2. Отправьте команду /start\n"
            "3. Скопируйте ваш ID (число)\n"
            "4. Отправьте /cryptobot <ваш ID>"
        )
        return
    
    if user.balance < db.settings['min_withdrawal']:
        await message.answer(
            f"❌ Минимальная сумма вывода: {db.settings['min_withdrawal']} USDT\n"
            f"Ваш баланс: {format_number(user.balance)} USDT"
        )
        return
    
    text = (
        f"💸 *Вывод через CryptoBot*\n\n"
        f"💰 Доступно: `{format_number(user.balance)} USDT`\n"
        f"📊 Комиссия: {db.settings['withdrawal_fee']*100}%\n"
        f"💎 Ваш CryptoBot ID: `{user.cryptobot_id}`\n\n"
        f"Введите сумму для вывода:"
    )
    
    await message.answer(text, reply_markup=ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="« Отмена")]],
        resize_keyboard=True
    ), parse_mode="Markdown")
    
    await state.set_state(WithdrawStates.waiting_amount)

@router.message(WithdrawStates.waiting_amount)
async def withdraw_amount(message: Message, state: FSMContext):
    if message.text == "« Отмена":
        await state.clear()
        await show_main_menu(message)
        return
    
    try:
        amount = float(message.text.strip())
        user = await db.get_user(message.from_user.id)
        
        if amount < db.settings['min_withdrawal']:
            await message.answer(f"❌ Минимальная сумма: {db.settings['min_withdrawal']} USDT")
            return
        if amount > db.settings['max_withdrawal']:
            await message.answer(f"❌ Максимальная сумма: {db.settings['max_withdrawal']} USDT")
            return
        if amount > user.balance:
            await message.answer("❌ Недостаточно средств")
            return
    except ValueError:
        await message.answer("❌ Введите число")
        return
    
    fee = amount * db.settings['withdrawal_fee']
    net_amount = amount - fee
    
    await state.update_data(amount=amount)
    
    text = (
        f"💸 *Подтверждение вывода*\n\n"
        f"Сумма: `{amount} USDT`\n"
        f"Комиссия: `{fee:.2f} USDT`\n"
        f"К получению: `{net_amount:.2f} USDT`\n"
        f"Способ: CryptoBot\n"
        f"Получатель ID: `{user.cryptobot_id}`\n\n"
        f"✅ Подтверждаете?"
    )
    
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Подтвердить", callback_data="confirm_cryptobot_withdraw"),
        InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_withdraw"),
        width=2
    )
    
    await message.answer(text, reply_markup=builder.as_markup(), parse_mode="Markdown")
    await state.set_state(WithdrawStates.waiting_cryptobot_confirm)

@router.callback_query(WithdrawStates.waiting_cryptobot_confirm, F.data == "confirm_cryptobot_withdraw")
async def withdraw_cryptobot_confirm(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    amount = data['amount']
    user = await db.get_user(callback.from_user.id)
    
    # Проверяем баланс еще раз
    if user.balance < amount:
        await callback.message.edit_text("❌ Недостаточно средств. Баланс изменился.")
        await state.clear()
        await callback.answer()
        return
    
    # Создаем заявку на вывод
    wd = await db.create_withdrawal(
        user_id=callback.from_user.id,
        amount=amount,
        method=PaymentMethod.CRYPTOBOT,
        wallet=str(user.cryptobot_id)
    )
    
    if not wd:
        await callback.message.edit_text("❌ Ошибка создания заявки")
        await state.clear()
        await callback.answer()
        return
    
    # Если включен автоматический вывод - отправляем сразу
    if db.settings.get('auto_withdraw_enabled') and cryptobot:
        try:
            transfer = await cryptobot.transfer(
                user_id=user.cryptobot_id,
                amount=wd.net_amount,
                asset="USDT"
            )
            
            if transfer:
                wd.status = PaymentStatus.COMPLETED
                wd.processed_at = datetime.now().isoformat()
                wd.cryptobot_transfer_id = transfer['transfer_id']
                
                user.frozen_balance -= wd.amount
                user.total_withdrawals += wd.net_amount
                db.settings['treasury_balance'] += wd.fee
                
                await db.save_cryptobot_transfer(transfer, user.id, wd.id)
                
                await callback.message.edit_text(
                    f"✅ *Вывод успешно выполнен!*\n\n"
                    f"📝 Заявка #{wd.id}\n"
                    f"💰 Сумма: {amount} USDT\n"
                    f"💎 Получено: {wd.net_amount:.2f} USDT\n"
                    f"🆔 Transfer ID: {transfer['transfer_id']}\n\n"
                    f"Средства зачислены на ваш CryptoBot кошелек.",
                    reply_markup=get_back_keyboard("main_menu"),
                    parse_mode="Markdown"
                )
                
                await db.log_admin_action(
                    admin_id=0,
                    action_type="auto_withdraw",
                    target_id=wd.id,
                    details={"amount": amount, "user_id": user.id}
                )
            else:
                # Если автоматический вывод не удался, оставляем заявку на ручную обработку
                await callback.message.edit_text(
                    f"✅ *Заявка на вывод #{wd.id} создана!*\n\n"
                    f"Автоматический вывод временно недоступен.\n"
                    f"Заявка передана администратору для ручной обработки.",
                    reply_markup=get_back_keyboard("main_menu"),
                    parse_mode="Markdown"
                )
                
                # Уведомляем админов
                for admin_id in ADMIN_IDS:
                    await safe_send(
                        bot, admin_id,
                        f"💳 Новая заявка на вывод #{wd.id}\n"
                        f"👤 {user.mention}\n"
                        f"💰 {amount} USDT\n"
                        f"💎 Получит: {wd.net_amount:.2f} USDT"
                    )
        except Exception as e:
            logger.error(f"Auto withdraw error: {e}")
            await callback.message.edit_text(
                f"✅ *Заявка на вывод #{wd.id} создана!*\n\n"
                f"Произошла ошибка при автоматическом выводе.\n"
                f"Заявка передана администратору для ручной обработки.",
                reply_markup=get_back_keyboard("main_menu"),
                parse_mode="Markdown"
            )
    else:
        await callback.message.edit_text(
            f"✅ *Заявка на вывод #{wd.id} создана!*\n\n"
            f"Ожидайте обработки администратором.",
            reply_markup=get_back_keyboard("main_menu"),
            parse_mode="Markdown"
        )
        
        # Уведомляем админов
        for admin_id in ADMIN_IDS:
            await safe_send(
                bot, admin_id,
                f"💳 Новая заявка на вывод #{wd.id}\n"
                f"👤 {user.mention}\n"
                f"💰 {amount} USDT"
            )
    
    await state.clear()
    await callback.answer()

@router.callback_query(F.data == "cancel_withdraw")
async def withdraw_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("❌ Вывод отменен")
    await callback.answer()

# ============================================
# АДМИН ПАНЕЛЬ (ТОЛЬКО ДЛЯ АДМИНОВ)
# ============================================

@router.message(F.text == "⚙️ АДМИН ПАНЕЛЬ")
async def admin_panel(message: Message):
    """Главная админ панель - только для админов"""
    if not await check_admin(message.from_user.id):
        await message.answer("❌ Эта функция доступна только администраторам!")
        return
    
    user = await db.get_user(message.from_user.id)
    # Блок "Настройки" как на скрине
    tech = "вкл" if db.settings.get("test_mode") else "выкл"
    stop = "вкл" if db.settings.get("stop_accepting") else "выкл"
    min_wd = float(db.settings.get("min_withdrawal", 0))
    fee = float(db.settings.get("withdrawal_fee", 0)) * 100

    text = "⚙️ *Настройки*\n\n"
    text += f"🧪 Тех. режим: *{tech}*\n"
    text += f"🛑 Стоп-приём: *{stop}*\n"
    text += f"🏦 Мин. вывод: *{min_wd} USDT*\n"
    text += f"📊 Комиссия: *{fee:.1f}%*\n\n"
    text += f"🕐 {datetime.now().strftime('%d.%m.%Y %H:%M')}"

    await message.answer(text, reply_markup=get_admin_panel_keyboard(), parse_mode="Markdown")

@router.callback_query(F.data == "admin_panel")
async def admin_panel_cb(callback: CallbackQuery):
    if not await check_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    await callback.answer()
    await admin_panel(callback.message)

@router.callback_query(F.data == "admin_refresh")
async def admin_refresh(callback: CallbackQuery):
    """Обновление админ панели"""
    if not await check_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    await callback.answer("🔄 Панель обновлена")
    await admin_panel(callback.message)

# ============================================
# АДМИН-ПАНЕЛЬ: ПЛИТКИ (КАК НА СКРИНЕ)
# ============================================

@router.callback_query(F.data == "admin_stop_toggle")
async def admin_stop_toggle(callback: CallbackQuery):
    if not await check_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    db.settings["stop_accepting"] = not bool(db.settings.get("stop_accepting"))
    await db.save()
    await callback.answer("Готово", show_alert=False)
    await admin_panel(callback.message)

@router.callback_query(F.data == "admin_maintenance")
async def admin_maintenance(callback: CallbackQuery):
    if not await check_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    # Быстрые тумблеры: тест-режим / maintenance
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text=f"🧪 Тест-режим: {'вкл' if db.settings.get('test_mode') else 'выкл'}",
            callback_data="admin_toggle_testmode",
        ),
        InlineKeyboardButton(
            text=f"🛠 Техобслуживание: {'вкл' if db.settings.get('maintenance_mode') else 'выкл'}",
            callback_data="admin_toggle_maint",
        ),
        width=1,
    )
    builder.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel"), width=1)
    await callback.message.edit_text("🛠 *Техобслуживание*\n\nВыберите действие.", reply_markup=builder.as_markup(), parse_mode="Markdown")
    await callback.answer()

@router.callback_query(F.data == "admin_toggle_testmode")
async def admin_toggle_testmode(callback: CallbackQuery):
    if not await check_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    db.settings["test_mode"] = not bool(db.settings.get("test_mode"))
    await db.save()
    await callback.answer("Готово")
    await admin_maintenance(callback)

@router.callback_query(F.data == "admin_toggle_maint")
async def admin_toggle_maint(callback: CallbackQuery):
    if not await check_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    db.settings["maintenance_mode"] = not bool(db.settings.get("maintenance_mode"))
    await db.save()
    await callback.answer("Готово")
    await admin_maintenance(callback)

@router.callback_query(F.data == "admin_backup_db")
async def admin_backup_db(callback: CallbackQuery):
    if not await check_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    try:
        backup_file = f"backup_manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        async with aiofiles.open(db.db_path, 'rb') as src:
            async with aiofiles.open(backup_file, 'wb') as dst:
                await dst.write(await src.read())
        db.settings['last_backup'] = datetime.now().isoformat()
        await db.save()
        await callback.answer("✅ Бэкап создан", show_alert=True)
    except Exception as e:
        logger.error(f"Manual backup error: {e}")
        await callback.answer("❌ Ошибка бэкапа", show_alert=True)

@router.callback_query(F.data == "admin_export_csv")
async def admin_export_csv(callback: CallbackQuery):
    if not await check_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    # Пока только выгрузка пользователей
    try:
        filename = f"export_users_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        header = ["id", "username", "role", "balance", "bonus_balance", "total_deposits", "total_withdrawals", "registration_date"]
        async with aiofiles.open(filename, "w", encoding="utf-8", newline="") as f:
            await f.write(",".join(header) + "\n")
            for u in db.users.values():
                row = [
                    str(u.id),
                    (u.username or "").replace(",", " "),
                    u.role.value,
                    str(u.balance),
                    str(u.bonus_balance),
                    str(u.total_deposits),
                    str(u.total_withdrawals),
                    u.registration_date,
                ]
                await f.write(",".join(row) + "\n")
        await callback.answer("✅ CSV создан", show_alert=True)
    except Exception as e:
        logger.error(f"Export CSV error: {e}")
        await callback.answer("❌ Ошибка экспорта", show_alert=True)

@router.callback_query(F.data.in_(["admin_params", "admin_promocodes", "admin_blacklist", "admin_disputes", "admin_admins"]))
async def admin_tiles_stub(callback: CallbackQuery):
    if not await check_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    await callback.answer("⏳ Раздел в разработке", show_alert=True)

# ============================================
# УПРАВЛЕНИЕ ВЫВОДАМИ (АДМИНКА)
# ============================================

@router.callback_query(F.data == "admin_pending_withdrawals")
async def admin_pending_withdrawals(callback: CallbackQuery):
    """Список ожидающих выводов"""
    if not await check_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    pending = await db.get_pending_withdrawals()
    
    if not pending:
        await callback.message.edit_text(
            "📝 Нет заявок на вывод",
            reply_markup=get_back_keyboard("admin_panel")
        )
        await callback.answer()
        return
    
    text = "⏳ *Ожидают вывода*\n\n"
    
    for wd in pending:
        user = await db.get_user(wd.user_id)
        text += f"• #{wd.id} – {user.mention}\n"
        text += f"  💰 {wd.amount} USDT (к получению: {wd.net_amount:.2f})\n"
        text += f"  💎 {wd.method.value.upper()}\n"
        text += f"  🕐 {wd.created_at[:19]}\n\n"
    
    # Создаем клавиатуру со списком выводов
    builder = InlineKeyboardBuilder()
    for wd in pending[:10]:
        builder.row(InlineKeyboardButton(
            text=f"#{wd.id} – {wd.amount} USDT",
            callback_data=f"admin_view_withdraw_{wd.id}"
        ))
    builder.row(
        InlineKeyboardButton(text="« Назад", callback_data="admin_panel"),
        width=1
    )
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")
    await callback.answer()

@router.callback_query(F.data.startswith("admin_view_withdraw_"))
async def admin_view_withdraw(callback: CallbackQuery):
    """Просмотр конкретного вывода"""
    if not await check_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    wd_id = int(callback.data.split("_")[3])
    wd = await db.get_withdrawal(wd_id)
    
    if not wd:
        await callback.answer("❌ Вывод не найден", show_alert=True)
        return
    
    user = await db.get_user(wd.user_id)
    
    text = f"💳 *Вывод #{wd.id}*\n\n"
    text += f"👤 Пользователь: {user.mention} (`{user.id}`)\n"
    text += f"💰 Сумма: `{wd.amount} USDT`\n"
    text += f"💎 Комиссия: `{wd.fee:.2f} USDT`\n"
    text += f"💵 К выплате: `{wd.net_amount:.2f} USDT`\n"
    text += f"📊 Метод: {wd.method.value.upper()}\n"
    text += f"💳 Кошелек: `{wd.wallet}`\n"
    
    if wd.method == PaymentMethod.CRYPTOBOT and user.cryptobot_id:
        text += f"🤖 CryptoBot ID: `{user.cryptobot_id}`\n"
    
    text += f"📅 Создан: {wd.created_at[:19]}\n"
    text += f"📊 Статус: {wd.status.value}\n"
    
    if wd.comment:
        text += f"📝 Комментарий: {wd.comment}\n"
    
    await callback.message.edit_text(
        text,
        reply_markup=get_withdrawal_admin_keyboard(wd.id),
        parse_mode="Markdown"
    )
    await callback.answer()

@router.callback_query(F.data.startswith("admin_confirm_withdraw_"))
async def admin_confirm_withdraw(callback: CallbackQuery):
    """Подтверждение вывода админом"""
    if not await check_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    wd_id = int(callback.data.split("_")[3])
    wd = await db.get_withdrawal(wd_id)
    
    if not wd:
        await callback.answer("❌ Вывод не найден", show_alert=True)
        return
    
    # Если это CryptoBot и есть API - пробуем выполнить перевод
    if wd.method == PaymentMethod.CRYPTOBOT and cryptobot:
        user = await db.get_user(wd.user_id)
        
        if user.cryptobot_id:
            transfer = await cryptobot.transfer(
                user_id=user.cryptobot_id,
                amount=wd.net_amount,
                asset="USDT"
            )
            
            if transfer:
                wd.cryptobot_transfer_id = transfer['transfer_id']
                await db.process_withdrawal(
                    withdrawal_id=wd.id,
                    admin_id=callback.from_user.id,
                    status=PaymentStatus.COMPLETED,
                    comment="Автоматически через CryptoBot"
                )
                
                await db.save_cryptobot_transfer(transfer, user.id, wd.id)
                
                await callback.message.edit_text(
                    f"✅ *Вывод #{wd.id} успешно выполнен!*\n\n"
                    f"Перевод через CryptoBot выполнен.\n"
                    f"Transfer ID: {transfer['transfer_id']}",
                    reply_markup=get_back_keyboard("admin_pending_withdrawals")
                )
                
                # Уведомляем пользователя
                await safe_send(
                    bot, user.id,
                    f"✅ *Ваш вывод #{wd.id} выполнен!*\n\n"
                    f"💰 Сумма: {wd.amount} USDT\n"
                    f"💵 Получено: {wd.net_amount:.2f} USDT\n"
                    f"Средства зачислены на ваш CryptoBot кошелек.",
                    parse_mode="Markdown"
                )
                
                await callback.answer("✅ Вывод подтвержден")
                return
    
    # Если не CryptoBot или ошибка - просто подтверждаем вручную
    await db.process_withdrawal(
        withdrawal_id=wd.id,
        admin_id=callback.from_user.id,
        status=PaymentStatus.COMPLETED,
        comment="Подтверждено администратором"
    )
    
    user = await db.get_user(wd.user_id)
    await safe_send(
        bot, user.id,
        f"✅ *Ваш вывод #{wd.id} выполнен!*\n\n"
        f"💰 Сумма: {wd.amount} USDT\n"
        f"💵 Получено: {wd.net_amount:.2f} USDT",
        parse_mode="Markdown"
    )
    
    await callback.message.edit_text(
        f"✅ Вывод #{wd.id} подтвержден",
        reply_markup=get_back_keyboard("admin_pending_withdrawals")
    )
    await callback.answer("✅ Вывод подтвержден")

@router.callback_query(F.data.startswith("admin_reject_withdraw_"))
async def admin_reject_withdraw(callback: CallbackQuery, state: FSMContext):
    """Отклонение вывода"""
    if not await check_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    wd_id = int(callback.data.split("_")[3])
    await state.update_data(withdrawal_id=wd_id)
    
    await callback.message.edit_text(
        "📝 Введите причину отклонения (или отправьте '-' для отклонения без комментария):",
        reply_markup=get_back_keyboard(f"admin_view_withdraw_{wd_id}")
    )
    await state.set_state(AdminWithdrawStates.waiting_comment)
    await callback.answer()

@router.message(AdminWithdrawStates.waiting_comment)
async def admin_reject_withdraw_comment(message: Message, state: FSMContext):
    """Обработка комментария при отклонении"""
    data = await state.get_data()
    wd_id = data.get('withdrawal_id')
    comment = message.text.strip() if message.text != "-" else ""
    
    wd = await db.get_withdrawal(wd_id)
    if wd:
        await db.process_withdrawal(
            withdrawal_id=wd_id,
            admin_id=message.from_user.id,
            status=PaymentStatus.CANCELLED,
            comment=comment
        )
        
        user = await db.get_user(wd.user_id)
        await safe_send(
            bot, user.id,
            f"❌ *Ваш вывод #{wd_id} отклонен*\n\n"
            f"Причина: {comment if comment else 'не указана'}\n\n"
            f"Средства разморожены и возвращены на баланс.",
            parse_mode="Markdown"
        )
        
        await message.answer(f"✅ Вывод #{wd_id} отклонен")
    
    await state.clear()

# ============================================
# CRYPTOBOT АДМИН ФУНКЦИИ
# ============================================

@router.callback_query(F.data == "admin_cryptobot_balance")
async def admin_cryptobot_balance(callback: CallbackQuery):
    """Просмотр баланса CryptoBot"""
    if not await check_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    if not cryptobot:
        await callback.answer("❌ CryptoBot не настроен", show_alert=True)
        return
    
    balances = await cryptobot.get_balance()
    me = await cryptobot.get_me()
    
    text = "🤖 *CryptoBot баланс*\n\n"
    
    if me:
        text += f"👤 Владелец: {me.get('name', 'Неизвестно')}\n"
        text += f"🆔 App ID: {me.get('app_id', 'Неизвестно')}\n\n"
    
    if balances:
        for item in balances:
            asset = item.get('asset', 'Unknown')
            available = float(item.get('available', 0))
            text += f"• {asset}: `{format_number(available)}`\n"
    else:
        text += "Нет данных о балансе"
    
    await callback.message.edit_text(
        text,
        reply_markup=get_back_keyboard("admin_panel"),
        parse_mode="Markdown"
    )
    await callback.answer()

@router.callback_query(F.data == "admin_cryptobot_transfers")
async def admin_cryptobot_transfers(callback: CallbackQuery):
    """История переводов CryptoBot"""
    if not await check_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    if not cryptobot:
        await callback.answer("❌ CryptoBot не настроен", show_alert=True)
        return
    
    transfers = await cryptobot.get_transfers(limit=20)
    
    text = "📊 *История переводов CryptoBot*\n\n"
    
    if transfers:
        for t in transfers[:10]:
            status_emoji = "✅" if t.get('status') == 'completed' else "⏳"
            amount = float(t.get('amount', 0))
            asset = t.get('asset', 'USDT')
            user_id = t.get('user_id', '?')
            text += f"{status_emoji} {amount} {asset} → {user_id}\n"
    else:
        text += "Нет переводов"
    
    await callback.message.edit_text(
        text,
        reply_markup=get_back_keyboard("admin_panel"),
        parse_mode="Markdown"
    )
    await callback.answer()

# ============================================
# СТАТИСТИКА
# ============================================

@router.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    """Полная статистика"""
    if not await check_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    stats = await db.get_stats()
    
    # Дополнительная статистика
    total_withdrawals = sum(w.amount for w in db.withdrawals.values() if w.status == PaymentStatus.COMPLETED)
    total_withdrawn = sum(w.net_amount for w in db.withdrawals.values() if w.status == PaymentStatus.COMPLETED)
    total_fees = sum(w.fee for w in db.withdrawals.values() if w.status == PaymentStatus.COMPLETED)
    
    text = f"📊 *ПОЛНАЯ СТАТИСТИКА*\n\n"
    
    text += f"👥 *Пользователи*\n"
    text += f"• Всего: {stats['users']}\n"
    text += f"• Новых сегодня: {stats['users_today']}\n"
    text += f"• Администраторов: {stats['admins']}\n\n"
    
    text += f"📝 *Заявки*\n"
    text += f"• Всего: {stats['requests']}\n"
    text += f"• В ожидании: {stats['requests_pending']}\n"
    text += f"• Сегодня: {stats['requests_today']}\n\n"
    
    text += f"💳 *Выводы*\n"
    text += f"• Всего заявок: {len(db.withdrawals)}\n"
    text += f"• В обработке: {stats['withdrawals_pending']}\n"
    text += f"• Выполнено: {total_withdrawals:.2f} USDT\n"
    text += f"• Выплачено пользователям: {total_withdrawn:.2f} USDT\n"
    text += f"• Заработано комиссий: {total_fees:.2f} USDT\n\n"
    
    text += f"💰 *Финансы*\n"
    text += f"• Казна: {format_number(stats['treasury'])} USDT\n"
    
    if cryptobot:
        crypto_balance = await cryptobot.get_asset_balance("USDT")
        text += f"• CryptoBot: {format_number(crypto_balance)} USDT\n"
    
    text += f"\n🕐 {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
    
    await callback.message.edit_text(
        text,
        reply_markup=get_back_keyboard("admin_panel"),
        parse_mode="Markdown"
    )
    await callback.answer()

# ============================================
# ТЕСТ-РЕЖИМ
# ============================================

@router.callback_query(F.data == "admin_test_mode")
async def admin_test_mode(callback: CallbackQuery):
    """Переключение тест-режима"""
    if not await check_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    db.settings['test_mode'] = not db.settings.get('test_mode', False)
    status = "ВКЛЮЧЕН" if db.settings['test_mode'] else "ВЫКЛЮЧЕН"
    
    await callback.answer(f"🧪 Тест-режим {status}", show_alert=True)
    await admin_panel(callback.message)

# ============================================
# ВОЗВРАТ В АДМИН ПАНЕЛЬ
# ============================================

@router.callback_query(F.data == "admin_panel")
async def callback_admin_panel(callback: CallbackQuery):
    """Возврат в админ панель"""
    if not await check_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    await admin_panel(callback.message)

@router.callback_query(F.data == "main_menu")
async def callback_main_menu(callback: CallbackQuery):
    """Возврат в главное меню"""
    await callback.answer()
    user = await db.get_user(callback.from_user.id)
    
    pending = len([r for r in db.requests.values() 
                  if r.user_id == user.id and r.status == RequestStatus.PENDING])
    
    text = f"👋 *Главное меню*\n\n"
    text += f"💰 Баланс: `{format_number(user.balance)} USDT`\n"
    text += f"🎁 Бонус: `{format_number(user.bonus_balance)} USDT`\n"
    text += f"📊 В очереди: {pending}\n"
    text += f"\n🕐 {datetime.now().strftime('%H:%M')}"
    
    await callback.message.edit_text(text, reply_markup=None, parse_mode="Markdown")
    await show_main_menu(callback.message)

# ============================================
# ЗАГЛУШКИ ДЛЯ НЕРЕАЛИЗОВАННЫХ ФУНКЦИЙ
# ============================================

@router.callback_query(F.data.startswith("admin_"))
async def admin_not_implemented(callback: CallbackQuery):
    """Заглушка для нереализованных функций"""
    if not await check_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    await callback.answer("⏳ Функция в разработке", show_alert=True)

# ============================================
# ЗАПУСК БОТА
# ============================================

async def check_cryptobot_periodically():
    """Периодическая проверка статуса CryptoBot"""
    while True:
        await asyncio.sleep(300)  # Каждые 5 минут
        if cryptobot:
            try:
                balance = await cryptobot.get_asset_balance("USDT")
                logger.info(f"CryptoBot balance check: {balance} USDT")
                
                # Автопроверка неоплаченных инвойсов (чтобы не пропустить оплату)
                for inv_id, inv in list(db.cryptobot_invoices.items())[:200]:
                    if inv.status == "paid" or bool(getattr(inv, "_credited", False)):
                        continue
                    try:
                        status = await cryptobot.get_invoice_status(inv_id)
                        if status and status != inv.status:
                            await db.update_invoice_status(inv_id, status)
                        if status == "paid":
                            await _try_credit_paid_invoice(inv_id)
                    except Exception as e:
                        logger.error(f"Invoice check error {inv_id}: {e}")
            except Exception as e:
                logger.error(f"CryptoBot check error: {e}")

async def auto_backup():
    """Автоматическое создание бэкапов"""
    while True:
        await asyncio.sleep(3600)  # Каждый час
        try:
            backup_file = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
            async with aiofiles.open(db.db_path, 'rb') as src:
                async with aiofiles.open(backup_file, 'wb') as dst:
                    await dst.write(await src.read())
            db.settings['last_backup'] = datetime.now().isoformat()
            logger.info(f"Auto backup created: {backup_file}")
            
            # Удаляем старые бэкапы (старше 7 дней)
            for f in os.listdir('.'):
                if f.startswith('backup_') and f.endswith('.db'):
                    try:
                        file_time = datetime.fromtimestamp(os.path.getctime(f))
                        if datetime.now() - file_time > timedelta(days=7):
                            os.remove(f)
                            logger.info(f"Removed old backup: {f}")
                    except:
                        pass
        except Exception as e:
            logger.error(f"Backup error: {e}")

async def on_startup():
    """Действия при запуске"""
    logger.info("Загрузка базы данных...")
    await db.load()
    
    # Проверка CryptoBot
    if cryptobot:
        try:
            me = await cryptobot.get_me()
            if me:
                logger.info(f"CryptoBot connected: {me.get('name')}")
                balance = await cryptobot.get_asset_balance("USDT")
                logger.info(f"CryptoBot USDT balance: {balance}")
            else:
                logger.warning("CryptoBot connection failed")
        except Exception as e:
            logger.error(f"CryptoBot init error: {e}")
    else:
        logger.warning("CryptoBot not configured")
    
    logger.info(f"Бот запущен! Администраторы: {ADMIN_IDS}")
    
    # Запускаем фоновые задачи
    asyncio.create_task(check_cryptobot_periodically())
    asyncio.create_task(auto_backup())

async def on_shutdown():
    """Действия при остановке"""
    logger.info("Сохранение базы данных...")
    await db.save()
    logger.info("Бот остановлен")

async def main():
    """Главная функция"""
    await on_startup()
    
    dp.include_router(router)
    
    try:
        await dp.start_polling(bot)
    finally:
        await on_shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")