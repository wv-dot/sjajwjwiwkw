import os
os.system("pip install aiosqlite")
import aiosqlite
import logging
import asyncio
from datetime import datetime
from typing import List, Tuple, Optional, Dict
from zoneinfo import ZoneInfo
import config

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_path: str = "bot_database.db"):
        self.db_path = db_path
        self.lock = asyncio.Lock()

    async def initialize(self) -> None:
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                await db.execute('''
                    CREATE TABLE IF NOT EXISTS users (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER UNIQUE NOT NULL,
                        username TEXT,
                        first_name TEXT,
                        last_name TEXT,
                        registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        is_banned BOOLEAN DEFAULT FALSE
                    )
                ''')

                await db.execute('''
                    CREATE TABLE IF NOT EXISTS phone_numbers (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER NOT NULL,
                        phone_number TEXT NOT NULL,
                        original_format TEXT,
                        status TEXT DEFAULT 'в очереди',
                        position_in_queue INTEGER,
                        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        taken_at TIMESTAMP,
                        taken_by INTEGER,
                        completed_at TIMESTAMP,
                        code TEXT,
                        result_reason TEXT,
                        FOREIGN KEY (user_id) REFERENCES users(user_id)
                    )
                ''')

                await db.execute('''
                    CREATE TABLE IF NOT EXISTS work_status (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        is_active BOOLEAN DEFAULT FALSE,
                        changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')

                await db.execute('''
                    CREATE TABLE IF NOT EXISTS blocked_numbers (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        phone_number TEXT UNIQUE NOT NULL,
                        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')

                await db.execute('''
                    CREATE TABLE IF NOT EXISTS admins (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER UNIQUE NOT NULL,
                        is_owner BOOLEAN DEFAULT FALSE,
                        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        added_by INTEGER
                    )
                ''')
                await db.execute('''
                    CREATE TABLE IF NOT EXISTS subscription_channel (
                    id INTEGER PRIMARY KEY CHECK (id = 1),  -- Только одна запись
                    channel_id INTEGER UNIQUE
                    )
                ''')
                
                await db.execute('''
                    CREATE TABLE IF NOT EXISTS auto_mode (
                    id INTEGER PRIMARY KEY CHECK (id = 1),  -- Только одна запись
                    is_enabled BOOLEAN DEFAULT FALSE,
                    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                await db.execute('''
                    CREATE TABLE IF NOT EXISTS user_balances (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER UNIQUE NOT NULL,
                    balance REAL DEFAULT 0.0,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                    )
                ''')
                
                await db.execute('''
                    CREATE TABLE IF NOT EXISTS bot_settings (
                    id INTEGER PRIMARY KEY CHECK (id = 1),  -- Только одна запись
                    price_per_number REAL DEFAULT 0.0,
                    bot_balance REAL DEFAULT 0.0
                    )
                ''')
                
                await db.execute('''
                    CREATE TABLE IF NOT EXISTS transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    type TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                    )
                ''')

                # Создаем индексы для оптимизации запросов
                await db.execute('''
                    CREATE INDEX IF NOT EXISTS idx_phone_status ON phone_numbers(status)
                ''')
                await db.execute('''
                    CREATE INDEX IF NOT EXISTS idx_phone_user ON phone_numbers(user_id)
                ''')
                await db.execute('''
                    CREATE INDEX IF NOT EXISTS idx_phone_number ON phone_numbers(phone_number)
                ''')
                await db.execute('''
                    CREATE INDEX IF NOT EXISTS idx_phone_position ON phone_numbers(position_in_queue) 
                    WHERE status = 'в очереди'
                ''')
                await db.execute('''
                    CREATE INDEX IF NOT EXISTS idx_user_balance ON user_balances(user_id)
                ''')
                await db.execute('''
                    CREATE INDEX IF NOT EXISTS idx_transactions_user ON transactions(user_id)
                ''')

                await db.commit()
                logger.info("База данных инициализирована")

    async def register_user(self, user_id: int, username: str | None, first_name: str | None, last_name: str | None):
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                await db.execute(
                    "INSERT OR IGNORE INTO users (user_id, username, first_name, last_name) VALUES (?, ?, ?, ?)",
                    (user_id, username, first_name, last_name)
                )
                await db.commit()

    async def update_user_info(self, user_id: int, username: str | None, first_name: str | None, last_name: str | None):
        """Обновляет информацию о пользователе (username, first_name, last_name)"""
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                # Сначала проверяем, существует ли пользователь
                async with db.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,)) as cursor:
                    exists = await cursor.fetchone()
                
                if exists:
                    # Обновляем существующего пользователя
                    await db.execute(
                        "UPDATE users SET username = ?, first_name = ?, last_name = ? WHERE user_id = ?",
                        (username, first_name, last_name, user_id)
                    )
                else:
                    # Если пользователя нет, создаем его
                    await db.execute(
                        "INSERT INTO users (user_id, username, first_name, last_name) VALUES (?, ?, ?, ?)",
                        (user_id, username, first_name, last_name)
                    )
                await db.commit()

    async def is_user_banned(self, user_id: int) -> bool:
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                async with db.execute("SELECT is_banned FROM users WHERE user_id = ?", (user_id,)) as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row else False

    async def ban_user(self, user_id: int):
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                await db.execute("UPDATE users SET is_banned = TRUE WHERE user_id = ?", (user_id,))
                await db.commit()

    async def unban_user(self, user_id: int):
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                await db.execute("UPDATE users SET is_banned = FALSE WHERE user_id = ?", (user_id,))
                await db.commit()

    async def is_work_active(self) -> bool:
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                async with db.execute("SELECT is_active FROM work_status ORDER BY id DESC LIMIT 1") as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row else False

    async def set_work_active(self, active: bool):
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                await db.execute("INSERT INTO work_status (is_active) VALUES (?)", (active,))
                await db.commit()

    async def is_number_blocked(self, phone: str) -> bool:
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                async with db.execute("SELECT 1 FROM blocked_numbers WHERE phone_number = ?", (phone,)) as cursor:
                    return await cursor.fetchone() is not None

    async def is_number_in_queue_or_success(self, phone: str) -> bool:
        """Проверяет, есть ли номер уже в очереди или со статусом 'успешно'"""
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                async with db.execute('''
                    SELECT 1 FROM phone_numbers 
                    WHERE phone_number = ? AND (status = 'в очереди' OR status = 'успешно')
                ''', (phone,)) as cursor:
                    return await cursor.fetchone() is not None

    async def block_number(self, phone: str):
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                await db.execute("INSERT OR IGNORE INTO blocked_numbers (phone_number) VALUES (?)", (phone,))
                await db.commit()

    async def add_phone_number(self, user_id: int, phone: str, original: str) -> int:
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                async with db.execute("SELECT MAX(position_in_queue) FROM phone_numbers WHERE status = 'в очереди'") as cursor:
                    row = await cursor.fetchone()
                    position = (row[0] or 0) + 1

                await db.execute('''
                    INSERT INTO phone_numbers (user_id, phone_number, original_format, position_in_queue)
                    VALUES (?, ?, ?, ?)
                ''', (user_id, phone, original, position))
                await db.commit()
                async with db.execute("SELECT last_insert_rowid()") as cursor:
                    row = await cursor.fetchone()
                    return row[0]

    async def get_queue_count(self) -> int:
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                async with db.execute("SELECT COUNT(*) FROM phone_numbers WHERE status = 'в очереди'") as cursor:
                    row = await cursor.fetchone()
                    return row[0]

    async def get_today_stats(self) -> Tuple[int, int]:
        # Получаем текущую дату в МСК
        msk_tz = ZoneInfo(config.TIMEZONE)
        today_msk = datetime.now(msk_tz).date()
        today_str = today_msk.strftime("%Y-%m-%d")
        
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                async with db.execute('''
                    SELECT COUNT(*), SUM(CASE WHEN status = 'успешно' THEN 1 ELSE 0 END)
                    FROM phone_numbers WHERE DATE(added_at) = ?
                ''', (today_str,)) as cursor:
                    row = await cursor.fetchone()
                    return row[0] or 0, row[1] or 0

    async def get_user_queue(self, user_id: int) -> List[Dict]:
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute('''
                    SELECT phone_number, position_in_queue FROM phone_numbers
                    WHERE user_id = ? AND status = 'в очереди'
                    ORDER BY position_in_queue
                ''', (user_id,)) as cursor:
                    return [dict(row) for row in await cursor.fetchall()]

    async def get_next_in_queue(self) -> Optional[Dict]:
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute('''
                    SELECT p.*, u.username, u.first_name FROM phone_numbers p
                    LEFT JOIN users u ON p.user_id = u.user_id
                    WHERE p.status = 'в очереди'
                    ORDER BY p.position_in_queue ASC LIMIT 1
                ''') as cursor:
                    row = await cursor.fetchone()
                    return dict(row) if row else None

    async def take_number(self, number_id: int, admin_id: int):
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                await db.execute('''
                    UPDATE phone_numbers SET status = 'взято', taken_at = CURRENT_TIMESTAMP, taken_by = ?
                    WHERE id = ?
                ''', (admin_id, number_id))
                await db.execute('''
                    UPDATE phone_numbers SET position_in_queue = position_in_queue - 1
                    WHERE status = 'в очереди' AND position_in_queue > (SELECT position_in_queue FROM phone_numbers WHERE id = ?)
                ''', (number_id,))
                await db.commit()

    async def update_number_status(self, number_id: int, status: str, code: str = None, reason: str = None):
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                await db.execute('''
                    UPDATE phone_numbers SET status = ?, completed_at = CURRENT_TIMESTAMP, code = ?, result_reason = ?
                    WHERE id = ?
                ''', (status, code, reason, number_id))
                await db.commit()

    async def return_to_queue(self, number_id: int):
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                async with db.execute("SELECT MAX(position_in_queue) FROM phone_numbers WHERE status = 'в очереди'") as cursor:
                    row = await cursor.fetchone()
                    new_pos = (row[0] or 0) + 1
                await db.execute('''
                    UPDATE phone_numbers SET status = 'в очереди', position_in_queue = ?
                    WHERE id = ?
                ''', (new_pos, number_id))
                await db.commit()

    async def get_phone_by_id(self, number_id: int) -> Optional[Dict]:
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute('''
                    SELECT p.*, u.username, u.first_name
                    FROM phone_numbers p
                    LEFT JOIN users u ON p.user_id = u.user_id
                    WHERE p.id = ?
                ''', (number_id,)) as cursor:
                    row = await cursor.fetchone()
                    return dict(row) if row else None

    async def get_phone_by_number(self, phone: str, status: str = None) -> Optional[Dict]:
        """Получает номер по телефону, опционально фильтруя по статусу"""
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                db.row_factory = aiosqlite.Row
                if status:
                    async with db.execute('''
                        SELECT p.*, u.username, u.first_name
                        FROM phone_numbers p
                        LEFT JOIN users u ON p.user_id = u.user_id
                        WHERE p.phone_number = ? AND p.status = ?
                        ORDER BY p.taken_at DESC
                        LIMIT 1
                    ''', (phone, status)) as cursor:
                        row = await cursor.fetchone()
                        return dict(row) if row else None
                else:
                    async with db.execute('''
                        SELECT p.*, u.username, u.first_name
                        FROM phone_numbers p
                        LEFT JOIN users u ON p.user_id = u.user_id
                        WHERE p.phone_number = ?
                        ORDER BY p.taken_at DESC
                        LIMIT 1
                    ''', (phone,)) as cursor:
                        row = await cursor.fetchone()
                        return dict(row) if row else None

    async def get_user_by_id(self, user_id: int) -> Optional[Dict]:
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
                    row = await cursor.fetchone()
                    return dict(row) if row else None

    async def get_all_users(self) -> List[int]:
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                async with db.execute("SELECT user_id FROM users WHERE is_banned = FALSE") as cursor:
                    rows = await cursor.fetchall()
                    return [row[0] for row in rows]

    async def get_report_dates(self) -> List[str]:
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                async with db.execute('''
                    SELECT DISTINCT DATE(added_at) FROM phone_numbers
                    ORDER BY DATE(added_at) DESC
                ''') as cursor:
                    rows = await cursor.fetchall()
                    return [row[0] for row in rows]

    async def get_report_for_date(self, date_str: str) -> List[Dict]:
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute('''
                    SELECT p.user_id, u.username, p.phone_number, p.status, p.added_at, p.completed_at, p.code, p.result_reason
                    FROM phone_numbers p
                    LEFT JOIN users u ON p.user_id = u.user_id
                    WHERE DATE(p.added_at) = ?
                    ORDER BY 
                        CASE p.status WHEN 'успешно' THEN 1 ELSE 2 END,
                        p.added_at
                ''', (date_str,)) as cursor:
                    return [dict(row) for row in await cursor.fetchall()]

    async def get_user_report_for_date(self, user_id: int, date_str: str) -> List[Dict]:
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute('''
                    SELECT p.phone_number, p.status, p.added_at, p.completed_at, p.code, p.result_reason
                    FROM phone_numbers p
                    WHERE p.user_id = ? AND DATE(p.added_at) = ?
                    ORDER BY p.added_at
                ''', (user_id, date_str)) as cursor:
                    return [dict(row) for row in await cursor.fetchall()]

    # ===================== Методы для работы с админами =====================
    async def add_admin(self, user_id: int, added_by: int, is_owner: bool = False):
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                await db.execute('''
                    INSERT OR REPLACE INTO admins (user_id, is_owner, added_by)
                    VALUES (?, ?, ?)
                ''', (user_id, is_owner, added_by))
                await db.commit()

    async def remove_admin(self, user_id: int):
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                await db.execute("DELETE FROM admins WHERE user_id = ?", (user_id,))
                await db.commit()

    async def is_admin_in_db(self, user_id: int) -> bool:
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                async with db.execute("SELECT 1 FROM admins WHERE user_id = ?", (user_id,)) as cursor:
                    return await cursor.fetchone() is not None

    async def is_owner_in_db(self, user_id: int) -> bool:
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                async with db.execute("SELECT 1 FROM admins WHERE user_id = ? AND is_owner = TRUE", (user_id,)) as cursor:
                    return await cursor.fetchone() is not None

    async def get_all_admins(self) -> List[Dict]:
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute('''
                    SELECT a.*, u.username, u.first_name
                    FROM admins a
                    LEFT JOIN users u ON a.user_id = u.user_id
                    ORDER BY a.is_owner DESC, a.added_at
                ''') as cursor:
                    return [dict(row) for row in await cursor.fetchall()]

    # ===================== Методы для работы с номерами =====================
    async def delete_number_from_queue(self, number_id: int) -> Optional[Dict]:
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                db.row_factory = aiosqlite.Row
                # Получаем данные номера перед удалением, включая позицию
                async with db.execute('''
                    SELECT p.id, p.user_id, p.phone_number, p.position_in_queue, u.username, u.first_name
                    FROM phone_numbers p
                    LEFT JOIN users u ON p.user_id = u.user_id
                    WHERE p.id = ? AND p.status = 'в очереди'
                ''', (number_id,)) as cursor:
                    row = await cursor.fetchone()
                    if not row:
                        return None
                    number_data = dict(row)
                    position = number_data['position_in_queue']
                
                # Удаляем номер
                await db.execute("DELETE FROM phone_numbers WHERE id = ?", (number_id,))
                
                # Обновляем позиции в очереди (уменьшаем на 1 для всех номеров после удаленного)
                await db.execute('''
                    UPDATE phone_numbers SET position_in_queue = position_in_queue - 1
                    WHERE status = 'в очереди' AND position_in_queue > ?
                ''', (position,))
                
                await db.commit()
                return number_data

    async def clear_queue(self) -> Dict[int, List[str]]:
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                db.row_factory = aiosqlite.Row
                # Получаем все номера в очереди с данными пользователей
                async with db.execute('''
                    SELECT p.user_id, p.phone_number
                    FROM phone_numbers p
                    WHERE p.status = 'в очереди'
                ''') as cursor:
                    rows = await cursor.fetchall()
                
                # Группируем по user_id
                result: Dict[int, List[str]] = {}
                for row in rows:
                    user_id = row['user_id']
                    phone = row['phone_number']
                    if user_id not in result:
                        result[user_id] = []
                    result[user_id].append(phone)
                
                # Удаляем все номера в очереди
                await db.execute("DELETE FROM phone_numbers WHERE status = 'в очереди'")
                await db.commit()
                
                return result

    async def get_number_by_id_for_user(self, user_id: int, number_id: int) -> Optional[Dict]:
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute('''
                    SELECT p.*, u.username, u.first_name
                    FROM phone_numbers p
                    LEFT JOIN users u ON p.user_id = u.user_id
                    WHERE p.id = ? AND p.user_id = ? AND p.status = 'в очереди'
                ''', (number_id, user_id)) as cursor:
                    row = await cursor.fetchone()
                    return dict(row) if row else None

    async def get_user_queue_with_ids(self, user_id: int) -> List[Dict]:
        """Получить очередь пользователя с ID номеров для кнопок"""
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute('''
                    SELECT id, phone_number, position_in_queue FROM phone_numbers
                    WHERE user_id = ? AND status = 'в очереди'
                    ORDER BY position_in_queue
                ''', (user_id,)) as cursor:
                    return [dict(row) for row in await cursor.fetchall()]

    async def set_subscription_channel(self, channel_id: int):
        """Устанавливает обязательный канал для подписки"""
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                await db.execute(
                    "INSERT OR REPLACE INTO subscription_channel (id, channel_id) VALUES (1, ?)",
                    (channel_id,)
                )
                await db.commit()
                logger.info(f"Обязательный канал установлен: {channel_id}")

    async def get_subscription_channel(self) -> Optional[int]:
        """Возвращает ID обязательного канала или None"""
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                async with db.execute("SELECT channel_id FROM subscription_channel WHERE id = 1") as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row else None

    async def remove_subscription_channel(self):
        """Удаляет обязательную подписку (очищает канал)"""
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                await db.execute("DELETE FROM subscription_channel")
                await db.commit()
                logger.info("Обязательная подписка отключена")

    # ===================== Методы для авторежима =====================
    async def is_auto_mode_enabled(self) -> bool:
        """Проверяет, включен ли автоматический режим"""
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                async with db.execute("SELECT is_enabled FROM auto_mode WHERE id = 1") as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row else False

    async def set_auto_mode(self, enabled: bool):
        """Включает или выключает автоматический режим"""
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                await db.execute('''
                    INSERT OR REPLACE INTO auto_mode (id, is_enabled, changed_at)
                    VALUES (1, ?, CURRENT_TIMESTAMP)
                ''', (enabled,))
                await db.commit()
                logger.info(f"Автоматический режим {'включен' if enabled else 'выключен'}")

    # ===================== Методы для батчей номеров =====================
    async def get_next_numbers_in_queue(self, count: int) -> List[Dict]:
        """Получает несколько номеров из очереди (для юзербота)"""
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute('''
                    SELECT p.*, u.username, u.first_name FROM phone_numbers p
                    LEFT JOIN users u ON p.user_id = u.user_id
                    WHERE p.status = 'в очереди'
                    ORDER BY p.position_in_queue ASC LIMIT ?
                ''', (count,)) as cursor:
                    rows = await cursor.fetchall()
                    return [dict(row) for row in rows]

    async def take_numbers_batch(self, number_ids: List[int], taken_by: int = 0):
        """Массовое взятие номеров юзерботом (taken_by = 0 означает юзербот)"""
        if not number_ids:
            return
        
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                # Получаем позиции взятых номеров
                placeholders = ','.join('?' * len(number_ids))
                async with db.execute(f'''
                    SELECT position_in_queue FROM phone_numbers
                    WHERE id IN ({placeholders}) AND status = 'в очереди'
                ''', number_ids) as cursor:
                    positions = [row[0] for row in await cursor.fetchall()]
                
                if not positions:
                    return
                
                min_position = min(positions)
                
                # Обновляем статус взятых номеров
                await db.execute(f'''
                    UPDATE phone_numbers 
                    SET status = 'взято', taken_at = CURRENT_TIMESTAMP, taken_by = ?
                    WHERE id IN ({placeholders}) AND status = 'в очереди'
                ''', [taken_by] + number_ids)
                
                # Обновляем позиции остальных номеров в очереди
                await db.execute(f'''
                    UPDATE phone_numbers 
                    SET position_in_queue = position_in_queue - ?
                    WHERE status = 'в очереди' AND position_in_queue > ?
                ''', (len(positions), min_position - 1))
                
                await db.commit()
                logger.info(f"Взято номеров юзерботом: {len(number_ids)}")

    # ===================== Методы для работы с балансами =====================
    async def get_user_balance(self, user_id: int) -> float:
        """Получить баланс пользователя"""
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                async with db.execute("SELECT balance FROM user_balances WHERE user_id = ?", (user_id,)) as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row else 0.0

    async def update_user_balance(self, user_id: int, amount: float):
        """Обновить баланс пользователя (добавить или вычесть)"""
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                # Проверяем, существует ли запись
                async with db.execute("SELECT 1 FROM user_balances WHERE user_id = ?", (user_id,)) as cursor:
                    exists = await cursor.fetchone()
                
                if exists:
                    # Обновляем существующий баланс
                    await db.execute(
                        "UPDATE user_balances SET balance = balance + ? WHERE user_id = ?",
                        (amount, user_id)
                    )
                else:
                    # Создаем новую запись
                    await db.execute(
                        "INSERT INTO user_balances (user_id, balance) VALUES (?, ?)",
                        (user_id, amount)
                    )
                await db.commit()

    async def set_user_balance(self, user_id: int, balance: float):
        """Установить баланс пользователя (абсолютное значение)"""
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                await db.execute(
                    "INSERT OR REPLACE INTO user_balances (user_id, balance) VALUES (?, ?)",
                    (user_id, balance)
                )
                await db.commit()

    async def get_all_user_balances(self) -> Dict[int, float]:
        """Получить все балансы пользователей"""
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                async with db.execute("SELECT user_id, balance FROM user_balances") as cursor:
                    rows = await cursor.fetchall()
                    return {row[0]: row[1] for row in rows}

    async def get_total_user_balances(self) -> float:
        """Получить сумму всех балансов пользователей"""
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                async with db.execute("SELECT SUM(balance) FROM user_balances") as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row[0] is not None else 0.0

    # ===================== Методы для работы с настройками бота =====================
    async def get_price_per_number(self) -> float:
        """Получить цену за номер"""
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                async with db.execute("SELECT price_per_number FROM bot_settings WHERE id = 1") as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row else 0.0

    async def set_price_per_number(self, price: float):
        """Установить цену за номер"""
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                await db.execute(
                    "INSERT OR REPLACE INTO bot_settings (id, price_per_number) VALUES (1, ?)",
                    (price,)
                )
                await db.commit()
                logger.info(f"Цена за номер установлена: ${price}")

    async def get_bot_balance_from_db(self) -> float:
        """Получить баланс бота из БД"""
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                async with db.execute("SELECT bot_balance FROM bot_settings WHERE id = 1") as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row else 0.0

    async def update_bot_balance(self, amount: float):
        """Обновить баланс бота в БД"""
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                # Проверяем, существует ли запись
                async with db.execute("SELECT 1 FROM bot_settings WHERE id = 1") as cursor:
                    exists = await cursor.fetchone()
                
                if exists:
                    await db.execute(
                        "UPDATE bot_settings SET bot_balance = ? WHERE id = 1",
                        (amount,)
                    )
                else:
                    await db.execute(
                        "INSERT INTO bot_settings (id, bot_balance) VALUES (1, ?)",
                        (amount,)
                    )
                await db.commit()

    # ===================== Методы для работы с транзакциями =====================
    async def add_transaction(self, user_id: int, amount: float, transaction_type: str):
        """Добавить транзакцию в историю"""
        async with self.lock:
            async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
                await db.execute(
                    "INSERT INTO transactions (user_id, amount, type) VALUES (?, ?, ?)",
                    (user_id, amount, transaction_type)
                )
                await db.commit()
