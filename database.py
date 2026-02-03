import sqlite3
import logging
from datetime import datetime
from typing import List, Tuple, Optional, Dict
from zoneinfo import ZoneInfo
import config

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_path: str = "bot_database.db"):
        self.db_path = db_path

    def _get_connection(self):
        """Получить соединение с БД"""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row  # Для работы со словарями
        return conn

    def initialize(self) -> None:
        """Инициализация базы данных"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Создаем таблицы
        cursor.execute('''
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

        cursor.execute('''
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

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS work_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                is_active BOOLEAN DEFAULT FALSE,
                changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS blocked_numbers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone_number TEXT UNIQUE NOT NULL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS admins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE NOT NULL,
                is_owner BOOLEAN DEFAULT FALSE,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                added_by INTEGER
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS subscription_channel (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                channel_id INTEGER UNIQUE
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS auto_mode (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                is_enabled BOOLEAN DEFAULT FALSE,
                changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_balances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE NOT NULL,
                balance REAL DEFAULT 0.0,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bot_settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                price_per_number REAL DEFAULT 0.0,
                bot_balance REAL DEFAULT 0.0
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                type TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')

        # Создаем индексы
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_phone_status ON phone_numbers(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_phone_user ON phone_numbers(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_phone_number ON phone_numbers(phone_number)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_phone_position ON phone_numbers(position_in_queue) WHERE status = "в очереди"')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_balance ON user_balances(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_user ON transactions(user_id)')

        conn.commit()
        conn.close()
        logger.info("База данных инициализирована")

    def register_user(self, user_id: int, username: str | None, first_name: str | None, last_name: str | None):
        """Регистрация пользователя"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO users (user_id, username, first_name, last_name) VALUES (?, ?, ?, ?)",
            (user_id, username, first_name, last_name)
        )
        conn.commit()
        conn.close()

    def update_user_info(self, user_id: int, username: str | None, first_name: str | None, last_name: str | None):
        """Обновление информации о пользователе"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
        exists = cursor.fetchone()
        
        if exists:
            cursor.execute(
                "UPDATE users SET username = ?, first_name = ?, last_name = ? WHERE user_id = ?",
                (username, first_name, last_name, user_id)
            )
        else:
            cursor.execute(
                "INSERT INTO users (user_id, username, first_name, last_name) VALUES (?, ?, ?, ?)",
                (user_id, username, first_name, last_name)
            )
        conn.commit()
        conn.close()

    def is_user_banned(self, user_id: int) -> bool:
        """Проверка бана пользователя"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT is_banned FROM users WHERE user_id = ?", (user_id,))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else False

    def ban_user(self, user_id: int):
        """Бан пользователя"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET is_banned = TRUE WHERE user_id = ?", (user_id,))
        conn.commit()
        conn.close()

    def unban_user(self, user_id: int):
        """Разбан пользователя"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET is_banned = FALSE WHERE user_id = ?", (user_id,))
        conn.commit()
        conn.close()

    def is_work_active(self) -> bool:
        """Проверка статуса работы"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT is_active FROM work_status ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else False

    def set_work_active(self, active: bool):
        """Установка статуса работы"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO work_status (is_active) VALUES (?)", (active,))
        conn.commit()
        conn.close()

    def is_number_blocked(self, phone: str) -> bool:
        """Проверка блокировки номера"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM blocked_numbers WHERE phone_number = ?", (phone,))
        result = cursor.fetchone() is not None
        conn.close()
        return result

    def is_number_in_queue_or_success(self, phone: str) -> bool:
        """Проверка номера в очереди или успешно"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT 1 FROM phone_numbers 
            WHERE phone_number = ? AND (status = 'в очереди' OR status = 'успешно')
        ''', (phone,))
        result = cursor.fetchone() is not None
        conn.close()
        return result

    def block_number(self, phone: str):
        """Блокировка номера"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT OR IGNORE INTO blocked_numbers (phone_number) VALUES (?)", (phone,))
        conn.commit()
        conn.close()

    def add_phone_number(self, user_id: int, phone: str, original: str) -> int:
        """Добавление номера телефона"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT MAX(position_in_queue) FROM phone_numbers WHERE status = 'в очереди'")
        row = cursor.fetchone()
        position = (row[0] or 0) + 1

        cursor.execute('''
            INSERT INTO phone_numbers (user_id, phone_number, original_format, position_in_queue)
            VALUES (?, ?, ?, ?)
        ''', (user_id, phone, original, position))
        
        last_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return last_id

    def get_queue_count(self) -> int:
        """Получение количества в очереди"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM phone_numbers WHERE status = 'в очереди'")
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else 0

    def get_today_stats(self) -> Tuple[int, int]:
        """Статистика за сегодня"""
        msk_tz = ZoneInfo(config.TIMEZONE)
        today_msk = datetime.now(msk_tz).date()
        today_str = today_msk.strftime("%Y-%m-%d")
        
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT COUNT(*), SUM(CASE WHEN status = 'успешно' THEN 1 ELSE 0 END)
            FROM phone_numbers WHERE DATE(added_at) = ?
        ''', (today_str,))
        row = cursor.fetchone()
        conn.close()
        return row[0] or 0, row[1] or 0

    def get_user_queue(self, user_id: int) -> List[Dict]:
        """Очередь пользователя"""
        conn = self._get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('''
            SELECT phone_number, position_in_queue FROM phone_numbers
            WHERE user_id = ? AND status = 'в очереди'
            ORDER BY position_in_queue
        ''', (user_id,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def get_next_in_queue(self) -> Optional[Dict]:
        """Следующий номер в очереди"""
        conn = self._get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('''
            SELECT p.*, u.username, u.first_name FROM phone_numbers p
            LEFT JOIN users u ON p.user_id = u.user_id
            WHERE p.status = 'в очереди'
            ORDER BY p.position_in_queue ASC LIMIT 1
        ''')
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

    def take_number(self, number_id: int, admin_id: int):
        """Взятие номера в работу"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE phone_numbers SET status = 'взято', taken_at = CURRENT_TIMESTAMP, taken_by = ?
            WHERE id = ?
        ''', (admin_id, number_id))
        cursor.execute('''
            UPDATE phone_numbers SET position_in_queue = position_in_queue - 1
            WHERE status = 'в очереди' AND position_in_queue > (SELECT position_in_queue FROM phone_numbers WHERE id = ?)
        ''', (number_id,))
        conn.commit()
        conn.close()

    def update_number_status(self, number_id: int, status: str, code: str = None, reason: str = None):
        """Обновление статуса номера"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE phone_numbers SET status = ?, completed_at = CURRENT_TIMESTAMP, code = ?, result_reason = ?
            WHERE id = ?
        ''', (status, code, reason, number_id))
        conn.commit()
        conn.close()

    def return_to_queue(self, number_id: int):
        """Возврат номера в очередь"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(position_in_queue) FROM phone_numbers WHERE status = 'в очереди'")
        row = cursor.fetchone()
        new_pos = (row[0] or 0) + 1
        
        cursor.execute('''
            UPDATE phone_numbers SET status = 'в очереди', position_in_queue = ?
            WHERE id = ?
        ''', (new_pos, number_id))
        conn.commit()
        conn.close()

    def get_phone_by_id(self, number_id: int) -> Optional[Dict]:
        """Получение номера по ID"""
        conn = self._get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('''
            SELECT p.*, u.username, u.first_name
            FROM phone_numbers p
            LEFT JOIN users u ON p.user_id = u.user_id
            WHERE p.id = ?
        ''', (number_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

    def get_phone_by_number(self, phone: str, status: str = None) -> Optional[Dict]:
        """Получение номера по телефону"""
        conn = self._get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        if status:
            cursor.execute('''
                SELECT p.*, u.username, u.first_name
                FROM phone_numbers p
                LEFT JOIN users u ON p.user_id = u.user_id
                WHERE p.phone_number = ? AND p.status = ?
                ORDER BY p.taken_at DESC
                LIMIT 1
            ''', (phone, status))
        else:
            cursor.execute('''
                SELECT p.*, u.username, u.first_name
                FROM phone_numbers p
                LEFT JOIN users u ON p.user_id = u.user_id
                WHERE p.phone_number = ?
                ORDER BY p.taken_at DESC
                LIMIT 1
            ''', (phone,))
        
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

    def get_user_by_id(self, user_id: int) -> Optional[Dict]:
        """Получение пользователя по ID"""
        conn = self._get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

    def get_all_users(self) -> List[int]:
        """Получение всех пользователей"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM users WHERE is_banned = FALSE")
        rows = cursor.fetchall()
        conn.close()
        return [row[0] for row in rows]

    def get_report_dates(self) -> List[str]:
        """Даты отчетов"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT DISTINCT DATE(added_at) FROM phone_numbers
            ORDER BY DATE(added_at) DESC
        ''')
        rows = cursor.fetchall()
        conn.close()
        return [row[0] for row in rows]

    def get_report_for_date(self, date_str: str) -> List[Dict]:
        """Отчет за дату"""
        conn = self._get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('''
            SELECT p.user_id, u.username, p.phone_number, p.status, p.added_at, p.completed_at, p.code, p.result_reason
            FROM phone_numbers p
            LEFT JOIN users u ON p.user_id = u.user_id
            WHERE DATE(p.added_at) = ?
            ORDER BY 
                CASE p.status WHEN 'успешно' THEN 1 ELSE 2 END,
                p.added_at
        ''', (date_str,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def get_user_report_for_date(self, user_id: int, date_str: str) -> List[Dict]:
        """Отчет пользователя за дату"""
        conn = self._get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('''
            SELECT p.phone_number, p.status, p.added_at, p.completed_at, p.code, p.result_reason
            FROM phone_numbers p
            WHERE p.user_id = ? AND DATE(p.added_at) = ?
            ORDER BY p.added_at
        ''', (user_id, date_str))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    # ===================== Методы для работы с админами =====================
    def add_admin(self, user_id: int, added_by: int, is_owner: bool = False):
        """Добавление админа"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO admins (user_id, is_owner, added_by)
            VALUES (?, ?, ?)
        ''', (user_id, is_owner, added_by))
        conn.commit()
        conn.close()

    def remove_admin(self, user_id: int):
        """Удаление админа"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM admins WHERE user_id = ?", (user_id,))
        conn.commit()
        conn.close()

    def is_admin_in_db(self, user_id: int) -> bool:
        """Проверка админа"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM admins WHERE user_id = ?", (user_id,))
        result = cursor.fetchone() is not None
        conn.close()
        return result

    def is_owner_in_db(self, user_id: int) -> bool:
        """Проверка овнера"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM admins WHERE user_id = ? AND is_owner = TRUE", (user_id,))
        result = cursor.fetchone() is not None
        conn.close()
        return result

    def get_all_admins(self) -> List[Dict]:
        """Все админы"""
        conn = self._get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('''
            SELECT a.*, u.username, u.first_name
            FROM admins a
            LEFT JOIN users u ON a.user_id = u.user_id
            ORDER BY a.is_owner DESC, a.added_at
        ''')
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    # ===================== Методы для работы с номерами =====================
    def delete_number_from_queue(self, number_id: int) -> Optional[Dict]:
        """Удаление номера из очереди"""
        conn = self._get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT p.id, p.user_id, p.phone_number, p.position_in_queue, u.username, u.first_name
            FROM phone_numbers p
            LEFT JOIN users u ON p.user_id = u.user_id
            WHERE p.id = ? AND p.status = 'в очереди'
        ''', (number_id,))
        row = cursor.fetchone()
        if not row:
            conn.close()
            return None
            
        number_data = dict(row)
        position = number_data['position_in_queue']
        
        cursor.execute("DELETE FROM phone_numbers WHERE id = ?", (number_id,))
        cursor.execute('''
            UPDATE phone_numbers SET position_in_queue = position_in_queue - 1
            WHERE status = 'в очереди' AND position_in_queue > ?
        ''', (position,))
        
        conn.commit()
        conn.close()
        return number_data

    def clear_queue(self) -> Dict[int, List[str]]:
        """Очистка очереди"""
        conn = self._get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT p.user_id, p.phone_number
            FROM phone_numbers p
            WHERE p.status = 'в очереди'
        ''')
        rows = cursor.fetchall()
        
        result: Dict[int, List[str]] = {}
        for row in rows:
            user_id = row['user_id']
            phone = row['phone_number']
            if user_id not in result:
                result[user_id] = []
            result[user_id].append(phone)
        
        cursor.execute("DELETE FROM phone_numbers WHERE status = 'в очереди'")
        conn.commit()
        conn.close()
        return result

    def get_number_by_id_for_user(self, user_id: int, number_id: int) -> Optional[Dict]:
        """Номер пользователя по ID"""
        conn = self._get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('''
            SELECT p.*, u.username, u.first_name
            FROM phone_numbers p
            LEFT JOIN users u ON p.user_id = u.user_id
            WHERE p.id = ? AND p.user_id = ? AND p.status = 'в очереди'
        ''', (number_id, user_id))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

    def get_user_queue_with_ids(self, user_id: int) -> List[Dict]:
        """Очередь пользователя с ID"""
        conn = self._get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, phone_number, position_in_queue FROM phone_numbers
            WHERE user_id = ? AND status = 'в очереди'
            ORDER BY position_in_queue
        ''', (user_id,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def set_subscription_channel(self, channel_id: int):
        """Установка канала подписки"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO subscription_channel (id, channel_id) VALUES (1, ?)",
            (channel_id,)
        )
        conn.commit()
        conn.close()
        logger.info(f"Обязательный канал установлен: {channel_id}")

    def get_subscription_channel(self) -> Optional[int]:
        """Получение канала подписки"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT channel_id FROM subscription_channel WHERE id = 1")
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else None

    def remove_subscription_channel(self):
        """Удаление канала подписки"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM subscription_channel")
        conn.commit()
        conn.close()
        logger.info("Обязательная подписка отключена")

    # ===================== Методы для авторежима =====================
    def is_auto_mode_enabled(self) -> bool:
        """Проверка авторежима"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT is_enabled FROM auto_mode WHERE id = 1")
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else False

    def set_auto_mode(self, enabled: bool):
        """Установка авторежима"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO auto_mode (id, is_enabled, changed_at)
            VALUES (1, ?, CURRENT_TIMESTAMP)
        ''', (enabled,))
        conn.commit()
        conn.close()
        logger.info(f"Автоматический режим {'включен' if enabled else 'выключен'}")

    # ===================== Методы для батчей номеров =====================
    def get_next_numbers_in_queue(self, count: int) -> List[Dict]:
        """Следующие номера в очереди"""
        conn = self._get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('''
            SELECT p.*, u.username, u.first_name FROM phone_numbers p
            LEFT JOIN users u ON p.user_id = u.user_id
            WHERE p.status = 'в очереди'
            ORDER BY p.position_in_queue ASC LIMIT ?
        ''', (count,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def take_numbers_batch(self, number_ids: List[int], taken_by: int = 0):
        """Массовое взятие номеров"""
        if not number_ids:
            return
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        placeholders = ','.join('?' * len(number_ids))
        cursor.execute(f'''
            SELECT position_in_queue FROM phone_numbers
            WHERE id IN ({placeholders}) AND status = 'в очереди'
        ''', number_ids)
        positions = [row[0] for row in cursor.fetchall()]
        
        if not positions:
            conn.close()
            return
        
        min_position = min(positions)
        
        cursor.execute(f'''
            UPDATE phone_numbers 
            SET status = 'взято', taken_at = CURRENT_TIMESTAMP, taken_by = ?
            WHERE id IN ({placeholders}) AND status = 'в очереди'
        ''', [taken_by] + number_ids)
        
        cursor.execute(f'''
            UPDATE phone_numbers 
            SET position_in_queue = position_in_queue - ?
            WHERE status = 'в очереди' AND position_in_queue > ?
        ''', (len(positions), min_position - 1))
        
        conn.commit()
        conn.close()
        logger.info(f"Взято номеров юзерботом: {len(number_ids)}")

    # ===================== Методы для работы с балансами =====================
    def get_user_balance(self, user_id: int) -> float:
        """Баланс пользователя"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT balance FROM user_balances WHERE user_id = ?", (user_id,))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else 0.0

    def update_user_balance(self, user_id: int, amount: float):
        """Обновление баланса"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM user_balances WHERE user_id = ?", (user_id,))
        exists = cursor.fetchone()
        
        if exists:
            cursor.execute(
                "UPDATE user_balances SET balance = balance + ? WHERE user_id = ?",
                (amount, user_id)
            )
        else:
            cursor.execute(
                "INSERT INTO user_balances (user_id, balance) VALUES (?, ?)",
                (user_id, amount)
            )
        conn.commit()
        conn.close()

    def set_user_balance(self, user_id: int, balance: float):
        """Установка баланса"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO user_balances (user_id, balance) VALUES (?, ?)",
            (user_id, balance)
        )
        conn.commit()
        conn.close()

    def get_all_user_balances(self) -> Dict[int, float]:
        """Все балансы"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT user_id, balance FROM user_balances")
        rows = cursor.fetchall()
        conn.close()
        return {row[0]: row[1] for row in rows}

    def get_total_user_balances(self) -> float:
        """Сумма всех балансов"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT SUM(balance) FROM user_balances")
        row = cursor.fetchone()
        conn.close()
        return row[0] if row[0] is not None else 0.0

    # ===================== Методы для работы с настройками бота =====================
    def get_price_per_number(self) -> float:
        """Цена за номер"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT price_per_number FROM bot_settings WHERE id = 1")
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else 0.0

    def set_price_per_number(self, price: float):
        """Установка цены"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO bot_settings (id, price_per_number) VALUES (1, ?)",
            (price,)
        )
        conn.commit()
        conn.close()
        logger.info(f"Цена за номер установлена: ${price}")

    def get_bot_balance_from_db(self) -> float:
        """Баланс бота"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT bot_balance FROM bot_settings WHERE id = 1")
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else 0.0

    def update_bot_balance(self, amount: float):
        """Обновление баланса бота"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM bot_settings WHERE id = 1")
        exists = cursor.fetchone()
        
        if exists:
            cursor.execute(
                "UPDATE bot_settings SET bot_balance = ? WHERE id = 1",
                (amount,)
            )
        else:
            cursor.execute(
                "INSERT INTO bot_settings (id, bot_balance) VALUES (1, ?)",
                (amount,)
            )
        conn.commit()
        conn.close()

    # ===================== Методы для работы с транзакциями =====================
    def add_transaction(self, user_id: int, amount: float, transaction_type: str):
        """Добавление транзакции"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO transactions (user_id, amount, type) VALUES (?, ?, ?)",
            (user_id, amount, transaction_type)
        )
        conn.commit()
        conn.close()