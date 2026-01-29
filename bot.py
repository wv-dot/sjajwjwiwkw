import asyncio
import logging
import json
import os
import re
import aiohttp
from datetime import datetime
from typing import Dict, Any, Callable, List, Optional, Union
import random

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery, Dice, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.utils.deep_linking import create_start_link, decode_payload
from aiogram.enums import ParseMode, ChatType

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

GROUP_LINK = "https://t.me/+dhmAZKXtzUg1ZDM5"

BOT_TOKEN = "8546112286:AAHRJgEEr7T0t5hDqyyIklIOxglzPjucPRw"
CRYPTOBOT_API_TOKEN = "523782:AAjp3a4qWeewMrFkctINnv08lohoOqwDOjj"
ADMIN_ID = 8575934828

SUPPORT_BOTS = [
    "8020741570:AAFpG9vJVzq-nJvgm1ob1xp4BDurtc2BY1Y",
    "8259431709:AAEZ9m243IAU1bQp8fModF5wq6dwOaBkvro",
    "8474667844:AAERGKnMPRTjgg6vZ7Yq3uZQNw8XqIurNsA"
]

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


MIN_WITHDRAWAL = 100.0
THRESHOLD = 1000.0
USDT_RATE = 80.0
SUPPORT_URL = "https://t.me/news_manga"

PHOTOS = {
    "start": "AgACAgIAAxkBAAMEaXuxoVrxs-h3bGONBrd49ywlzT0AApYPaxvkPeBL8ePNMalOY9QBAAMCAAN5AAM4BA",
    "menu": "AgACAgIAAxkBAAMEaXuxoVrxs-h3bGONBrd49ywlzT0AApYPaxvkPeBL8ePNMalOY9QBAAMCAAN5AAM4BA",
    "games": "AgACAgIAAxkBAAMGaXuxobBEks_46ndiflV7ieMXs08AApgPaxvkPeBLYxsYJ3dkSmQBAAMCAAN5AAM4BA",
    "profile": "AgACAgIAAxkBAAMDaXuxodb1B0l68vJFiedJne6bA-8AApUPaxvkPeBL0wKC1nCX9jIBAAMCAAN5AAM4BA",
    "deposit": "AgACAgIAAxkBAAMFaXuxoWPxSB6Pm55dTdmVhV62wwQAApcPaxvkPeBLtJnwaYTQt8wBAAMCAAN5AAM4BA",
    "withdraw": "AgACAgIAAxkBAAMCaXuxocTt2tK_9-skX67MCSCdt-sAApQPaxvkPeBL6qdT-pE96jcBAAMCAAN5AAM4BA"
}

DB_FILE = "bot_data.json"

class DepositStates(StatesGroup):
    waiting_amount = State()

class WithdrawStates(StatesGroup):
    waiting_amount = State()


def load_db():
    default_db = {
        "menu_users": {},
        "games": {},
        "game_counter": 0,
        "promocodes": {},
        "transactions": [],
        "referral_bonuses": {},
        "deposits": [],
        "withdrawals": [],
        "last_dump": None
    }
    
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if not content:
                    save_db(default_db)
                    return default_db
                
                loaded_data = json.loads(content)
                
                for key in default_db:
                    if key not in loaded_data:
                        loaded_data[key] = default_db[key]
                        
                return loaded_data
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка декодирования JSON в БД: {e}. Создаем новую БД.")
            save_db(default_db)
            return default_db
        except Exception as e:
            logger.error(f"Ошибка загрузки БД: {e}. Создаем новую БД.")
            save_db(default_db)
            return default_db
    else:
        save_db(default_db)
        return default_db

def save_db(data):
    try:
        with open(DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Ошибка сохранения БД: {e}")

def get_or_create_user(user_id, username=""):
    data = load_db()
    user_id_str = str(user_id)
    
    logger.info(f"[DEBUG] get_or_create_user: user={user_id_str}, username={username}")
    
    if user_id_str not in data['menu_users']:
        logger.info(f"[DEBUG] Creating new user: {user_id_str}")
        data['menu_users'][user_id_str] = {
            'username': username or f"user_{user_id}",
            'balance': 0.0,
            'registered': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'referrer': None,
            'referrals': 0,
            'referrals_list': [],
            'total_spent': 0,
            'total_deposited': 0,
            'total_withdrawn': 0,
            'total_referral_income': 0.0,
            'used_promocodes': [],
            'currency': 'RUB'
        }
        save_db(data)
        logger.info(f"[DEBUG] New user created with balance: 0.0")
    
    return data['menu_users'][user_id_str]

def update_user_balance(user_id, amount, transaction_type="deposit", **kwargs):
    data = load_db()
    user_id_str = str(user_id)
    
    logger.info(f"[DEBUG] update_user_balance START: user={user_id_str}, amount={amount}, type={transaction_type}")
    
    if user_id_str not in data['menu_users']:
        logger.error(f"[DEBUG] ERROR: User {user_id_str} not found in DB!")
        return False
    
    user_data = data['menu_users'][user_id_str]
    old_balance = user_data['balance']
    old_total_spent = user_data.get('total_spent', 0)
    
    logger.info(f"[DEBUG] Before: balance={old_balance}, total_spent={old_total_spent}, total_deposited={user_data.get('total_deposited', 0)}")
    
    if transaction_type == "deposit":
        user_data['balance'] += amount
        user_data['total_deposited'] += amount

        if user_data.get('referrer'):
            referrer_id = user_data['referrer']
            referrer_id_str = str(referrer_id)
            referrer_data = data['menu_users'].get(referrer_id_str)
            if referrer_data:
                bonus = amount * 0.02
                referrer_data['balance'] += bonus
                referrer_data['total_referral_income'] = referrer_data.get('total_referral_income', 0) + bonus
                
                bonus_id = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{user_id}"
                data['referral_bonuses'][bonus_id] = {
                    'referrer_id': referrer_id,
                    'user_id': user_id,
                    'user_username': user_data['username'],
                    'amount': amount,
                    'bonus': bonus,
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
    elif transaction_type == "withdraw":
        if user_data['balance'] >= amount:
            user_data['balance'] -= amount
            user_data['total_withdrawn'] += amount
        else:
            logger.error(f"[DEBUG] Insufficient balance: {user_data['balance']} < {amount}")
            return False
    elif transaction_type == "promo":
        user_data['balance'] += amount
    elif transaction_type == "gift_sent":
        if user_data['balance'] >= amount:
            user_data['balance'] -= amount
        else:
            logger.error(f"[DEBUG] Insufficient balance for gift: {user_data['balance']} < {amount}")
            return False
    elif transaction_type == "gift_received":
        user_data['balance'] += amount
    elif transaction_type == "game_bet":
        if user_data['balance'] >= amount:
            user_data['balance'] -= amount
            user_data['total_spent'] += amount
            logger.info(f"[DEBUG] Game bet deducted: {user_id_str} {old_balance} -> {user_data['balance']} (-{amount})")
        else:
            logger.error(f"[DEBUG] Insufficient balance for game bet: {user_data['balance']} < {amount}")
            return False
    elif transaction_type == "game_win":
        user_data['balance'] += amount
        logger.info(f"[DEBUG] Game win added: {user_id_str} {old_balance} -> {user_data['balance']} (+{amount})")
    elif transaction_type == "deposit_completed":
        user_data['balance'] += amount
        user_data['total_deposited'] += amount

        if user_data.get('referrer'):
            referrer_id = user_data['referrer']
            referrer_id_str = str(referrer_id)
            referrer_data = data['menu_users'].get(referrer_id_str)
            if referrer_data:
                bonus = amount * 0.02
                referrer_data['balance'] += bonus
                referrer_data['total_referral_income'] = referrer_data.get('total_referral_income', 0) + bonus
                
                bonus_id = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{user_id}"
                data['referral_bonuses'][bonus_id] = {
                    'referrer_id': referrer_id,
                    'user_id': user_id,
                    'user_username': user_data['username'],
                    'amount': amount,
                    'bonus': bonus,
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
    
    transaction = {
        'user_id': user_id_str,
        'type': transaction_type,
        'amount': amount,
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    for key, value in kwargs.items():
        if value is not None:
            transaction[key] = value
    
    data['transactions'].append(transaction)
    
    try:
        save_db(data)
        logger.info(f"[DEBUG] SUCCESS: {user_id_str} {transaction_type} {amount}. New balance: {user_data['balance']}")
        logger.info(f"[DEBUG] Total spent updated: {old_total_spent} -> {user_data.get('total_spent', 0)}")
        return True
    except Exception as e:
        logger.error(f"[DEBUG] ERROR saving DB: {e}")
        return False

def get_user_balance(user_id):
    data = load_db()
    user_id_str = str(user_id)
    
    if user_id_str in data['menu_users']:
        return data['menu_users'][user_id_str]['balance']
    else:
        user_data = get_or_create_user(user_id)
        return user_data['balance']

# ==================== ФУНКЦИИ ДЛЯ ПРОМОКОДОВ ====================

def create_promocode(code, amount, uses_left=float('inf')):
    try:
        data = load_db()
        code = code.upper()
        
        data['promocodes'][code] = {
            'amount': float(amount),
            'uses_left': uses_left if uses_left == float('inf') else int(uses_left),
            'used_by': [],
            'created_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        save_db(data)
        return True, "Успешно"
    except Exception as e:
        logger.error(f"Ошибка создания промокода: {e}")
        return False, f"Ошибка: {e}"

def use_promocode(code, user_id):
    try:
        data = load_db()
        code = code.upper()
        user_id_str = str(user_id)
        
        if code not in data['promocodes']:
            return None, "Промокод не найден"
        
        promocode = data['promocodes'][code]
        
        if promocode['uses_left'] == 0:
            return None, "Закончились активации этого промокода"
        
        if user_id_str in promocode.get('used_by', []):
            return None, "Вы уже использовали этот промокод"
        
        amount = promocode['amount']
        
        if user_id_str in data['menu_users']:
            data['menu_users'][user_id_str]['balance'] += amount
            
            if 'used_promocodes' not in data['menu_users'][user_id_str]:
                data['menu_users'][user_id_str]['used_promocodes'] = []
            
            if code not in data['menu_users'][user_id_str]['used_promocodes']:
                data['menu_users'][user_id_str]['used_promocodes'].append(code)
            
            if 'used_by' not in promocode:
                promocode['used_by'] = []
            
            if user_id_str not in promocode['used_by']:
                promocode['used_by'].append(user_id_str)
            
            if promocode['uses_left'] != float('inf'):
                promocode['uses_left'] -= 1
            
            data['transactions'].append({
                'user_id': user_id_str,
                'type': 'promo',
                'amount': amount,
                'promocode': code,
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
            
            save_db(data)
            
            return amount, f"Промокод активирован! На ваш баланс начислено {amount} RUB"
        else:
            return None, "Ошибка начисления баланса"
            
    except Exception as e:
        logger.error(f"Ошибка использования промокода: {e}")
        return None, f"Ошибка активации промокода: {e}"


# ==================== КОМАНДЫ АДМИНА ====================

@dp.message(Command("reset_balance", "resetbal", "rb"))
async def cmd_reset_balance(message: Message, command: CommandObject = None):
    """Сбросить баланс пользователя"""
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ Эта команда только для администратора!")
        return
    
    if not command or not command.args:
        await message.answer(
            "❌ Формат: /reset_balance @username или ID\n"
            "Примеры:\n"
            "/reset_balance @username\n"
            "/reset_balance 123456789\n"
            "/reset_balance @username 0 (обнулить)\n"
            "/reset_balance 123456789 100 (установить на 100)"
        )
        return
    
    args = command.args.split()
    identifier = args[0]
    new_balance = 0.0
    
    if len(args) > 1:
        try:
            new_balance = float(args[1].replace(',', '.'))
        except ValueError:
            await message.answer("❌ Неверная сумма! Введите число. Например: 0 или 100.50")
            return
    
    try:
        data = load_db()
        user_id_str = None
        user_data = None
        
        if identifier.startswith('@'):
            username = identifier[1:].lower()
            for uid, user in data['menu_users'].items():
                if user.get('username', '').lower() == username:
                    user_id_str = uid
                    user_data = user
                    break
        else:
            user_id_str = identifier
            user_data = data['menu_users'].get(user_id_str)
        
        if not user_data:
            await message.answer(f"❌ Пользователь {identifier} не найден!")
            return
        
        old_balance = user_data['balance']
        
        user_data['balance'] = new_balance
        
        data['transactions'].append({
            'user_id': user_id_str,
            'type': 'admin_balance_adjustment',
            'old_balance': old_balance,
            'new_balance': new_balance,
            'admin_id': message.from_user.id,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'note': f"Админ {message.from_user.id} изменил баланс"
        })
        
        save_db(data)
        
        username = user_data.get('username', f'user_{user_id_str}')
        response = (f"✅ Баланс пользователя @{username} (ID: {user_id_str}) изменен:\n"
                   f"💰 Было: {old_balance:.2f} RUB\n"
                   f"💰 Стало: {new_balance:.2f} RUB")
        
        await message.answer(response)
        
        try:
            await bot.send_message(
                int(user_id_str),
                f"⚠️ Ваш баланс был изменен администратором:\n"
                f"💰 Было: {old_balance:.2f} RUB\n"
                f"💰 Стало: {new_balance:.2f} RUB\n\n"
            )
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления пользователю: {e}")
        
    except Exception as e:
        logger.error(f"Ошибка сброса баланса: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")


@dp.message(Command("dump"))
async def cmd_dump(message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("❌ Эта команда только для администратора!")
        return
    
    try:
        data = load_db()
        
        data['last_dump'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save_db(data)
        
        dump_file = f"bot_dump_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(dump_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        
        with open(dump_file, 'rb') as f:
            await message.reply_document(
                document=types.FSInputFile(dump_file),
                caption=f"📊 Дамп базы данных\n\n"
                       f"👥 Пользователей: {len(data['menu_users'])}\n"
                       f"🎲 Активных игр: {len(data['games'])}\n"
                       f"💳 Транзакций: {len(data['transactions'])}\n"
                       f"🎟 Промокодов: {len(data['promocodes'])}\n\n"
                       f"🕐 Время дампа: {data['last_dump']}"
            )
        
        os.remove(dump_file)
        
        logger.info(f"Админ {message.from_user.id} выгрузил базу данных")
        
    except Exception as e:
        logger.error(f"Ошибка дампа базы данных: {e}")
        await message.reply(f"❌ Ошибка выгрузки базы данных: {e}")

@dp.message(Command("set_promo"))
async def cmd_set_promo(message: types.Message, command: CommandObject):
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ Эта команда только для администратора!")
        return
    
    if not command.args:
        await message.answer("❌ Формат: /set_promo КОД СУММА [КОЛИЧЕСТВО_АКТИВАЦИЙ]\nПример: /set_promo BONUS100 100 50")
        return
    
    args = command.args.split()
    if len(args) < 2:
        await message.answer("❌ Формат: /set_promo КОД СУММА [КОЛИЧЕСТВО_АКТИВАЦИЙ]\nПример: /set_promo BONUS100 100 50")
        return
    
    code = args[0].upper()
    
    try:
        amount = float(args[1].replace(',', '.'))
    except ValueError:
        await message.answer("❌ Неверная сумма! Введите число. Например: 100 или 100.50")
        return
    
    if len(args) == 3:
        try:
            uses = int(args[2])
            success, msg = create_promocode(code, amount, uses)
            if success:
                await message.answer(f"✅ Промокод {code} создан!\n💰 Сумма: {amount} RUB\n📊 Количество активаций: {uses}")
            else:
                await message.answer(f"❌ Ошибка создания промокода: {msg}")
        except ValueError:
            await message.answer("❌ Неверное количество активаций! Введите целое число.")
    else:
        success, msg = create_promocode(code, amount)
        if success:
            await message.answer(f"✅ Промокод {code} создан!\n💰 Сумма: {amount} RUB\n📊 Количество активаций: безлимитно")
        else:
            await message.answer(f"❌ Ошибка создания промокода: {msg}")

@dp.message(Command("promos"))
async def cmd_promos(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ Эта команда только для администратора!")
        return
    
    data = load_db()
    
    if not data['promocodes']:
        await message.answer("❌ Нет активных промокодов")
        return
    
    promos_text = "🎟 Активные промокоды:\n\n"
    
    for promocode, promo_data in data['promocodes'].items():
        amount = promo_data.get('amount', 0)
        used_count = len(promo_data.get('used_by', []))
        uses_left = promo_data.get('uses_left', 0)
        
        if uses_left == float('inf'):
            uses_left_str = "∞"
            total_str = f"{used_count}/{uses_left_str}"
        else:
            total_uses = used_count + uses_left
            total_str = f"{used_count}/{total_uses}"
        
        promos_text += f"• {promocode}[{total_str}] - {amount} RUB\n"
    
    await message.answer(promos_text)

@dp.message(Command("pending"))
async def cmd_pending(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ Эта команда только для администратора!")
        return
    
    data = load_db()
    
    pending_withdrawals = [w for w in data.get('withdrawals', []) if w.get('status') == 'pending']
    
    if not pending_withdrawals:
        await message.answer("✅ Нет ожидающих выводов")
        return
    
    text = "📋 Ожидающие выводы:\n\n"
    
    for w in pending_withdrawals:
        user_id = w.get('user_id')
        user_data = data['menu_users'].get(str(user_id), {})
        username = user_data.get('username', 'Неизвестно')
        text += f"🆔 ID: {w.get('id')}\n👤 @{username}\n💰 {w.get('amount', 0):.2f} RUB\n\n"
    
    await message.answer(text)

# ==================== КОМАНДЫ ДЛЯ ПОЛЬЗОВАТЕЛЕЙ ====================

@dp.message(Command("promo"))
async def cmd_promo(message: types.Message, command: CommandObject):
    if not command.args:
        await message.answer("❌ Укажите промокод!\nПример: /promo BONUS100")
        return
    
    code = command.args.strip().upper()
    amount, message_text = use_promocode(code, message.from_user.id)
    
    if amount is None:
        await message.answer(f"{message_text}")
    else:
        user_data = get_or_create_user(message.from_user.id)
        await message.answer(
            f"{message_text}\n\n"
            f"💰 Ваш текущий баланс: {user_data['balance']:.2f} RUB"
        )

@dp.message(Command("ref"))
async def cmd_ref(message: types.Message):
    user_id = str(message.from_user.id)
    user_data = get_or_create_user(message.from_user.id)
    
    try:
        ref_link = await create_start_link(bot, user_id, encode=True)
    except Exception as e:
        logger.error(f"Ошибка создания реф. ссылки: {e}")
        ref_link = f"https://t.me/{(await bot.get_me()).username}?start={user_id}"
    
    referrals_list = ""
    if user_data['referrals_list']:
        for i, ref_id in enumerate(user_data['referrals_list'][:10], 1):
            data = load_db()
            ref_data = data['menu_users'].get(str(ref_id), {})
            ref_name = ref_data.get('username', f'user_{ref_id}')
            referrals_list += f"{i}. @{ref_name}\n"
        if len(user_data['referrals_list']) > 10:
            referrals_list += f"... и еще {len(user_data['referrals_list']) - 10} рефералов"
    else:
        referrals_list = "У вас пока нет рефералов"
    
    response = (
        f"📊 Ваша реферальная статистика:\n\n"
        f"🔗 Ссылка: {ref_link}\n\n"
        f"👥 Всего рефералов: {user_data['referrals']}\n"
        f"💰 Доход от рефералов: {user_data.get('total_referral_income', 0):.2f} RUB\n\n"
        f"📋 Список рефералов:\n{referrals_list}\n\n"
        f"🎯 Вы получаете 2% от каждого депозита ваших рефералов!"
    )
    
    await message.answer(response)

@dp.message(Command("gift"))
async def cmd_gift(message: types.Message, command: CommandObject = None):
    try:
        if message.reply_to_message:
            if message.reply_to_message.from_user.is_bot:
                await message.reply("❌ Нельзя дарить баланс боту")
                return
            
            parts = message.text.split()
            if len(parts) != 2:
                await message.reply("❌ Используйте: /gift <сумма> (в ответ на сообщение)\nИли: /gift username/id <сумма>")
                return
            
            try:
                amount = float(parts[1].replace(',', '.'))
            except ValueError:
                await message.reply("❌ Неверная сумма. Введите число!")
                return
            
            if amount <= 0:
                await message.reply("❌ Сумма должна быть положительной")
                return
            
            if amount > 10000:
                await message.reply("❌ Максимальная сумма подарка: 10000 RUB")
                return
            
            sender_id = str(message.from_user.id)
            receiver_id = str(message.reply_to_message.from_user.id)
            
            if sender_id == receiver_id:
                await message.reply("❌ Нельзя дарить самому себе")
                return
            
            sender_data = get_or_create_user(message.from_user.id, message.from_user.username or message.from_user.first_name)
            receiver_data = get_or_create_user(message.reply_to_message.from_user.id, 
                                              message.reply_to_message.from_user.username or message.reply_to_message.from_user.first_name)
            
            if sender_data['balance'] < amount:
                await message.reply(f"❌ Недостаточно средств. Ваш баланс: {sender_data['balance']:.2f} RUB")
                return
            
            update_user_balance(message.from_user.id, amount, "gift_sent")
            update_user_balance(message.reply_to_message.from_user.id, amount, "gift_received")
            
            sender_name = sender_data['username']
            receiver_name = receiver_data['username']
            
            await message.reply(f"🎁 Подарок отправлен!\n\n"
                              f"👤 От: @{sender_name}\n"
                              f"👥 Кому: @{receiver_name}\n"
                              f"💰 Сумма: {amount:.2f} RUB\n\n"
                              f"💳 Ваш баланс: {sender_data['balance'] - amount:.2f} RUB")
            
            try:
                await bot.send_message(
                    message.reply_to_message.from_user.id,
                    f"🎁 Вы получили подарок {amount:.2f} RUB от @{sender_name}!\n\n"
                    f"💳 Ваш баланс: {receiver_data['balance'] + amount:.2f} RUB"
                )
            except Exception as e:
                logger.error(f"Ошибка уведомления получателя: {e}")
            
        else:
            if not command or not command.args:
                await message.reply(
                    "📝 Формат команды:\n"
                    "1. В ответ на сообщение: /gift <сумма>\n"
                    "2. По username: /gift @username <сумма>\n"
                    "3. По ID: /gift 123456789 <сумма>\n\n"
                    "📋 Примеры:\n"
                    "/gift @username 100\n"
                    "/gift 123456789 100"
                )
                return
            
            args = command.args.split()
            if len(args) < 2:
                await message.reply("❌ Формат: /gift username/id <сумма>\nПример: /gift @username 100")
                return
            
            username_or_id = args[0]
            
            try:
                amount = float(args[1].replace(',', '.'))
            except ValueError:
                await message.reply("❌ Неверная сумма. Введите число!")
                return
            
            if amount <= 0:
                await message.reply("❌ Сумма должна быть положительной")
                return
            
            if amount > 10000:
                await message.reply("❌ Максимальная сумма подарка: 10000 RUB")
                return
            
            sender_id = str(message.from_user.id)
            
            sender_data = get_or_create_user(message.from_user.id, message.from_user.username or message.from_user.first_name)
            
            receiver = None
            receiver_id = None
            
            if username_or_id.startswith('@'):
                username = username_or_id[1:]
                
                data = load_db()
                for user_id_str, user_data in data['menu_users'].items():
                    if user_data.get('username', '').lower() == username.lower():
                        receiver = user_data
                        receiver_id = int(user_id_str)
                        break
            
            else:
                try:
                    receiver_id = int(username_or_id)
                    data = load_db()
                    receiver = data['menu_users'].get(str(receiver_id))
                except ValueError:
                    await message.reply("❌ Неверный формат. Используйте @username или ID пользователя")
                    return
            
            if not receiver:
                await message.reply("❌ Пользователь не найден. Убедитесь, что он зарегистрирован в боте.")
                return
            
            if receiver_id == message.from_user.id:
                await message.reply("❌ Нельзя дарить самому себе")
                return
            
            if sender_data['balance'] < amount:
                await message.reply(f"❌ Недостаточно средств. Ваш баланс: {sender_data['balance']:.2f} RUB")
                return
            
            update_user_balance(message.from_user.id, amount, "gift_sent")
            update_user_balance(receiver_id, amount, "gift_received")
            
            sender_name = sender_data['username']
            receiver_name = receiver['username']
            
            await message.reply(f"🎁 Подарок отправлен!\n\n"
                              f"👤 От: @{sender_name}\n"
                              f"👥 Кому: @{receiver_name}\n"
                              f"💰 Сумма: {amount:.2f} RUB\n\n"
                              f"💳 Ваш баланс: {sender_data['balance'] - amount:.2f} RUB")
            
            try:
                await bot.send_message(
                    receiver_id,
                    f"🎁 Вы получили подарок {amount:.2f} RUB от @{sender_name}!\n\n"
                    f"💳 Ваш баланс: {receiver.get('balance', 0) + amount:.2f} RUB"
                )
            except Exception as e:
                logger.error(f"Ошибка уведомления получателя: {e}")
        
    except Exception as e:
        logger.error(f"Ошибка отправки подарка: {e}")
        await message.reply("❌ Ошибка отправки подарка")

async def send_dice_from_support_bot(token: str, chat_id: int, emoji: str) -> int:
    try:
        url = f"https://api.telegram.org/bot{token}/sendDice"
        params = {
            'chat_id': chat_id,
            'emoji': emoji
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('ok'):
                        dice_value = data['result']['dice']['value']
                        logger.info(f"Саппорт бот {token[-4:]} отправил {emoji}: {dice_value}")
                        return dice_value
                else:
                    logger.error(f"Ошибка отправки дайса от саппорт бота: {await response.text()}")
                    return random.randint(1, 6)
    except Exception as e:
        logger.error(f"Ошибка в send_dice_from_support_bot: {e}")
        return random.randint(1, 6)

async def get_support_bot_tokens(dice_per_player: int = 1, current_round: int = 1) -> List[str]:
    tokens = []
    for i in range(dice_per_player):
        token_index = (current_round + i) % len(SUPPORT_BOTS)
        tokens.append(SUPPORT_BOTS[token_index])
    return tokens

def get_main_menu():
    keyboard = [
        [InlineKeyboardButton(text="Активные игры", callback_data="active_games")],
        [InlineKeyboardButton(text="Профиль", callback_data="profile")],
        [InlineKeyboardButton(text="Тех поддержка", url=SUPPORT_URL)]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_back_to_main():
    keyboard = [[InlineKeyboardButton(text="Назад", callback_data="back_to_main")]]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_back_to_games():
    keyboard = [[InlineKeyboardButton(text="Назад", callback_data="active_games")]]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_profile_menu():
    keyboard = [
        [InlineKeyboardButton(text="Пополнение", callback_data="deposit"), 
         InlineKeyboardButton(text="Вывод", callback_data="withdraw")],
        [InlineKeyboardButton(text="Статистика", callback_data="stats")],
        [InlineKeyboardButton(text="Сделать подарок", callback_data="make_gift")],
        [InlineKeyboardButton(text="Промокоды", callback_data="promocodes_menu")],
        [InlineKeyboardButton(text="Реферальная система", callback_data="referral")],
        [InlineKeyboardButton(text="Назад", callback_data="back_to_main")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_back_to_profile():
    keyboard = [[InlineKeyboardButton(text="Назад", callback_data="profile")]]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_payment_back():
    keyboard = [[InlineKeyboardButton(text="Назад", callback_data="payment_back")]]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

@dp.message(Command("start"))
async def cmd_start(message: Message, command: CommandObject = None):
    user_id = str(message.from_user.id)
    username = message.from_user.username or message.from_user.first_name
    
    user_data = get_or_create_user(message.from_user.id, username)
    
    if command and command.args:
        try:
            referrer_id = decode_payload(command.args)
            if referrer_id and referrer_id != user_id and not user_data.get('referrer'):
                data = load_db()
                
                if user_id in data['menu_users']:
                    data['menu_users'][user_id]['referrer'] = referrer_id
                    
                    referrer_id_str = str(referrer_id)
                    if referrer_id_str in data['menu_users']:
                        referrer_data = data['menu_users'][referrer_id_str]
                        
                        if 'referrals_list' not in referrer_data:
                            referrer_data['referrals_list'] = []
                        
                        if user_id not in referrer_data['referrals_list']:
                            referrer_data['referrals_list'].append(user_id)
                            referrer_data['referrals'] = len(referrer_data['referrals_list'])
                            
                            try:
                                await bot.send_message(
                                    int(referrer_id),
                                    f"По вашей ссылке зарегистрировался новый пользователь!\n"
                                    f"@{username}\n"
                                    f"Теперь у вас {referrer_data['referrals']} рефералов\n"
                                    f"Вы будете получать 2% от всех депозитов этого пользователя!"
                                )
                            except Exception as e:
                                logger.error(f"Ошибка отправки уведомления рефереру: {e}")
                    
                    save_db(data)
        except Exception as e:
            logger.error(f"Ошибка декодирования payload: {e}")
    
    cap = "Добро пожаловать в бота!\n\nВыберите раздел:"

    if int(user_id) == ADMIN_ID:
        cap += "\n\nАдмин команды:\n/set_promo - Создание промокода\n/promos - Просмотр активных промокодов\n/rb - обнулить баланс\n/pending - Просмотр ожидающих выводов\n/dump - Выгрузка базы данных"
    if message.chat.type == ChatType.PRIVATE:
        await message.answer_photo(
            PHOTOS["start"],
            caption=cap,
            reply_markup=get_main_menu()
        )
    else:
        await message.reply(
            "Доступные игры:\n"
            "/cub <ставка> - игра в кубики\n"
            "/dart <ставка> - игра в дартс\n"
            "/basket <ставка> - баскетбол\n"
            "/bowl <ставка> - боулинг\n"
            "/foot <ставка> - футбол\n"
            "/21cub <ставка> - игра 21 очко\n\n"
            "⚙️ Режимы для любой игры:\n"
            "/gameNx <ставка> - игра до N побед\n"
            "/gameNt <ставка> - игра с N бросками на игрока\n"
            "/gameNp <ставка> - игра на N игроков\n\n"
            "💰 Управление балансом:\n"
            "/bal - посмотреть баланс\n"
            "/gift <сумма> - подарить баланс (ответьте на сообщение)\n"
            "/del - удалить последнюю созданную игру\n\n"
            "Примеры:\n"
            "/cub 10 - классика (1 бросок, боты кидают)\n"
            "/dart3x 20 - игра до 3 побед (боты)\n"
            "/basket5t 30 - 5 бросков на игрока (боты)\n"
            "/bowl4p 40 - игра на 4 игроков (боты)\n"
            "/21cub 50 - игра 21 очко (5 бросков, боты)"
        )

@dp.message(Command("bal", "бал"))
async def cmd_balance(message: Message):
    user_id = str(message.from_user.id)
    balance = get_user_balance(message.from_user.id)
    await message.reply(f"💰 Баланс: {balance:.2f} RUB")

@dp.message(Command("del"))
async def cmd_delete_last_game(message: Message):
    try:
        user_id = str(message.from_user.id)
        username = message.from_user.username or message.from_user.first_name
        
        data = load_db()
        active_games = data.get('games', {})
        user_games = []
        
        for game_id, game in active_games.items():
            if game.get('creator_id') == message.from_user.id and game.get('status') == 'waiting':
                user_games.append((game_id, game))
        
        if not user_games:
            await message.reply("❌ У вас нет активных игр, ожидающих начала")
            return
        
        user_games.sort(key=lambda x: int(x[0]), reverse=True)
        last_game_id, last_game = user_games[0]
        
        bet = last_game['bet']
        
        update_user_balance(message.from_user.id, bet, "game_win")
        
        if len(last_game['players']) > 1:
            for player_id, player_name in zip(last_game['players'], last_game['player_names']):
                if player_id != message.from_user.id:
                    update_user_balance(player_id, bet, "game_win")
        
        try:
            await bot.delete_message(
                chat_id=last_game['chat_id'],
                message_id=last_game['message_id']
            )
        except Exception as e:
            logger.error(f"Ошибка удаления сообщения игры: {e}")
        
        if last_game_id in data['games']:
            del data['games'][last_game_id]
            save_db(data)
        
        await message.reply(f"✅ Игра #{last_game_id} удалена. Ставка {bet} RUB возвращена на баланс")
        
        logger.info(f"Пользователь {username} удалил игру #{last_game_id}")
        
    except Exception as e:
        logger.error(f"Ошибка удаления игры: {e}")
        await message.reply("❌ Ошибка удаления игры")

@dp.message(Command("help"))
async def cmd_help(message: Message):
    await message.reply(
        "🎮 Доступные команды:\n\n"
        "/start - меню бота\n"
        "/bal - баланс (RUB)\n"
        "/gift <сумма> - подарить баланс\n"
        "/del - удалить последнюю игру\n"
        "/promo КОД - активировать промокод\n"
        "/ref - реферальная статистика\n\n"
        "Игры:\n"
        "/cub <ставка> - кубики\n"
        "/dart <ставка> - дартс\n"
        "/basket <ставка> - баскетбол\n"
        "/bowl <ставка> - боулинг\n"
        "/foot <ставка> - футбол\n"
        "/21cub <ставка> - игра 21 очко"
    )

async def get_pay_link(amount: float, payload: str = "", asset: str = "USDT"):
    headers = {"Crypto-Pay-API-Token": CRYPTOBOT_API_TOKEN}
    
    data = {
        "asset": asset,
        "amount": str(amount),
        "expires_in": 3600,
        "paid_btn_name": "openBot",
        "paid_btn_url": f"https://t.me/{(await bot.get_me()).username}",
        "payload": payload,
        "allow_comments": False,
        "allow_anonymous": True
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                'https://pay.crypt.bot/api/createInvoice',
                headers=headers,
                json=data,
                timeout=10
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    if result.get('ok'):
                        invoice = result['result']
                        return invoice['pay_url'], str(invoice['invoice_id'])
                
                return None, None
                
    except Exception as e:
        logger.error(f"Ошибка создания счета: {e}")
        return None, None

async def create_check(amount_usdt: float, user_id: int, pin_to_user_id: bool = True):
    headers = {"Crypto-Pay-API-Token": CRYPTOBOT_API_TOKEN}
    
    data = {
        "asset": "USDT",
        "amount": str(amount_usdt),
        "pin_to_user_id": user_id if pin_to_user_id else None
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                'https://pay.crypt.bot/api/createCheck',
                headers=headers,
                json=data,
                timeout=10
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    if result.get('ok'):
                        check = result['result']
                        return check['bot_check_url'], str(check['check_id'])
                
                return None, None
                
    except Exception as e:
        logger.error(f"Ошибка создания чека: {e}")
        return None, None

async def check_invoice_status(invoice_id: str):
    headers = {"Crypto-Pay-API-Token": CRYPTOBOT_API_TOKEN}
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f'https://pay.crypt.bot/api/getInvoices?invoice_ids={invoice_id}',
                headers=headers,
                timeout=10
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    if result.get('ok') and result['result']['items']:
                        invoice = result['result']['items'][0]
                        return invoice['status']
                
                return None
                
    except Exception as e:
        logger.error(f"Ошибка проверки счета: {e}")
        return None

def create_deposit(user_id, amount, invoice_url, invoice_id):
    data = load_db()
    
    deposit_id = len(data['deposits']) + 1
    deposit = {
        'id': deposit_id,
        'user_id': user_id,
        'amount': amount,
        'status': 'pending',
        'invoice_url': invoice_url,
        'invoice_id': invoice_id,
        'created_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    data['deposits'].append(deposit)
    save_db(data)
    return deposit_id

def complete_deposit(invoice_id):
    data = load_db()
    
    for deposit in data['deposits']:
        if deposit['invoice_id'] == invoice_id and deposit['status'] == 'pending':
            deposit['status'] = 'completed'
            user_id = deposit['user_id']
            amount = deposit['amount']
            
            user_id_str = str(user_id)
            if user_id_str in data['menu_users']:
                user_data = data['menu_users'][user_id_str]
                user_data['balance'] += amount
                user_data['total_deposited'] += amount
                
                if user_data.get('referrer'):
                    referrer_id = user_data['referrer']
                    referrer_id_str = str(referrer_id)
                    referrer_data = data['menu_users'].get(referrer_id_str)
                    if referrer_data:
                        bonus = amount * 0.02
                        referrer_data['balance'] += bonus
                        referrer_data['total_referral_income'] = referrer_data.get('total_referral_income', 0) + bonus
                        
                        bonus_id = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{user_id}"
                        data['referral_bonuses'][bonus_id] = {
                            'referrer_id': referrer_id,
                            'user_id': user_id,
                            'user_username': user_data['username'],
                            'amount': amount,
                            'bonus': bonus,
                            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                
                data['transactions'].append({
                    'user_id': user_id_str,
                    'type': 'deposit',
                    'amount': amount,
                    'invoice_id': invoice_id,
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
                
                save_db(data)
                return user_id, amount, True
            else:
                user_data = get_or_create_user(user_id)
                user_data['balance'] += amount
                user_data['total_deposited'] += amount

                save_db(data)
                return user_id, amount, True
    
    return None, None, False

def create_withdrawal(user_id, amount):
    data = load_db()
    user_id_str = str(user_id)
    
    if user_id_str not in data['menu_users']:
        return None
    
    user_data = data['menu_users'][user_id_str]
    
    if user_data['balance'] < amount:
        return None
    
    user_data['balance'] -= amount
    user_data['total_withdrawn'] += amount
    
    withdrawal_id = len(data['withdrawals']) + 1
    withdrawal = {
        'id': withdrawal_id,
        'user_id': user_id,
        'amount': amount,
        'status': 'pending',
        'check_url': None,
        'check_id': None,
        'admin_approved': False,
        'created_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    data['transactions'].append({
        'user_id': user_id_str,
        'type': 'withdraw_pending',
        'amount': amount,
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })
    
    data['withdrawals'].append(withdrawal)
    save_db(data)
    return withdrawal_id

def update_withdrawal_check(withdrawal_id, check_url, check_id):
    data = load_db()
    
    for withdrawal in data['withdrawals']:
        if withdrawal['id'] == withdrawal_id:
            withdrawal['check_url'] = check_url
            withdrawal['check_id'] = check_id
            withdrawal['status'] = 'completed'

            for transaction in reversed(data['transactions']):
                if (transaction.get('user_id') == str(withdrawal['user_id']) and 
                    transaction.get('type') == 'withdraw_pending' and 
                    transaction.get('amount') == withdrawal['amount']):
                    transaction['type'] = 'withdraw'
                    transaction['check_id'] = check_id
                    break
            
            save_db(data)
            return True
    
    return False

def reject_withdrawal_func(withdrawal_id):
    data = load_db()
    
    for withdrawal in data['withdrawals']:
        if withdrawal['id'] == withdrawal_id:
            withdrawal['status'] = 'rejected'
            user_id = withdrawal['user_id']
            amount = withdrawal['amount']
            
            user_id_str = str(user_id)
            if user_id_str in data['menu_users']:
                data['menu_users'][user_id_str]['balance'] += amount
                
                for transaction in reversed(data['transactions']):
                    if (transaction.get('user_id') == user_id_str and 
                        transaction.get('type') == 'withdraw_pending' and 
                        transaction.get('amount') == amount):
                        transaction['type'] = 'withdraw_rejected'
                        break
            
            save_db(data)
            return True
    
    return False

def get_withdrawal(withdrawal_id):
    data = load_db()
    
    for withdrawal in data['withdrawals']:
        if withdrawal['id'] == withdrawal_id:
            return withdrawal
    
    return None

def get_pending_withdrawals():
    data = load_db()
    
    pending = []
    for withdrawal in data['withdrawals']:
        if withdrawal['status'] == 'pending':
            pending.append(withdrawal)
    
    return pending

@dp.message(DepositStates.waiting_amount)
async def process_deposit_amount(message: types.Message, state: FSMContext):
    try:
        amount_rub = float(message.text.replace(',', '.'))
        
        if amount_rub < 10:
            await message.answer("Минимальная сумма пополнения: 10 RUB")
            return
        
        user_id = message.from_user.id
        
        amount_usdt = round(amount_rub / USDT_RATE, 6)
        
        invoice_url, invoice_id = await get_pay_link(amount_usdt, f"deposit_{user_id}", "USDT")
        
        if not invoice_url:
            await message.answer("Ошибка при создании счета. Попробуйте позже.")
            await state.clear()
            return
        
        deposit_id = create_deposit(user_id, amount_rub, invoice_url, invoice_id)
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Оплатить", url=invoice_url)],
            [InlineKeyboardButton(text="Проверить оплату", callback_data=f"check_deposit_{invoice_id}")]
        ])
        
        await message.answer(
            f"Счет на {amount_rub:.2f} RUB ({amount_usdt:.6f} USDT) создан!\n\n"
            f"Ссылка для оплаты: {invoice_url}\n\n"
            f"После оплаты нажмите 'Проверить оплату'",
            reply_markup=keyboard
        )
        
        await state.clear()
        
    except ValueError:
        await message.answer("Введите число! Например: 100 или 100.50")
    except Exception as e:
        logger.error(f"Ошибка обработки депозита: {e}")
        await message.answer("Произошла ошибка при обработке запроса")

@dp.message(WithdrawStates.waiting_amount)
async def process_withdraw_amount(message: types.Message, state: FSMContext):
    try:
        amount_rub = float(message.text.replace(',', '.'))
        user_data = get_or_create_user(message.from_user.id)
        
        if amount_rub <= 0:
            await message.answer("Введите положительную сумму!")
            return
        
        if amount_rub > user_data['balance']:
            await message.answer(f"Максимальная сумма для вывода: {user_data['balance']:.2f} RUB")
            return
        
        if amount_rub < MIN_WITHDRAWAL:
            await message.answer(f"Минимальная сумма вывода: {MIN_WITHDRAWAL} RUB")
            return
        
        user_id = message.from_user.id
        username = message.from_user.username or 'без username'
        
        amount_usdt = round(amount_rub / USDT_RATE, 6)
        
        if amount_rub < THRESHOLD:
            withdrawal_id = create_withdrawal(user_id, amount_rub)
            
            if withdrawal_id is None:
                await message.answer("Недостаточно средств на балансе!")
                return
            
            check_url, check_id = await create_check(amount_usdt, user_id, pin_to_user_id=True)
            
            if not check_url:
                reject_withdrawal_func(withdrawal_id)
                await message.answer("Ошибка при создании чека для вывода. Попробуйте позже.")
                return
            
            update_withdrawal_check(withdrawal_id, check_url, check_id)
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Получить чек", url=check_url)]
            ])
            
            user_data = get_or_create_user(user_id)
            
            await message.answer(
                f"Вывод {amount_rub:.2f} RUB ({amount_usdt:.3f} USDT) готов!\n\n"
                f"🔗 Ссылка для получения: {check_url}\n\n"
                f"Чек привязан к вашему ID и может быть активирован только вами.\n"
                f"💰 Ваш баланс: {user_data['balance']:.2f} RUB",
                reply_markup=keyboard
            )
        else:
            withdrawal_id = create_withdrawal(user_id, amount_rub)
            
            if withdrawal_id is None:
                await message.answer("Недостаточно средств на балансе!")
                return
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve_withdraw_{withdrawal_id}"),
                    InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_withdraw_{withdrawal_id}")
                ]
            ])
            
            admin_text = f"""
🚨 ТРЕБУЕТСЯ ПОДТВЕРЖДЕНИЕ ВЫВОДА!

👤 Пользователь: @{username}
🆔 ID: {user_id}
💰 Сумма: {amount_rub:.2f} RUB ({amount_usdt:.6f} USDT)
📋 ID заявки: {withdrawal_id}
            """
            
            try:
                await bot.send_message(ADMIN_ID, admin_text, reply_markup=keyboard)
                await message.answer(f"✅ Заявка на вывод {amount_rub:.2f} RUB отправлена на проверку админу.\n💰 Средства временно заморожены.")
            except Exception as e:
                logger.error(f"Ошибка уведомления админа: {e}")
                await message.answer("❌ Ошибка отправки заявки. Попробуйте позже.")
                reject_withdrawal_func(withdrawal_id)
        
        await state.clear()
        
    except ValueError:
        await message.answer("Введите число! Например: 100 или 100.50")
    except Exception as e:
        logger.error(f"Ошибка обработки вывода: {e}")
        await message.answer("Произошла ошибка при обработке запроса")

@dp.callback_query(F.data == "back_to_main")
async def back_to_main(callback: types.CallbackQuery):
    await callback.message.edit_media(
        types.InputMediaPhoto(
            media=PHOTOS["start"],
            caption="Добро пожаловать в бота!\n\nВыберите раздел:"
        ),
        reply_markup=get_main_menu()
    )

@dp.callback_query(F.data == "payment_back")
async def payment_back(callback: types.CallbackQuery):
    await show_profile(callback)

@dp.callback_query(F.data == "active_games")
async def show_active_games(callback: types.CallbackQuery):
    data = load_db()
    active_games = []
    
    for game_id, game in data['games'].items():
        if game['status'] == 'waiting' and len(game['players']) < game['max_players']:
            active_games.append(game)
    
    if not active_games:
        await callback.message.edit_media(
            types.InputMediaPhoto(
                media=PHOTOS["games"],
                caption="🎮 Активные игры\n\nНет активных игр, ожидающих игроков.\n\nСоздайте игру в группе командой:\n/cub <ставка> - кубики\n/dart <ставка> - дартс"
            ),
            reply_markup=get_back_to_main()
        )
        return
    
    game_text = "🎮 Активные игры\n\n"
    
    keyboard = InlineKeyboardBuilder()
    
    for game in active_games[:30]:
        
        game_type = game['emoji']
        bet = game['bet']
        players = f"{len(game['players'])}/{game['max_players']}"
        
        button_text = f"{game_type} | {bet:.0f} RUB | {players}"
        
        if game.get('message_link'):
            keyboard.button(text=button_text, url=game['message_link'])
        else:
            try:
                chat_id = game['chat_id']
                message_id = game['message_id']
                
                if chat_id < 0:
                    chat_id_str = str(chat_id).replace('-100', '')
                    message_link = f"https://t.me/c/{chat_id_str}/{message_id}"
                    game['message_link'] = message_link
                    
                    data['games'][game['game_id']]['message_link'] = message_link
                    save_db(data)
                    
                    keyboard.button(text=button_text, url=message_link)
                else:
                    message_link = f"https://t.me/{(await bot.get_me()).username}?start=game_{game['game_id']}"
                    keyboard.button(text=button_text, url=message_link)
            except Exception as e:
                logger.error(f"Ошибка создания ссылки для игры {game['game_id']}: {e}")
                continue
    keyboard.button(text="Создать игру", url=GROUP_LINK)
    keyboard.adjust(1)
    
    
    await callback.message.edit_media(
        types.InputMediaPhoto(
            media=PHOTOS["games"],
            caption=game_text
        ),
        reply_markup=keyboard.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data == "profile")
async def show_profile(callback: types.CallbackQuery):
    user_id = str(callback.from_user.id)
    user_data = get_or_create_user(callback.from_user.id, callback.from_user.username)
    
    profile_text = f"""
Личный кабинет:

ID: {user_id}
Username: @{user_data['username']}

💰 Баланс: {user_data['balance']:.2f} RUB
📅 Дата регистрации: {user_data['registered']}

👥 Рефералов: {user_data['referrals']}
💸 Доход от рефералов: {user_data.get('total_referral_income', 0):.2f} RUB
    """
    
    await callback.message.edit_media(
        types.InputMediaPhoto(
            media=PHOTOS["profile"],
            caption=profile_text
        ),
        reply_markup=get_profile_menu()
    )

@dp.callback_query(F.data == "deposit")
async def start_deposit(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_media(
        types.InputMediaPhoto(
            media=PHOTOS["deposit"],
            caption="Введите сумму для пополнения (в RUB):\n\nМинимальная сумма: 10 RUB\nКурс: 1 USDT = 80 RUB"
        ),
        reply_markup=get_payment_back()
    )
    await state.set_state(DepositStates.waiting_amount)
    await callback.answer()

@dp.callback_query(F.data == "withdraw")
async def start_withdraw(callback: types.CallbackQuery, state: FSMContext):
    user_data = get_or_create_user(callback.from_user.id)
    
    if user_data['balance'] <= 0:
        await callback.message.edit_media(
            types.InputMediaPhoto(
                media=PHOTOS["withdraw"],
                caption="На вашем балансе недостаточно средств для вывода!"
            ),
            reply_markup=get_payment_back()
        )
        return
    
    await callback.message.edit_media(
        types.InputMediaPhoto(
            media=PHOTOS["withdraw"],
            caption=f"💰 Ваш баланс: {user_data['balance']:.2f} RUB\n\n"
                   f"📌 Минимальная сумма вывода: {MIN_WITHDRAWAL} RUB\n"
                   f"💱 Курс: 1 USDT = 80 RUB\n"
                   f"✏️ Введите сумму для вывода (в RUB):"
        ),
        reply_markup=get_payment_back()
    )
    await state.set_state(WithdrawStates.waiting_amount)
    await callback.answer()

@dp.callback_query(F.data == "stats")
async def show_stats(callback: types.CallbackQuery):
    user_data = get_or_create_user(callback.from_user.id)
    
    stats_text = f"""
📊 Статистика:

💰 Всего пополнено: {user_data['total_deposited']:.2f} RUB
💸 Всего выведено: {user_data['total_withdrawn']:.2f} RUB
🛒 Всего потрачено: {user_data['total_spent']:.2f} RUB

👥 Доход от рефералов: {user_data.get('total_referral_income', 0):.2f} RUB
🎟 Использовано промокодов: {len(user_data['used_promocodes'])}
📈 Приглашено рефералов: {user_data['referrals']}
    """
    
    await callback.message.edit_media(
        types.InputMediaPhoto(
            media=PHOTOS["profile"],
            caption=stats_text
        ),
        reply_markup=get_back_to_profile()
    )

@dp.callback_query(F.data == "make_gift")
async def make_gift_info(callback: types.CallbackQuery):
    await callback.message.edit_media(
        types.InputMediaPhoto(
            media=PHOTOS["profile"],
            caption="🎁 Сделать подарок\n\n"
                   "Для отправки подарка другому пользователю:\n"
                   "/gift <сумма> (в ответ на сообщение в группе)\n"
                   "/gift @username <сумма>\n"
                   "/gift ID <сумма>\n\n"
                   "📊 Лимиты:\n"
                   "Максимальная сумма подарка: 10000 RUB\n"
                   "Минимальная: 1 RUB"
        ),
        reply_markup=get_back_to_profile()
    )
    await callback.answer()

@dp.callback_query(F.data == "promocodes_menu")
async def show_promocodes_menu(callback: types.CallbackQuery):
    user_data = get_or_create_user(callback.from_user.id)
    
    promocodes_text = f"""
🎟 Промокоды

📋 Использованные промокоды: {', '.join(user_data['used_promocodes']) if user_data['used_promocodes'] else 'нет'}

Для активации промокода используйте команду:
/promo КОД

Пример: /promo BONUS100

ℹ️ Промокоды начисляют баланс на ваш счет (RUB).
ℹ️ Каждый промокод можно активировать только 1 раз на аккаунт.
    """
    
    await callback.message.edit_media(
        types.InputMediaPhoto(
            media=PHOTOS["profile"],
            caption=promocodes_text
        ),
        reply_markup=get_back_to_profile()
    )
    await callback.answer()

@dp.callback_query(F.data == "referral")
async def show_referral(callback: types.CallbackQuery):
    user_id = str(callback.from_user.id)
    user_data = get_or_create_user(callback.from_user.id)
    
    try:
        ref_link = await create_start_link(bot, user_id, encode=True)
    except Exception as e:
        logger.error(f"Ошибка создания реф. ссылки: {e}")
        ref_link = f"https://t.me/{(await bot.get_me()).username}?start={user_id}"
    
    referral_text = f"""
👥 Реферальная система

🔗 Ваша реферальная ссылка:
{ref_link}

📊 Ваших рефералов: {user_data['referrals']}
💰 Доход от рефералов: {user_data.get('total_referral_income', 0):.2f} RUB

🎯 За каждого приглашенного друга вы получаете 2% от его депозитов!
💸 Приглашайте друзей по вашей ссылке и получайте пассивный доход.

📝 Как это работает:
1. Друг регистрируется по вашей ссылке
2. Когда он пополняет баланс, вы получаете 2% от суммы
3. Бонус начисляется автоматически
    """
    
    await callback.message.edit_media(
        types.InputMediaPhoto(
            media=PHOTOS["profile"],
            caption=referral_text
        ),
        reply_markup=get_back_to_profile()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("check_deposit_"))
async def check_deposit(callback: types.CallbackQuery):
    try:
        invoice_id = callback.data.replace("check_deposit_", "")
        
        status = await check_invoice_status(invoice_id)
        
        if status == 'paid':
            user_id, amount_rub, success = complete_deposit(invoice_id)
            
            if success and user_id:
                user_data = get_or_create_user(user_id)
                
                await callback.message.edit_text(
                    f"✅ Оплата подтверждена!\n\n"
                    f"На ваш баланс зачислено {amount_rub:.2f} RUB\n"
                    f"Текущий баланс: {user_data['balance']:.2f} RUB"
                )
                
                if user_data.get('referrer'):
                    referrer_bonus = amount_rub * 0.02
                    try:
                        await bot.send_message(
                            user_data['referrer'],
                            f"💰 Ваш реферал @{user_data['username']} пополнил баланс на {amount_rub:.2f} RUB\n"
                            f"💸 Вам начислен бонус: {referrer_bonus:.2f} RUB (2%)\n"
                            f"📊 Ваш баланс: {get_or_create_user(user_data['referrer'])['balance']:.2f} RUB"
                        )
                    except Exception as e:
                        logger.error(f"Ошибка уведомления рефереру: {e}")
            else:
                await callback.message.edit_text("⚠️ Депозит не найден или ошибка обработки")
        
        elif status == 'expired':
            await callback.message.edit_text("⏰ Счет просрочен. Создайте новый счет.")
        elif status == 'active':
            await callback.answer("⌛ Счет еще не оплачен", show_alert=True)
        else:
            await callback.answer("❌ Не удалось проверить статус счета", show_alert=True)
    
    except Exception as e:
        logger.error(f"Ошибка проверки депозита: {e}")
        await callback.answer("❌ Ошибка проверки", show_alert=True)

@dp.callback_query(F.data.startswith("approve_withdraw_"))
async def approve_withdrawal(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Вы не админ!", show_alert=True)
        return
    
    try:
        withdrawal_id = int(callback.data.replace("approve_withdraw_", ""))
        withdrawal = get_withdrawal(withdrawal_id)
        
        if not withdrawal:
            await callback.answer("Заявка не найдена", show_alert=True)
            return
        
        amount_rub = withdrawal['amount']
        user_id = withdrawal['user_id']
        
        amount_usdt = round(amount_rub / USDT_RATE, 6)
        
        check_url, check_id = await create_check(amount_usdt, user_id, pin_to_user_id=True)
        
        if not check_url:
            await callback.message.edit_text("Ошибка при создании чека")
            return
        
        update_withdrawal_check(withdrawal_id, check_url, check_id)
        
        await callback.message.edit_text(
            f"✅ ВЫВОД ОДОБРЕН!\n\n"
            f"👤 ID: {user_id}\n"
            f"💰 {amount_rub:.2f} RUB ({amount_usdt:.6f} USDT)\n"
            f"🔗 {check_url}"
        )
        
        try:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Получить чек", url=check_url)]
            ])
            
            user_data = get_or_create_user(user_id)
            await bot.send_message(
                user_id, 
                f"✅ Ваш вывод {amount_rub:.2f} RUB ({amount_usdt:.6f} USDT) одобрен!\n\n"
                f"🔗 Ссылка для получения: {check_url}\n\n"
                f"ℹ️ Чек привязан к вашему ID и может быть активирован только вами.\n"
                f"💰 Ваш баланс: {user_data['balance']:.2f} RUB",
                reply_markup=keyboard
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления пользователя: {e}")
        
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка одобрения вывода: {e}")
        await callback.answer("Ошибка обработки", show_alert=True)

@dp.callback_query(F.data.startswith("reject_withdraw_"))
async def reject_withdrawal(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Вы не админ!", show_alert=True)
        return
    
    try:
        withdrawal_id = int(callback.data.replace("reject_withdraw_", ""))
        
        success = reject_withdrawal_func(withdrawal_id)
        
        if success:
            withdrawal = get_withdrawal(withdrawal_id)
            user_id = withdrawal['user_id']
            amount = withdrawal['amount']
            
            await callback.message.edit_text(f"❌ Вывод {amount:.2f} RUB отклонен")
            
            try:
                user_data = get_or_create_user(user_id)
                await bot.send_message(
                    user_id, 
                    f"❌ Ваш вывод {amount:.2f} RUB отклонен.\n"
                    f"💰 Средства возвращены на баланс.\n\n"
                    f"💳 Ваш баланс: {user_data['balance']:.2f} RUB"
                )
            except Exception as e:
                logger.error(f"Ошибка уведомления пользователя: {e}")
        else:
            await callback.message.edit_text("Заявка не найдена")
        
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка отклонения вывода: {e}")
        await callback.answer("Ошибка обработки", show_alert=True)

def not_forwarded(message: Message) -> bool:
    return message.forward_date is None

def create_game(emoji: str, command: str, auto_dice: bool = False) -> Callable:
    
    async def game_handler(message: Message):
        if not not_forwarded(message):
            return
        
        try:
            parts = message.text.split()
            cmd_with_params = parts[0][1:]

            num_param = None
            mode_param = None
            
            match = re.search(r'(\d+)([xpt])$', cmd_with_params)
            if match:
                num_param = int(match.group(1))
                mode_param = match.group(2)
                cmd_base = cmd_with_params[:match.start()]
            else:
                cmd_base = cmd_with_params
            
            if not cmd_base.startswith(command):
                return
            
            bet = 10
            if len(parts) > 1:
                try:
                    bet = float(parts[1])
                except ValueError:
                    await message.reply(f"Неверная ставка. Используйте: /{cmd_with_params} <ставка>")
                    return
            
            if bet < 10:
                await message.reply(f"Ставка не может быть меньше 10 RUB")
                return

            max_players = 2
            game_mode = 'classic'
            dice_per_player = 1
            total_rounds = 1
            
            if mode_param == 'x':
                max_wins = num_param if num_param else 1
                game_mode = 'wins'
                dice_per_player = 1
                total_rounds = max_wins * 2
                
            elif mode_param == 't':
                dice_per_player = num_param if num_param else 5
                game_mode = 'total'
                total_rounds = 1
                
            elif mode_param == 'p': 
                max_players = min(max(num_param if num_param else 2, 2), 5)
                game_mode = 'players'
                dice_per_player = 1
                total_rounds = 1

            dice_source = 'bots_support_only'
            
            user = message.from_user
            username = user.username or user.first_name
            
            logger.info(f"[DEBUG] create_game: user={user.id}, bet={bet}")
            
            balance = get_user_balance(user.id)
            
            if balance < bet:
                await message.reply(f"Недостаточно средств. Баланс: {balance:.2f} RUB")
                return
            
            success = update_user_balance(user.id, bet, "game_bet")
            if not success:
                await message.reply("❌ Ошибка списания ставки. Попробуйте снова.")
                return
            
            data = load_db()
            game_counter = data.get('game_counter', 0)
            game_counter += 1
            game_id = str(game_counter)
            
            game = {
                'game_id': game_id,
                'emoji': emoji,
                'command': command,
                'creator_id': user.id,
                'creator_name': username,
                'max_players': max_players,
                'players': [user.id],
                'player_names': [username],
                'player_scores': {username: 0},
                'player_dice': {username: []},
                'player_wins': {username: 0},
                'player_round_wins': {username: 0},
                'round_scores': {username: []},
                'status': 'waiting',
                'chat_id': message.chat.id,
                'message_id': None,
                'bet': bet,
                'current_round': 1,
                'rounds_completed': 0,
                'game_mode': game_mode,
                'target_wins': max_wins if mode_param == 'x' else 1,
                'dice_per_player': dice_per_player,
                'total_rounds': total_rounds,
                'dice_source': dice_source,
                'auto_dice': auto_dice,
                'current_player_index': 0,
                'current_player_id': user.id,
                'bot_dice_thrown': False,
                'round_results': [],
                'last_round_update': None,
                'message_link': None
            }
            
            data['games'][game_id] = game
            data['game_counter'] = game_counter
            save_db(data)
            
            logger.info(f"[DEBUG] Game #{game_id} created, bet {bet} deducted")
            
            new_balance = get_user_balance(user.id)
            
            mode_display = ""
            if mode_param == 'x':
                mode_display = f"{game['target_wins']}WIN"
            elif mode_param == 't':
                mode_display = f"{dice_per_player}TOTAL"
            elif mode_param == 'p':
                mode_display = f"{max_players}PLAYER"
            else:
                mode_display = "CLASSIC"
            
            players_list = ""
            for i, (player_id, player_name) in enumerate(zip(game['players'], game['player_names']), 1):
                num_emoji = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣"][i-1]
                score = game['player_scores'][player_name]
                
                if player_name.startswith('user_'):
                    player_display = str(player_id)
                else:
                    player_display = f"@{player_name}"
                
                players_list += f"{num_emoji} - {player_display} [{score}]\n"
            
            game_text = (
                f"{emoji} {command.upper()} {mode_display} №{game_id}\n\n"
                f"👥 Игроки:\n{players_list}\n"
                f"💰 Ставка: {bet:.1f} RUB (списана)\n"
                f"📊 Ваш баланс: {new_balance:.2f} RUB"
            )
            
            keyboard = InlineKeyboardBuilder()
            keyboard.button(
                text=f"🎮 Присоединиться (1/{max_players})",
                callback_data=f"join_game:{game_id}"
            )
            
            sent_message = await message.reply(
                game_text,
                reply_markup=keyboard.as_markup()
            )
            
            try:
                chat_id = message.chat.id
                message_id = sent_message.message_id
                
                if chat_id < 0:
                    chat_id_str = str(chat_id).replace('-100', '')
                    message_link = f"https://t.me/c/{chat_id_str}/{message_id}"
                    game['message_link'] = message_link
                else:
                    message_link = f"https://t.me/{(await bot.get_me()).username}?start=game_{game_id}"
                    game['message_link'] = message_link
                
                logger.info(f"[DEBUG] Message link created: {message_link}")
            except Exception as e:
                logger.error(f"Ошибка создания ссылки для игры #{game_id}: {e}")

            data = load_db()
            if game_id in data['games']:
                data['games'][game_id]['message_id'] = sent_message.message_id
                data['games'][game_id]['message_link'] = game.get('message_link')
                save_db(data)
            
            logger.info(f"[DEBUG] Game #{game_id} setup complete")
            
        except Exception as e:
            logger.error(f"Ошибка создания игры: {e}", exc_info=True)
            await message.reply("Ошибка создания игры")
    
    return game_handler

cube = create_game("🎲", "cub", auto_dice=True)
dart = create_game("🎯", "dart", auto_dice=True)
basket = create_game("🏀", "basket", auto_dice=True)
bowling = create_game("🎳", "bowl", auto_dice=True)
football = create_game("⚽", "foot", auto_dice=True)

@dp.message(F.text.regexp(r'^/(cub\d*[xpt]?)\s+(\d+)$'))
async def handle_cub_command(message: Message):
    await cube(message)

@dp.message(F.text.regexp(r'^/(dart\d*[xpt]?)\s+(\d+)$'))
async def handle_dart_command(message: Message):
    await dart(message)

@dp.message(F.text.regexp(r'^/(basket\d*[xpt]?)\s+(\d+)$'))
async def handle_basket_command(message: Message):
    await basket(message)

@dp.message(F.text.regexp(r'^/(bowl\d*[xpt]?)\s+(\d+)$'))
async def handle_bowling_command(message: Message):
    await bowling(message)

@dp.message(F.text.regexp(r'^/(foot\d*[xpt]?)\s+(\d+)$'))
async def handle_football_command(message: Message):
    await football(message)

@dp.message(F.text.regexp(r'^/21cub\s+(\d+)$'))
async def cmd_21cub(message: Message):
    if not not_forwarded(message):
        return
    
    try:
        parts = message.text.split()
        bet = float(parts[1])
        
        if bet < 10:
            await message.reply("Ставка не может быть меньше 10 RUB")
            return
        
        user = message.from_user
        username = user.username or user.first_name
        
        balance = get_user_balance(user.id)
        
        if balance < bet:
            await message.reply(f"Недостаточно средств. Баланс: {balance:.2f} RUB")
            return
        
        success = update_user_balance(user.id, bet, "game_bet")
        if not success:
            await message.reply("❌ Ошибка списания ставки. Попробуйте снова.")
            return
        
        data = load_db()
        game_counter = data.get('game_counter', 0)
        game_counter += 1
        game_id = str(game_counter)
        
        game = {
            'game_id': game_id,
            'emoji': '🎲',
            'command': '21cub',
            'creator_id': user.id,
            'creator_name': username,
            'max_players': 2,
            'players': [user.id],
            'player_names': [username],
            'player_scores': {username: 0},
            'player_dice': {username: []},
            'player_wins': {username: 0},
            'player_round_wins': {username: 0},
            'round_scores': {username: []},
            'status': 'waiting',
            'chat_id': message.chat.id,
            'message_id': None,
            'bet': bet,
            'current_round': 1,
            'rounds_completed': 0,
            'game_mode': '21game',
            'target_wins': 21,
            'dice_per_player': 5,
            'total_rounds': 1,
            'dice_source': 'bots_support_only',
            'auto_dice': False,
            'current_player_index': 0,
            'current_player_id': user.id,
            'bot_dice_thrown': False,
            'round_results': [],
            'last_round_update': None,
            'message_link': None
        }
        
        data['games'][game_id] = game
        data['game_counter'] = game_counter
        save_db(data)
        
        logger.info(f"Создана игра 21 #{game_id}, ставка: {bet} RUB (списана сразу)")
        
        players_list = ""
        for i, (player_id, player_name) in enumerate(zip(game['players'], game['player_names']), 1):
            num_emoji = ["1️⃣", "2️⃣"][i-1]
            score = game['player_scores'][player_name]
            
            if player_name.startswith('user_'):
                player_display = str(player_id)
            else:
                player_display = f"@{player_name}"
            
            players_list += f"{num_emoji} - {player_display} [{score}]\n"
        
        game_text = (
            f"🎲 21CUB №{game_id}\n\n"
            f"👥 Игроки:\n{players_list}\n"
            f"💰 Ставка: {bet:.1f} RUB (списана)\n"
            f"📊 Ваш баланс: {balance - bet:.2f} RUB"
        )
        
        keyboard = InlineKeyboardBuilder()
        keyboard.button(
            text="🎮 Присоединиться (1/2)",
            callback_data=f"join_game:{game_id}"
        )
        
        sent_message = await message.reply(
            game_text,
            reply_markup=keyboard.as_markup()
        )
        
        try:
            chat_id = message.chat.id
            message_id = sent_message.message_id
            
            if chat_id < 0:
                chat_id_str = str(chat_id).replace('-100', '')
                message_link = f"https://t.me/c/{chat_id_str}/{message_id}"
                game['message_link'] = message_link
            else:
                message_link = f"https://t.me/{(await bot.get_me()).username}?start=game_{game_id}"
                game['message_link'] = message_link
            
            logger.info(f"Создана ссылка для игры 21 #{game_id}: {message_link}")
        except Exception as e:
            logger.error(f"Ошибка создания ссылки для игры 21 #{game_id}: {e}")
        
        data = load_db()
        if game_id in data['games']:
            data['games'][game_id]['message_id'] = sent_message.message_id
            data['games'][game_id]['message_link'] = game.get('message_link')
            save_db(data)
        
        logger.info(f"Сообщение игры 21 #{game_id} создано с ID: {sent_message.message_id}")
        
    except Exception as e:
        logger.error(f"Ошибка создания игры 21: {e}", exc_info=True)
        await message.reply("Ошибка создания игры")

async def update_game_message(game_id: str):
    try:
        data = load_db()
        game = data['games'].get(game_id)
        if not game:
            logger.error(f"Игра #{game_id} не найдена в БД")
            return
        
        mode_display = ""
        if game['game_mode'] == 'wins':
            mode_display = f"{game.get('target_wins', 1)}WIN"
        elif game['game_mode'] == 'total':
            mode_display = f"{game.get('dice_per_player', 1)}TOTAL"
        elif game['game_mode'] == 'players':
            mode_display = f"{game.get('max_players', 2)}PLAYER"
        elif game['game_mode'] == '21game':
            mode_display = "21GAME"
        else:
            mode_display = "CLASSIC"
        
        players_list = ""
        for i, (player_id, player_name) in enumerate(zip(game['players'], game['player_names']), 1):
            num_emoji = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣"][i-1]
            score = game['player_scores'].get(player_name, 0)
            
            if player_name.startswith('user_'):
                player_display = str(player_id)
            else:
                player_display = f"@{player_name}"
            
            if game['status'] == 'playing' and i-1 == game.get('current_player_index', 0):
                players_list += f"{num_emoji} - {player_display}\n"
            else:
                players_list += f"{num_emoji} - {player_display}\n"
        
        if game['command'] == '21cub':
            game_text = (
                f"🎲 21CUB №{game_id}\n\n"
                f"👥 Игроки:\n{players_list}\n"
                f"💰 Ставка: {game['bet']:.1f} RUB"
            )
        else:
            game_text = (
                f"{game['emoji']} {game['command'].upper()} {mode_display} №{game_id}\n\n"
                f"👥 Игроки:\n{players_list}\n"
                f"💰 Ставка: {game['bet']:.1f} RUB"
            )
        
        keyboard = InlineKeyboardBuilder()
        if game['status'] == 'waiting' and len(game['players']) < game['max_players']:
            keyboard.button(
                text=f"🎮 Присоединиться ({len(game['players'])}/{game['max_players']})",
                callback_data=f"join_game:{game_id}"
            )
        
        try:
            await bot.edit_message_text(
                chat_id=game['chat_id'],
                message_id=game['message_id'],
                text=game_text,
                reply_markup=keyboard.as_markup() if keyboard.buttons else None
            )
        except Exception as e:
            if "message is not modified" in str(e):
                game_text += "\u200b"
                try:
                    await bot.edit_message_text(
                        chat_id=game['chat_id'],
                        message_id=game['message_id'],
                        text=game_text,
                        reply_markup=keyboard.as_markup() if keyboard.buttons else None
                    )
                    logger.error("попытка обновить сообщение")
                except Exception as e2:
                    logger.error(f"❌ Ошибка при втором обновлении: {e2}")
            else:
                logger.error(f"❌ Ошибка обновления сообщения игры: {e}")
        
    except Exception as e:
        logger.error(f"Ошибка обновления сообщения игры: {e}", exc_info=True)

@dp.callback_query(F.data.startswith("join_game:"))
async def handle_join_game(callback: CallbackQuery):
    try:
        game_id = callback.data.split(":")[1]
        
        logger.info(f"[DEBUG] handle_join_game START: user={callback.from_user.id}, game_id={game_id}")
        
        data = load_db()
        if game_id not in data['games']:
            logger.error(f"[DEBUG] Game {game_id} not found in DB")
            await callback.answer("Игра не найдена", show_alert=True)
            return
        
        game = data['games'][game_id]
        user = callback.from_user
        user_id_str = str(user.id)
        
        if user.username:
            username = user.username
        else:
            username = str(user.id)
        
        logger.info(f"[DEBUG] User info: id={user.id}, username={username}")
        logger.info(f"[DEBUG] Game info: bet={game['bet']}, players={game.get('players', [])}")
        
        logger.info(f"[DEBUG] Step 1: get_or_create_user")
        user_data = get_or_create_user(user.id, username)
        
        logger.info(f"[DEBUG] Step 2: reload DB after user creation")
        data = load_db()
        
        if game_id not in data['games']:
            logger.error(f"[DEBUG] Game {game_id} disappeared after reload!")
            await callback.answer("Ошибка: игра не найдена", show_alert=True)
            return
        
        game = data['games'][game_id]
        
        if user_id_str not in data['menu_users']:
            logger.error(f"[DEBUG] User {user_id_str} not in DB after get_or_create_user!")
            await callback.answer("❌ Ошибка: пользователь не создан", show_alert=True)
            return
        
        current_balance = data['menu_users'][user_id_str]['balance']
        logger.info(f"[DEBUG] Current balance for {user_id_str}: {current_balance}")
        
        if current_balance < game['bet']:
            logger.error(f"[DEBUG] Insufficient balance: {current_balance} < {game['bet']}")
            await callback.answer(f"Нужно {game['bet']} RUB\nВаш баланс: {current_balance:.2f} RUB", show_alert=True)
            return
        
        if user.id in game['players']:
            logger.info(f"[DEBUG] User {user.id} already in game")
            await callback.answer("Вы уже в игре", show_alert=True)
            return
        
        if len(game['players']) >= game['max_players']:
            logger.info(f"[DEBUG] Game {game_id} is full")
            await callback.answer("Игра заполнена", show_alert=True)
            return
        
        if game['status'] != 'waiting':
            logger.info(f"[DEBUG] Game {game_id} already started")
            await callback.answer("Игра уже началась", show_alert=True)
            return
        
        logger.info(f"[DEBUG] Step 3: deducting bet {game['bet']} from {user_id_str}")
        
        success = update_user_balance(user.id, game['bet'], "game_bet")
        
        logger.info(f"[DEBUG] update_user_balance result: {success}")
        
        if not success:
            await callback.answer("❌ Ошибка списания ставки", show_alert=True)
            return
        

        logger.info(f"[DEBUG] Step 4: reload DB after bet deduction")
        data = load_db()
        game = data['games'][game_id]
        
        if user_id_str in data['menu_users']:
            new_balance = data['menu_users'][user_id_str]['balance']
            total_spent = data['menu_users'][user_id_str].get('total_spent', 0)
            logger.info(f"[DEBUG] After deduction: balance={new_balance}, total_spent={total_spent}")
        
        game['players'].append(user.id)
        game['player_names'].append(username)
        game['player_scores'][username] = 0
        game['player_dice'][username] = []
        game['player_wins'][username] = 0
        game['player_round_wins'][username] = 0
        game['round_scores'][username] = []
        
        save_db(data)
        logger.info(f"[DEBUG] User {user_id_str} added to game {game_id}")
        
        await callback.answer(f"✅ Вы присоединились к игре! Ставка {game['bet']} RUB списана")
        
        await asyncio.sleep(0.5)
        await update_game_message(game_id)
        
        if len(game['players']) == game['max_players']:
            logger.info(f"[DEBUG] Game {game_id} is full, starting...")
            await start_game(game_id)
        else:
            logger.info(f"[DEBUG] Game {game_id} waiting for more players")
        
    except Exception as e:
        logger.error(f"[DEBUG] ERROR in handle_join_game: {e}", exc_info=True)
        await callback.answer("❌ Ошибка присоединения", show_alert=True)

async def start_game(game_id):
    try:
        data = load_db()
        if game_id not in data['games']:
            logger.error(f"Игра #{game_id} не найдена при запуске")
            return
        
        game = data['games'][game_id]
        
        if game['status'] == 'playing':
            logger.info(f"Игра #{game_id} уже запущена")
            return
        
        game['status'] = 'playing'
        logger.info(f"Начинаем игру #{game_id}")


        data['games'][game_id] = game
        save_db(data)
        
        logger.info(f"✅ Ставки уже списаны ранее для игры #{game_id}")
        
        await update_game_message(game_id)

        dice_source = game.get('dice_source', 'bots_support_only')
        
        if dice_source == 'bot_single_main':
            await bot_single_dice_game_main_only(game_id)
        elif dice_source == 'bots_support_only':
            game_mode = game.get('game_mode', 'classic')
            if game_mode == 'wins':
                await play_wins_mode_support_only(game_id)
            elif game_mode == 'total':
                await total_mode_support_only(game_id)
            elif game_mode == '21game':
                await play_21game_support_only(game_id)
            elif game_mode == 'players':
                await players_mode_support_only(game_id)
            else:
                await classic_mode_support_only(game_id)
        else:
            await update_game_message(game_id)
            
    except Exception as e:
        logger.error(f"Ошибка начала игры: {e}", exc_info=True)

async def bot_single_dice_game_main_only(game_id):
    try:
        data = load_db()
        game = data['games'][game_id]
        chat_id = game['chat_id']
        emoji = game['emoji']
        
        player_scores = {}
        
        for player_name in game['player_names']:
            try:
                dice_message = await bot.send_dice(chat_id=chat_id, emoji=emoji)
                dice_value = dice_message.dice.value
                
                game['player_dice'][player_name].append(dice_value)
                player_scores[player_name] = dice_value
                
                await asyncio.sleep(1.2)
                
            except Exception as e:
                logger.error(f"Ошибка броска для {player_name}: {e}")
                game['player_dice'][player_name].append(0)
                player_scores[player_name] = 0
        
        await finish_game(game, player_scores)
        
    except Exception as e:
        logger.error(f"Ошибка в bot_single_dice_game_main_only: {e}")

async def classic_mode_support_only(game_id):
    try:
        data = load_db()
        game = data['games'][game_id]
        chat_id = game['chat_id']
        emoji = game['emoji']
        dice_per_player = game.get('dice_per_player', 1)
        
        support_tokens = await get_support_bot_tokens(dice_per_player, game.get('current_round', 1))
        
        logger.info(f"Игра #{game['game_id']}: классический режим, {dice_per_player} саппорт ботов")
        
        player_scores = {}
        
        for player_index, player_name in enumerate(game['player_names']):
            player_total = 0
            player_dice_values = []
            
            for i in range(dice_per_player):
                try:
                    token = support_tokens[i % len(support_tokens)]
                    value = await send_dice_from_support_bot(token, chat_id, emoji)
                    
                    player_dice_values.append(value)
                    player_total += value
                    
                    await asyncio.sleep(1.2)
                    
                except Exception as e:
                    logger.error(f"Ошибка броска для {player_name}: {e}")
                    value = random.randint(1, 6)
                    player_dice_values.append(value)
                    player_total += value
            
            game['player_dice'][player_name] = player_dice_values
            player_scores[player_name] = player_total
            game['player_scores'][player_name] = player_total
            
            # Обновляем текущего игрока
            if player_index < len(game['player_names']) - 1:
                game['current_player_index'] = player_index + 1
            else:
                game['current_player_index'] = 0
            
            await update_game_message(game_id)
            
            await asyncio.sleep(0.5)
        
        await finish_game(game, player_scores)
        
    except Exception as e:
        logger.error(f"Ошибка в classic_mode_support_only: {e}")

async def total_mode_support_only(game_id):
    try:
        data = load_db()
        game = data['games'][game_id]
        chat_id = game['chat_id']
        emoji = game['emoji']
        dice_per_player = game.get('dice_per_player', 5)

        support_tokens = await get_support_bot_tokens(dice_per_player, game.get('current_round', 1))
        
        logger.info(f"Игра #{game['game_id']}: режим total, {dice_per_player} саппорт ботов")
        
        player_scores = {}
        
        for player_index, player_name in enumerate(game['player_names']):
            player_total = 0
            player_dice_values = []
            
            for i in range(dice_per_player):
                try:
                    token = support_tokens[i % len(support_tokens)]
                    value = await send_dice_from_support_bot(token, chat_id, emoji)
                    
                    player_dice_values.append(value)
                    player_total += value
                    
                    await asyncio.sleep(1.2)
                    
                except Exception as e:
                    logger.error(f"Ошибка броска {i+1} для {player_name}: {e}")
                    value = random.randint(1, 6)
                    player_dice_values.append(value)
                    player_total += value
            
            game['player_dice'][player_name] = player_dice_values
            player_scores[player_name] = player_total
            game['player_scores'][player_name] = player_total
            
            # Обновляем текущего игрока
            if player_index < len(game['player_names']) - 1:
                game['current_player_index'] = player_index + 1
            else:
                game['current_player_index'] = 0
            
            await update_game_message(game_id)
            
            await asyncio.sleep(0.5)
        
        await finish_game(game, player_scores)
        
    except Exception as e:
        logger.error(f"Ошибка в total_mode_support_only: {e}")

async def players_mode_support_only(game_id):
    try:
        data = load_db()
        game = data['games'][game_id]
        chat_id = game['chat_id']
        emoji = game['emoji']
        dice_per_player = 1

        support_tokens = await get_support_bot_tokens(dice_per_player, game.get('current_round', 1))
        
        logger.info(f"Игра #{game['game_id']}: режим players, {dice_per_player} саппорт ботов")
        
        player_scores = {}
        
        for player_index, player_name in enumerate(game['player_names']):
            player_total = 0
            player_dice_values = []
            
            for i in range(dice_per_player):
                try:
                    token = support_tokens[i % len(support_tokens)]
                    value = await send_dice_from_support_bot(token, chat_id, emoji)
                    
                    player_dice_values.append(value)
                    player_total += value
                    
                    await asyncio.sleep(1.2)
                    
                except Exception as e:
                    logger.error(f"Ошибка броска для {player_name}: {e}")
                    value = random.randint(1, 6)
                    player_dice_values.append(value)
                    player_total += value
            
            game['player_dice'][player_name] = player_dice_values
            player_scores[player_name] = player_total
            game['player_scores'][player_name] = player_total
            
            # Обновляем текущего игрока
            if player_index < len(game['player_names']) - 1:
                game['current_player_index'] = player_index + 1
            else:
                game['current_player_index'] = 0
            
            await update_game_message(game_id)
            
            await asyncio.sleep(0.5)
        
        await finish_game(game, player_scores)
        
    except Exception as e:
        logger.error(f"Ошибка в players_mode_support_only: {e}")

async def play_21game_support_only(game_id):
    try:
        data = load_db()
        game = data['games'][game_id]
        chat_id = game['chat_id']
        emoji = game['emoji']

        dice_per_player = 5
        support_tokens = await get_support_bot_tokens(dice_per_player, game.get('current_round', 1))
        
        logger.info(f"Игра 21 #{game['game_id']}: {dice_per_player} саппорт ботов")
        
        player_scores = {}
        
        for player_index, player_name in enumerate(game['player_names']):
            player_total = 0
            player_dice_values = []
            
            for i in range(dice_per_player):
                try:
                    token = support_tokens[i % len(support_tokens)]
                    value = await send_dice_from_support_bot(token, chat_id, emoji)
                    
                    player_dice_values.append(value)
                    player_total += value
                    
                    await asyncio.sleep(1.2)
                    
                except Exception as e:
                    logger.error(f"Ошибка броска для {player_name}: {e}")
                    value = random.randint(1, 6)
                    player_dice_values.append(value)
                    player_total += value
            
            game['player_dice'][player_name] = player_dice_values
            player_scores[player_name] = player_total
            game['player_scores'][player_name] = player_total
            
            # Обновляем текущего игрока
            if player_index < len(game['player_names']) - 1:
                game['current_player_index'] = player_index + 1
            else:
                game['current_player_index'] = 0
            
            await update_game_message(game_id)
            
            await asyncio.sleep(0.5)
        
        await finish_21game(game, player_scores)
        
    except Exception as e:
        logger.error(f"Ошибка в play_21game_support_only: {e}")

async def play_wins_mode_support_only(game_id):
    try:
        data = load_db()
        game = data['games'][game_id]
        chat_id = game['chat_id']
        emoji = game['emoji']
        target_wins = game.get('target_wins', 1)
        dice_per_player = game.get('dice_per_player', 1)
        
        logger.info(f"Игра #{game['game_id']}: режим до {target_wins} побед")
        
        round_number = 1
        
        while True:
            max_wins = 0
            for player_name in game['player_names']:
                wins = game['player_round_wins'].get(player_name, 0)
                if wins > max_wins:
                    max_wins = wins
            
            if max_wins >= target_wins:
                winners = []
                for player_name in game['player_names']:
                    if game['player_round_wins'].get(player_name, 0) >= target_wins:
                        winners.append(player_name)
                
                if winners:
                    await finish_wins_game(game, winners)
                return
 
            support_tokens = await get_support_bot_tokens(dice_per_player, round_number)

            round_scores = {}
            
            for player_index, player_name in enumerate(game['player_names']):
                player_total = 0
                player_dice_values = []
                
                for i in range(dice_per_player):
                    try:
                        token = support_tokens[i % len(support_tokens)]
                        value = await send_dice_from_support_bot(token, chat_id, emoji)
                        
                        player_dice_values.append(value)
                        player_total += value
                        
                        await asyncio.sleep(1.2)
                        
                    except Exception as e:
                        logger.error(f"Ошибка броска в раунде {round_number}: {e}")
                        value = random.randint(1, 6)
                        player_dice_values.append(value)
                        player_total += value

                if player_name not in game['player_dice']:
                    game['player_dice'][player_name] = []
                game['player_dice'][player_name].extend(player_dice_values)
                round_scores[player_name] = player_total

                game['player_scores'][player_name] = game['player_scores'].get(player_name, 0) + player_total
                
                # Обновляем текущего игрока для отображения в сообщении
                if player_index < len(game['player_names']) - 1:
                    game['current_player_index'] = player_index + 1
                else:
                    game['current_player_index'] = 0
                
                await update_game_message(game_id)
                
                await asyncio.sleep(0.5)

            max_round_score = max(round_scores.values())
            round_winners = [name for name, score in round_scores.items() if score == max_round_score]

            round_result = {
                'round': round_number,
                'scores': round_scores.copy(),
                'winners': round_winners.copy()
            }
            game['round_results'].append(round_result)

            if len(round_winners) == 1:
                winner = round_winners[0]
                game['player_round_wins'][winner] = game['player_round_wins'].get(winner, 0) + 1

            await update_game_message(game_id)
            
            round_number += 1
            game['current_round'] = round_number
            
            data = load_db()
            data['games'][game_id] = game
            save_db(data)
            
            await asyncio.sleep(1)
            
    except Exception as e:
        logger.error(f"Ошибка в play_wins_mode_support_only: {e}")
        await finish_game(game, {})

@dp.message(F.dice, F.func(not_forwarded))
async def handle_dice(message: Message):
    try:
        user_id = message.from_user.id
        
        # Определяем имя пользователя: юзернейм или ID
        if message.from_user.username:
            username = message.from_user.username
        else:
            username = str(user_id)
        
        data = load_db()
        current_game_id = None
        current_game = None
        
        for game_id, game in data['games'].items():
            if game.get('status') == 'playing' and game.get('dice_source') == 'players':
                if game.get('current_player_id') == user_id:
                    dice_emoji = game.get('emoji', '🎲')
                    if message.dice.emoji != dice_emoji:
                        continue
                    current_game_id = game_id
                    current_game = game
                    break
        
        if not current_game:
            return
        
        dice_value = message.dice.value
        
        current_game['player_dice'][username].append(dice_value)
        current_game['player_scores'][username] += dice_value

        await update_game_message(current_game_id)

        if current_game['game_mode'] == 'wins':
            all_players_thrown = True
            for player_name in current_game['player_names']:
                if len(current_game['player_dice'][player_name]) < current_game['current_round']:
                    all_players_thrown = False
                    break
            
            if all_players_thrown:
                round_scores = {}
                for player_name in current_game['player_names']:
                    last_dice_index = len(current_game['player_dice'][player_name]) - 1
                    round_scores[player_name] = current_game['player_dice'][player_name][last_dice_index]
                
                max_score = max(round_scores.values())
                round_winners = [name for name, score in round_scores.items() if score == max_score]
                
                if len(round_winners) == 1:
                    winner = round_winners[0]
                    current_game['player_round_wins'][winner] = current_game.get('player_round_wins', {}).get(winner, 0) + 1
                
                await update_game_message(current_game_id)
                
                if current_game['player_round_wins'].get(winner, 0) >= current_game['target_wins']:
                    winners = []
                    for player_name in current_game['player_names']:
                        if current_game['player_round_wins'].get(player_name, 0) >= current_game['target_wins']:
                            winners.append(player_name)
                    
                    if winners:
                        await finish_wins_game(current_game, winners)
                    return
        
        current_idx = current_game.get('current_player_index', 0) + 1
        total_players = len(current_game.get('players', []))
        
        if current_idx >= total_players:
            current_game['rounds_completed'] = current_game.get('rounds_completed', 0) + 1

            game_finished = False
            
            if current_game['game_mode'] == '21game':
                if current_game.get('rounds_completed', 0) >= current_game.get('total_rounds', 5):
                    game_finished = True
            elif current_game['game_mode'] in ['total', 'players', 'classic']:
                if current_game.get('rounds_completed', 0) >= 1:
                    game_finished = True
            
            if game_finished:
                player_scores = {}
                if current_game['game_mode'] in ['total', '21game']:
                    for player_name in current_game['player_names']:
                        dice_values = current_game['player_dice'].get(player_name, [])
                        player_scores[player_name] = sum(dice_values)
                else:
                    for player_name in current_game['player_names']:
                        dice_values = current_game['player_dice'].get(player_name, [])
                        player_scores[player_name] = dice_values[-1] if dice_values else 0
                
                if current_game['game_mode'] == '21game':
                    await finish_21game(current_game, player_scores)
                else:
                    await finish_game(current_game, player_scores)
                return
            
            current_game['current_round'] = current_game.get('current_round', 1) + 1
            current_game['current_player_index'] = 0
            current_game['current_player_id'] = current_game['players'][0]
        else:
            current_game['current_player_index'] = current_idx
            current_game['current_player_id'] = current_game['players'][current_idx]
        
        await update_game_message(current_game_id)
        
        data['games'][current_game_id] = current_game
        save_db(data)
        
    except Exception as e:
        logger.error(f"Ошибка обработки дайса: {e}")

async def finish_game(game, player_scores):
    try:
        game_id = game['game_id']
        chat_id = game['chat_id']
        message_id = game['message_id']
        
        dice_emoji = game.get('emoji', '🎲')
        command_name = game.get('command', 'game').upper()
        
        mode_display = ""
        if game['game_mode'] == 'wins':
            mode_display = f"{game.get('target_wins', 1)}WIN"
        elif game['game_mode'] == 'total':
            mode_display = f"{game.get('dice_per_player', 1)}TOTAL"
        elif game['game_mode'] == 'players':
            mode_display = f"{game.get('max_players', 2)}PLAYER"
        elif game['game_mode'] == '21game':
            mode_display = "21GAME"
        else:
            mode_display = "CLASSIC"
        
        results_text = f"{dice_emoji} {command_name} {mode_display} №{game_id}\n\n"
        results_text += "📊 Результаты:\n\n"
        
        for player_name in game.get('player_names', []):
            dice_values = game['player_dice'].get(player_name, [])
            score = player_scores.get(player_name, 0)
            
            if game['game_mode'] == 'total':
                dice_str = " + ".join(str(d) for d in dice_values)
                results_text += f"@{player_name}: {dice_str} = {score}\n"
            else:
                if dice_values:
                    results_text += f"@{player_name}: {dice_values[-1]} очков\n"
        
        results_text += "\n"

        max_score = max(player_scores.values())
        winners = [name for name, score in player_scores.items() if score == max_score]
        
        pot = game['bet'] * len(game['players'])
        
        if len(winners) == 1:
            winner = winners[0]
            winner_id = None
            for uid, name in zip(game['players'], game['player_names']):
                if name == winner:
                    winner_id = uid
                    break
            
            if winner_id:
                update_user_balance(winner_id, pot, "game_win")
            
            results_text += f"🏆 Победитель: @{winner}\n"
            results_text += f"💰 Выигрыш: {pot} RUB"
        else:
            results_text += f"🤝 Ничья: " + ", ".join([f"@{w}" for w in winners]) + "\n"
            
            for player_name in game['player_names']:
                for uid, name in zip(game['players'], game['player_names']):
                    if name == player_name:
                        update_user_balance(uid, game['bet'], "game_win")
                        break
            
            results_text += f"💰 Ставки возвращены на баланс"
        
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=results_text
            )
        except Exception as e:
            logger.error(f"Ошибка редактирования сообщения: {e}")
            await bot.send_message(chat_id, results_text)
        
        data = load_db()
        if game_id in data['games']:
            del data['games'][game_id]
            save_db(data)
        
        logger.info(f"Игра #{game_id} завершена")
        
    except Exception as e:
        logger.error(f"Ошибка завершения игры: {e}")

async def finish_21game(game, player_scores):
    try:
        game_id = game['game_id']
        chat_id = game['chat_id']
        message_id = game['message_id']
        
        results_text = f"🎲 21CUB №{game_id}\n\n"
        results_text += "📊 Результаты:\n\n"
        
        for player_name in game.get('player_names', []):
            dice_values = game['player_dice'].get(player_name, [])
            score = player_scores.get(player_name, 0)
            
            dice_str = " + ".join(str(d) for d in dice_values)
            results_text += f"@{player_name}: {dice_str} = {score}\n"
        
        results_text += "\n"
        
        valid_scores = {name: score for name, score in player_scores.items() if score <= 21}
        
        if not valid_scores:
            results_text += "❌ Все игроки проиграли (превысили 21)\n💰 Ставки возвращены на баланс"
            for player_name in game['player_names']:
                for uid, name in zip(game['players'], game['player_names']):
                    if name == player_name:
                        update_user_balance(uid, game['bet'], "game_win")
                        break
        else:
            max_score = max(valid_scores.values())
            winners = [name for name, score in valid_scores.items() if score == max_score]
            
            pot = game['bet'] * len(game['players'])
            
            if len(winners) == 1:
                winner = winners[0]
                winner_id = None
                for uid, name in zip(game['players'], game['player_names']):
                    if name == winner:
                        winner_id = uid
                        break
                
                if winner_id:
                    update_user_balance(winner_id, pot, "game_win")
                    
                results_text += f"🏆 Победитель: @{winner}\n"
                results_text += f"💰 Выигрыш: {pot} RUB"
            else:
                results_text += f"🤝 Ничья: " + ", ".join([f"@{w}" for w in winners]) + "\n"

                for player_name in game['player_names']:
                    for uid, name in zip(game['players'], game['player_names']):
                        if name == player_name:
                            update_user_balance(uid, game['bet'], "game_win")
                            break
                
                results_text += f"💰 Ставки возвращены на баланс"

        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=results_text
            )
        except Exception as e:
            logger.error(f"Ошибка редактирования сообщения: {e}")
            await bot.send_message(chat_id, results_text)

        data = load_db()
        if game_id in data['games']:
            del data['games'][game_id]
            save_db(data)
        
        logger.info(f"Игра 21 #{game_id} завершена")
        
    except Exception as e:
        logger.error(f"Ошибка завершения игры 21: {e}")

async def finish_wins_game(game, winners):
    try:
        game_id = game['game_id']
        chat_id = game['chat_id']
        message_id = game['message_id']
        
        dice_emoji = game.get('emoji', '🎲')
        command_name = game.get('command', 'game').upper()
        target_wins = game.get('target_wins', 1)

        results_text = f"{dice_emoji} {command_name} {target_wins}WIN №{game_id}\n\n"
        results_text += f"⚡️ Игра до {target_wins} побед завершена!\n\n"

        for i, round_result in enumerate(game.get('round_results', []), 1):
            round_scores = ", ".join([f"@{name}: {score}" for name, score in round_result['scores'].items()])
            round_winners = ", ".join([f"@{name}" for name in round_result['winners']])
            results_text += f"Раунд {i}: {round_scores} | Победитель: {round_winners}\n"
        
        results_text += "\n"

        pot = game['bet'] * len(game['players'])
        
        if len(winners) == 1:
            winner = winners[0]
            winner_id = None
            for uid, name in zip(game['players'], game['player_names']):
                if name == winner:
                    winner_id = uid
                    break
            
            if winner_id:
                update_user_balance(winner_id, pot, "game_win")
            
            results_text += f"🏆 Победитель: @{winner} ({game['player_round_wins'].get(winner, 0)} побед)\n"
            results_text += f"💰 Выигрыш: {pot} RUB"
        else:
            results_text += f"🤝 Ничья: " + ", ".join([f"@{w}" for w in winners]) + "\n"
            
            for player_name in game['player_names']:
                for uid, name in zip(game['players'], game['player_names']):
                    if name == player_name:
                        update_user_balance(uid, game['bet'], "game_win")
                        break
            
            results_text += f"💰 Ставки возвращены на баланс"
        
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=results_text
            )
        except Exception as e:
            logger.error(f"Ошибка редактирования сообщения: {e}")
            await bot.send_message(chat_id, results_text)

        data = load_db()
        if game_id in data['games']:
            del data['games'][game_id]
            save_db(data)
        
        logger.info(f"Игра #{game_id} завершена (режим wins)")
        
    except Exception as e:
        logger.error(f"Ошибка завершения игры wins: {e}")

@dp.message()
async def handle_other_messages(message: Message):
    if message.text.startswith('/'):
        await message.reply(
            "🎮 Используйте:\n"
            "/start - меню или список игр\n"
            "/bal - баланс\n"
            "/gift <сумма> - подарить баланс\n"
            "/del - удалить последнюю игру\n"
            "/promo КОД - активировать промокод\n"
            "/ref - реферальная статистика\n"
            "/21cub <ставка> - игра 21"
        )

async def check_pending_deposits():
    while True:
        try:
            data = load_db()
            
            for deposit in data['deposits']:
                if deposit['status'] == 'pending':
                    invoice_id = deposit['invoice_id']
                    status = await check_invoice_status(invoice_id)
                    
                    if status == 'paid':
                        user_id, amount, success = complete_deposit(invoice_id)
                        
                        if success and user_id:
                            user_data = get_or_create_user(user_id)
                            
                            try:
                                await bot.send_message(
                                    user_id,
                                    f"✅ Оплата депозита подтверждена!\n\n"
                                    f"💰 На ваш баланс зачислено {amount:.2f} RUB\n"
                                    f"💳 Текущий баланс: {user_data['balance']:.2f} RUB"
                                )
                            except Exception as e:
                                logger.error(f"Ошибка уведомления о депозите: {e}")
            
            await asyncio.sleep(60)
            
        except Exception as e:
            logger.error(f"Ошибка в check_pending_deposits: {e}")
            await asyncio.sleep(10)

async def main():
    logger.info("Бот запущен")
    logger.info(f"Админ: {ADMIN_ID}")
    logger.info(f"Минимальный вывод: {MIN_WITHDRAWAL} RUB")
    logger.info(f"Проверка админа от: {THRESHOLD} RUB")
    logger.info(f"Курс: 1 USDT = {USDT_RATE} RUB")
    
    asyncio.create_task(check_pending_deposits())
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
