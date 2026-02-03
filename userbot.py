import asyncio
import logging
import re
import time
import random
import string
from typing import Dict, Optional, List
from telethon import TelegramClient, events
from telethon.tl.types import Message
from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

import config
from database import Database

logger = logging.getLogger(__name__)

START_PAYLOAD = None
START_PAYLOAD_TS = 0
START_PAYLOAD_TTL = 30

# Глобальные переменные
userbot_client: Optional[TelegramClient] = None
bot1_client: Optional[Bot] = None
db: Database = None
userbot_task: Optional[asyncio.Task] = None
is_running = False

# Mapping для отслеживания номеров
# phone -> {number_id, user_id, bot2_message_id, bot1_request_message_id, timeout_task}
active_numbers: Dict[str, Dict] = {}
# number_id -> phone (для обратного поиска)
number_id_to_phone: Dict[int, str] = {}


async def init_userbot():
    """Инициализация юзербота"""
    global userbot_client, bot1_client, db
    
    db = Database(config.DB_PATH)
    
    # Инициализируем aiogram Bot для отправки сообщений пользователям
    bot1_client = Bot(
        token=config.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN),
    )
    
    # Инициализируем Telethon клиент
    if not config.API_ID or not config.API_HASH:
        raise ValueError("API_ID и API_HASH должны быть установлены в .env файле")
    
    userbot_client = TelegramClient(
        config.USERBOT_SESSION_NAME,
        api_id=int(config.API_ID),
        api_hash=config.API_HASH
    )
    
    if not config.BOT2_USERNAME:
        raise ValueError("BOT2_USERNAME не установлен в config.py")
    
    await userbot_client.start()
    
    # Регистрируем обработчик сообщений от бот2
    # Получаем entity бот2 для фильтрации
    try:
        bot2_entity = await userbot_client.get_entity(config.BOT2_USERNAME)
        userbot_client.add_event_handler(
            handle_bot2_messages,
            events.NewMessage(from_users=bot2_entity)
        )
    except Exception as e:
        logger.error(f"Ошибка регистрации обработчика сообщений от бот2: {e}")
        raise
    
    logger.info("Юзербот инициализирован")

def get_start_payload() -> str:
    global START_PAYLOAD, START_PAYLOAD_TS

    now = time.time()
    if START_PAYLOAD is None or now - START_PAYLOAD_TS >= START_PAYLOAD_TTL:
        START_PAYLOAD = ''.join(
            random.choices(string.ascii_letters + string.digits, k=8)
        )
        START_PAYLOAD_TS = now

    return START_PAYLOAD

async def cleanup_userbot():
    """Очистка ресурсов юзербота"""
    global userbot_client, bot1_client
    
    if bot1_client:
        await bot1_client.session.close()
        bot1_client = None
    
    if userbot_client:
        await userbot_client.disconnect()
        userbot_client = None
    
    logger.info("Юзербот остановлен")


async def send_numbers_to_bot2(numbers: List[Dict]) -> Optional[int]:
    """Отправляет номера в бот2"""
    if not userbot_client or not config.BOT2_USERNAME:
        return None
    
    try:
        # Получаем entity бот2
        try:
            bot2_entity = await userbot_client.get_entity(config.BOT2_USERNAME)
        except Exception as e:
            logger.error(f"Не удалось получить entity бот2: {e}")
            return None
        
        # Отправляем /start в бот2
        payload = get_start_payload()
        await userbot_client.send_message(bot2_entity, f"/start {payload}")
        logger.info(f"Отправлена команда /start с payload={payload}")

        await asyncio.sleep(3.5)  # Увеличена задержка для гарантированного получения ответа
        
        # Получаем последние сообщения от бот2, чтобы найти кнопку "📱Добавить номер"
        button_found = False
        
        # Сначала проверяем самое последнее сообщение (оно наиболее вероятно содержит кнопки после /start)
        try:
            last_message = await userbot_client.get_messages(bot2_entity, limit=1)
            if last_message and last_message[0].reply_markup:
                message = last_message[0]
                logger.info(f"Проверяю последнее сообщение от бот2: {message.id}")
                for row_index, row in enumerate(message.reply_markup.rows):
                    for col_index, button in enumerate(row.buttons):
                        button_text = button.text
                        logger.debug(f"Найдена кнопка: {button_text}")
                        if "Добавить номер" in button_text or ("📱" in button_text and "Добавить" in button_text):
                            try:
                                logger.info(f"Найдена кнопка 'Добавить номер' (row={row_index}, col={col_index}), нажимаю...")
                                # Используем индексы row и column для нажатия кнопки
                                await message.click(row_index, col_index)
                                button_found = True
                                logger.info("Кнопка 'Добавить номер' успешно нажата")
                                await asyncio.sleep(1)
                                break
                            except Exception as e:
                                logger.error(f"Ошибка нажатия кнопки: {e}")
                    if button_found:
                        break
        except Exception as e:
            logger.warning(f"Не удалось получить последнее сообщение: {e}")
        
        # Если кнопка не найдена в последнем сообщении, ищем в последних 10 сообщениях
        if not button_found:
            logger.info("Кнопка не найдена в последнем сообщении, ищу в последних 10 сообщениях...")
            async for message in userbot_client.iter_messages(bot2_entity, limit=10):
                if message.reply_markup:
                    # Ищем кнопку "📱Добавить номер" по тексту
                    for row_index, row in enumerate(message.reply_markup.rows):
                        for col_index, button in enumerate(row.buttons):
                            button_text = button.text
                            if "Добавить номер" in button_text or ("📱" in button_text and "Добавить" in button_text):
                                try:
                                    logger.info(f"Найдена кнопка 'Добавить номер' в сообщении {message.id} (row={row_index}, col={col_index}), нажимаю...")
                                    # Используем индексы row и column для нажатия кнопки
                                    await message.click(row_index, col_index)
                                    button_found = True
                                    logger.info("Кнопка 'Добавить номер' успешно нажата")
                                    await asyncio.sleep(1)
                                    break
                                except Exception as e:
                                    logger.error(f"Ошибка нажатия кнопки: {e}")
                        if button_found:
                            break
                    if button_found:
                        break
        
        if not button_found:
            logger.warning("Кнопка 'Добавить номер' не найдена после поиска в последних сообщениях, отправляю номера напрямую")
        
        # Формируем список номеров через \n
        phones_text = "\n".join([num['phone_number'] for num in numbers])
        
        # Отправляем номера
        sent_message = await userbot_client.send_message(bot2_entity, phones_text)
        
        # Сохраняем mapping номеров
        for num in numbers:
            phone = num['phone_number']
            number_id = num['id']
            active_numbers[phone] = {
                'number_id': number_id,
                'user_id': num['user_id'],
                'bot2_message_id': sent_message.id,
                'bot1_request_message_id': None,
                'timeout_task': None,
                'old_bot1_message_ids': []
            }
            number_id_to_phone[number_id] = phone
        
        logger.info(f"Отправлено {len(numbers)} номеров в бот2")
        return sent_message.id
        
    except Exception as e:
        logger.error(f"Ошибка отправки номеров в бот2: {e}")
        return None


async def notify_users_numbers_taken(numbers: List[Dict]):
    """Уведомляет пользователей, что их номера взяты автосистемой"""
    if not bot1_client:
        logger.warning("bot1_client не инициализирован, невозможно отправить уведомления")
        return
    
    logger.info(f"Начинаю отправку уведомлений пользователям о взятии номеров автосистемой. Количество номеров: {len(numbers)}")
    
    for num in numbers:
        try:
            # Проверяем, что num является словарем
            if not isinstance(num, dict):
                logger.error(f"Ожидался словарь, получен {type(num)}: {num}")
                continue
            
            # Извлекаем данные с проверкой наличия ключей
            if 'phone_number' not in num:
                logger.error(f"В словаре отсутствует ключ 'phone_number': {num}")
                continue
            
            if 'user_id' not in num:
                logger.error(f"В словаре отсутствует ключ 'user_id': {num}")
                continue
            
            phone = num['phone_number']
            user_id = num['user_id']
            
            # Проверяем типы данных
            if not isinstance(phone, str):
                logger.error(f"phone_number должен быть строкой, получен {type(phone)}: {phone}")
                continue
            
            if not isinstance(user_id, int):
                logger.error(f"user_id должен быть целым числом, получен {type(user_id)}: {user_id}")
                continue
            
            try:
                display_phone = format_phone_display(phone)
            except Exception as e:
                logger.error(f"Ошибка форматирования номера {phone}: {e}")
                display_phone = phone  # Используем оригинальный номер при ошибке форматирования
            
            await bot1_client.send_message(
                user_id,
                config.NUMBER_TAKEN_BY_AUTO_SYSTEM.format(phone=display_phone),
                parse_mode="Markdown"
            )
            logger.info(f"Уведомление отправлено пользователю {user_id} для номера {display_phone}")
        except KeyError as e:
            logger.error(f"Не удалось получить ключ из словаря num: {e}. Словарь: {num}")
        except Exception as e:
            logger.error(f"Не удалось уведомить пользователя: {e}. Тип num: {type(num)}, содержимое: {num if isinstance(num, dict) else 'не словарь'}")


def format_phone_display(phone: str) -> str:
    """Форматирует номер для отображения"""
    from utils import format_phone_display as utils_format
    return utils_format(phone)


async def numbers_fetch_loop():
    """Основной цикл взятия номеров (каждые 30 секунд)"""
    global is_running
    
    while is_running:
        try:
            # Проверяем, включен ли авторежим
            if not db.is_auto_mode_enabled():
                logger.info("Авторежим выключен, останавливаем цикл")
                break
            
            # Проверяем, активна ли работа
            if not db.is_work_active():
                logger.info("Работа не активна, пропускаем цикл")
                await asyncio.sleep(config.NUMBERS_FETCH_INTERVAL)
                continue
            
            # Получаем 10 номеров из очереди
            numbers = db.get_next_numbers_in_queue(config.NUMBERS_BATCH_SIZE)
            
            if numbers:
                logger.info(f"Получено {len(numbers)} номеров из очереди. Начинаю обработку...")
                # Берем номера
                number_ids = [num['id'] for num in numbers]
                db.take_numbers_batch(number_ids, taken_by=0)  # 0 = юзербот
                logger.info(f"Номера взяты в базу данных. ID номеров: {number_ids}")
                
                # Уведомляем пользователей
                await notify_users_numbers_taken(numbers)
                
                # Отправляем в бот2
                await send_numbers_to_bot2(numbers)
            
            await asyncio.sleep(config.NUMBERS_FETCH_INTERVAL)
            
        except Exception as e:
            logger.error(f"Ошибка в цикле взятия номеров: {e}")
            await asyncio.sleep(config.NUMBERS_FETCH_INTERVAL)


async def handle_code_timeout(phone: str, number_id: int, user_id: int):
    """Обработка таймаута запроса кода"""
    try:
        # Помечаем номер как отмененный по таймауту
        db.update_number_status(number_id, config.MESSAGES.STATUS_CANCELLED)
        
        # Уведомляем пользователя
        if bot1_client:
            number_data = db.get_phone_by_id(number_id)
            if number_data:
                display_phone = format_phone_display(number_data['phone_number'])
                await bot1_client.send_message(
                    user_id,
                    config.MESSAGES.TIMEOUT_NOTIFICATION.format(phone=display_phone),
                    parse_mode="Markdown"
                )
        
        # Пробуем отменить номер и нажать кнопку отмены в бот2 через общий хелпер
        try:
            await cancel_number_in_bot2(phone=phone, number_id=number_id)
        except Exception as e:
            logger.error(f"Ошибка отмены номера {phone} в бот2 при таймауте: {e}")
            
    except Exception as e:
        logger.error(f"Ошибка обработки таймаута для {phone}: {e}")


async def _try_click_cancel_button(message) -> bool:
    """Ищет и нажимает кнопку отмены в сообщении бот2"""
    if not message or not message.reply_markup:
        return False
    for row_index, row in enumerate(message.reply_markup.rows):
        for col_index, button in enumerate(row.buttons):
            text = getattr(button, "text", "") or ""
            if "Отменить" in text or "❌" in text:
                try:
                    # Для callback-кнопок используем data, иначе позицию row/col
                    if getattr(button, "data", None):
                        await message.click(data=button.data)
                    else:
                        await message.click(row_index, col_index)
                    logger.info(f"Кнопка отмены нажата в бот2 (row={row_index}, col={col_index})")
                    return True
                except Exception as e:
                    logger.error(f"Ошибка нажатия кнопки отмены: {e}")
    return False


async def cancel_number_in_bot2(phone: Optional[str] = None, number_id: Optional[int] = None) -> bool:
    """
    Отменяет номер в бот2, имитируя нажатие кнопки "Отменить номер".
    Используется, когда пользователь в бот1 нажимает отмену, а номер обслуживает юзербот.
    """
    try:
        if not userbot_client or not config.BOT2_USERNAME:
            logger.warning("userbot_client или BOT2_USERNAME не инициализированы для отмены номера")
            return False

        # Восстанавливаем телефон по number_id, если не передан явно
        if not phone and number_id is not None:
            phone = number_id_to_phone.get(number_id)
        if not phone:
            logger.warning("Не удалось определить телефон для отмены в бот2")
            return False

        data = active_numbers.get(phone)
        if not data and number_id is not None:
            # Попытка найти по number_id в обратной мапе
            mapped_phone = number_id_to_phone.get(number_id)
            data = active_numbers.get(mapped_phone) if mapped_phone else None
            if mapped_phone:
                phone = mapped_phone

        # Останавливаем таймер, если был
        if data and data.get("timeout_task"):
            data["timeout_task"].cancel()
            data["timeout_task"] = None

        bot2_entity = await userbot_client.get_entity(config.BOT2_USERNAME)

        clicked = False

        # 1) Пытаемся нажать кнопку по сохраненному message_id
        msg_id = data.get("bot2_message_id") if data else None
        if msg_id:
            try:
                bot2_message = await userbot_client.get_messages(bot2_entity, ids=msg_id)
                if bot2_message:
                    clicked = await _try_click_cancel_button(bot2_message)
            except Exception as e:
                logger.error(f"Ошибка получения сообщения бот2 по id {msg_id}: {e}")

        # 2) Фолбэк — ищем последнее сообщение с этим телефоном и кнопкой отмены
        if not clicked:
            try:
                async for msg in userbot_client.iter_messages(bot2_entity, limit=30):
                    if msg.raw_text and phone in msg.raw_text:
                        clicked = await _try_click_cancel_button(msg)
                        if clicked:
                            break
            except Exception as e:
                logger.error(f"Ошибка поиска сообщения для отмены номера {phone}: {e}")

        # Чистим локальные структуры
        if phone in active_numbers:
            active_numbers.pop(phone, None)
        if data and data.get("number_id") in number_id_to_phone:
            number_id_to_phone.pop(data.get("number_id"), None)
        if number_id is not None and number_id in number_id_to_phone:
            number_id_to_phone.pop(number_id, None)

        if clicked:
            logger.info(f"Отмена номера {phone} в бот2 выполнена")
        else:
            logger.warning(f"Не удалось нажать кнопку отмены для номера {phone} в бот2")

        return clicked
    except Exception as e:
        logger.error(f"Ошибка отмены номера {phone} в бот2: {e}")
        return False


async def handle_bot2_messages(event: Message):
    """Обработка сообщений от бот2"""
    if not event.message.raw_text:
        return
    
    text = event.message.raw_text
    logger.debug(f"Получено сообщение от бот2: {text[:200]}")  # Логируем первые 200 символов для отладки
    
    # Проверяем статусы ПЕРЕД запросом кода (фрод может появиться до запроса кода)
    # Фрод
    if config.BOT2_STATUS_FRAUD_PATTERN and re.search(config.BOT2_STATUS_FRAUD_PATTERN, text):
        match = re.search(config.BOT2_STATUS_FRAUD_PATTERN, text)
        if match:
            phone = match.group(1) if match.groups() else None
            if phone:
                await handle_status_update(phone, config.MESSAGES.STATUS_FRAUD)
        return
    
    # Проверяем запрос кода
    code_request_match = re.search(config.BOT2_CODE_REQUEST_PATTERN, text)
    if code_request_match:
        logger.info(f"Паттерн запроса кода найден в сообщении от бот2")
        phone = code_request_match.group(1)
        service = code_request_match.group(2)
        
        logger.info(f"Получен запрос кода для номера {phone} ({service})")
        
        # Проверяем, является ли это повторным запросом после неверного кода
        is_invalid_code_repeat = "Менеджер отметил код как неверный" in text or "отметил код как неверный" in text
        
        # Ищем номер в активных
        if phone not in active_numbers:
            # Пытаемся найти в БД по номеру со статусом "взято"
            number_data = db.get_phone_by_number(phone, status='взято')
            if number_data:
                number_id = number_data['id']
                user_id = number_data['user_id']
                active_numbers[phone] = {
                    'number_id': number_id,
                    'user_id': user_id,
                    'bot2_message_id': event.message.id,
                    'bot1_request_message_id': None,
                    'timeout_task': None,
                    'old_bot1_message_ids': []
                }
                number_id_to_phone[number_id] = phone
            else:
                logger.warning(f"Номер {phone} не найден в активных или в БД")
                return
        else:
            # Это повторный запрос кода - отменяем предыдущие таймауты и удаляем старые сообщения
            logger.info(f"Обнаружен повторный запрос кода для номера {phone}")
            
            # Отменяем предыдущий таймаут
            if active_numbers[phone].get('timeout_task'):
                active_numbers[phone]['timeout_task'].cancel()
                active_numbers[phone]['timeout_task'] = None
            
            # Удаляем старое сообщение с запросом кода, если оно есть
            old_message_id = active_numbers[phone].get('bot1_request_message_id')
            if old_message_id and bot1_client:
                try:
                    await bot1_client.delete_message(active_numbers[phone]['user_id'], old_message_id)
                    logger.info(f"Удалено старое сообщение с запросом кода (message_id={old_message_id})")
                except Exception as e:
                    logger.warning(f"Не удалось удалить старое сообщение {old_message_id}: {e}")
            
            # Удаляем старые записи из temp_data['requests'] для этого номера
            try:
                from handlers import temp_data
                if 'requests' in temp_data:
                    number_id = active_numbers[phone]['number_id']
                    to_remove = []
                    for msg_id, req_data in temp_data['requests'].items():
                        if req_data.get('number_id') == number_id and req_data.get('admin_id') == 0:
                            to_remove.append(msg_id)
                    for msg_id in to_remove:
                        temp_data['requests'].pop(msg_id, None)
                        logger.info(f"Удалена старая запись из temp_data['requests'] (message_id={msg_id})")
            except ImportError:
                logger.warning("Не удалось импортировать temp_data из handlers")
            
            # Обновляем message_id запроса кода
            active_numbers[phone]['bot2_message_id'] = event.message.id
        
        number_id = active_numbers[phone]['number_id']
        user_id = active_numbers[phone]['user_id']
        
        # Если это повторный запрос после неверного кода
        if is_invalid_code_repeat:
            logger.info(f"Обнаружен повторный запрос кода для номера {phone} после неверного кода")
            
            # Обновляем статус в БД на "неверный код"
            db.update_number_status(number_id, config.MESSAGES.STATUS_INVALID_CODE)
            
            # Отменяем текущий таймер, если он активен
            if active_numbers[phone]['timeout_task']:
                active_numbers[phone]['timeout_task'].cancel()
                active_numbers[phone]['timeout_task'] = None
            
            # bot2_message_id уже обновлен выше (строка 308 или 298)
            # Отправляем новый запрос кода пользователю через бот1
        
        # Отправляем запрос кода пользователю через бот1
        if bot1_client:
            try:
                from handlers import request_code_for_userbot
                request_msg_id = await request_code_for_userbot(
                    phone,
                    user_id,
                    number_id,
                    bot1_client,
                    is_invalid_code_repeat=is_invalid_code_repeat,
                )
                
                if request_msg_id:
                    active_numbers[phone]['bot1_request_message_id'] = request_msg_id
                    
                    # Запускаем таймер на 3 минуты
                    async def timeout_handler():
                        await asyncio.sleep(config.CODE_REQUEST_TIMEOUT)
                        # Если дошли сюда, значит таймаут
                        await handle_code_timeout(phone, number_id, user_id)
                    
                    timeout_task = asyncio.create_task(timeout_handler())
                    active_numbers[phone]['timeout_task'] = timeout_task
            except Exception as e:
                logger.error(f"Ошибка отправки запроса кода пользователю {user_id}: {e}")
        
        return
    
    # Проверяем статусы
    # Успешно
    if config.BOT2_STATUS_SUCCESS_PATTERN and re.search(config.BOT2_STATUS_SUCCESS_PATTERN, text):
        match = re.search(config.BOT2_STATUS_SUCCESS_PATTERN, text)
        if match:
            phone = match.group(1)
            await handle_status_update(phone, config.MESSAGES.STATUS_SUCCESS)
        return
    
    # Неуспешно
    if config.BOT2_STATUS_FAILED_PATTERN and re.search(config.BOT2_STATUS_FAILED_PATTERN, text):
        match = re.search(config.BOT2_STATUS_FAILED_PATTERN, text)
        if match:
            phone = match.group(1) if match.groups() else None
            if phone:
                await handle_status_update(phone, config.MESSAGES.STATUS_INVALID_CODE)
        return
    
    # Заблокирован (обрабатывается перед "занят", так как более специфичный паттерн)
    if config.BOT2_STATUS_BLOCKED_PATTERN and re.search(config.BOT2_STATUS_BLOCKED_PATTERN, text):
        match = re.search(config.BOT2_STATUS_BLOCKED_PATTERN, text)
        if match:
            phone = match.group(1) if match.groups() else None
            if phone:
                await handle_status_update(phone, config.MESSAGES.STATUS_BLOCKED)
        return
    
    # Занят
    if config.BOT2_STATUS_BUSY_PATTERN and re.search(config.BOT2_STATUS_BUSY_PATTERN, text):
        match = re.search(config.BOT2_STATUS_BUSY_PATTERN, text)
        if match:
            phone = match.group(1) if match.groups() else None
            if phone:
                await handle_status_update(phone, config.MESSAGES.STATUS_BUSY)
        return


async def handle_status_update(phone: str, status: str):
    """Обработка обновления статуса номера"""
    try:
        if phone not in active_numbers:
            # Пытаемся найти в БД
            number_data = db.get_phone_by_number(phone)
            if not number_data:
                logger.warning(f"Номер {phone} не найден для обновления статуса")
                return
            number_id = number_data['id']
            user_id = number_data['user_id']
        else:
            number_id = active_numbers[phone]['number_id']
            user_id = active_numbers[phone]['user_id']
        
        # Обновляем статус в БД
        db.update_number_status(number_id, status)
        
        # Начисляем баланс при статусе "успешно"
        if status == config.MESSAGES.STATUS_SUCCESS:
            price = db.get_price_per_number()
            if price > 0:
                db.update_user_balance(user_id, price)
                db.add_transaction(user_id, price, "payment")
                # Уведомляем пользователя о начислении
                if bot1_client:
                    try:
                        await bot1_client.send_message(
                            user_id,
                            config.MESSAGES.BALANCE_ADDED.format(amount=f"{price:.2f}"),
                            parse_mode="Markdown"
                        )
                    except Exception as e:
                        logger.error(f"Не удалось отправить уведомление о начислении баланса пользователю {user_id}: {e}")
        
        # Блокируем номер при необходимости
        if status in [config.MESSAGES.STATUS_FRAUD, config.MESSAGES.STATUS_BUSY, config.MESSAGES.STATUS_BLOCKED]:
            db.block_number(phone)
        
        # Уведомляем пользователя
        if bot1_client:
            number_data = db.get_phone_by_id(number_id)
            if number_data:
                display_phone = format_phone_display(number_data['phone_number'])
                status_text = {
                    config.MESSAGES.STATUS_SUCCESS: config.MESSAGES.STATUS_SUCCESS_USER,
                    config.MESSAGES.STATUS_FRAUD: config.MESSAGES.STATUS_FRAUD_USER,
                    config.MESSAGES.STATUS_BUSY: config.MESSAGES.STATUS_BUSY_USER,
                    config.MESSAGES.STATUS_BLOCKED: config.MESSAGES.STATUS_BLOCKED_USER,
                }.get(status, status)
                
                await bot1_client.send_message(
                    user_id,
                    config.MESSAGES.NUMBER_STATUS_TEMPLATE.format(
                        phone=display_phone,
                        status_text=status_text
                    ),
                    parse_mode="Markdown"
                )
        
        # Очищаем данные
        if phone in active_numbers:
            if active_numbers[phone]['timeout_task']:
                active_numbers[phone]['timeout_task'].cancel()
            del active_numbers[phone]
        if number_id in number_id_to_phone:
            del number_id_to_phone[number_id]
            
        logger.info(f"Статус номера {phone} обновлен: {status}")
        
    except Exception as e:
        logger.error(f"Ошибка обновления статуса для {phone}: {e}")


async def send_code_to_bot2(phone: str, code: str):
    """Отправка кода в бот2 (вызывается из handlers.py)"""
    if phone not in active_numbers:
        logger.warning(f"Номер {phone} не найден в активных для отправки кода")
        return False
    
    # Отменяем таймер
    if active_numbers[phone]['timeout_task']:
        active_numbers[phone]['timeout_task'].cancel()
        active_numbers[phone]['timeout_task'] = None
    
    # Отправляем код в бот2 реплаем
    if userbot_client and config.BOT2_USERNAME:
        try:
            bot2_entity = await userbot_client.get_entity(config.BOT2_USERNAME)
            bot2_message_id = active_numbers[phone]['bot2_message_id']
            # Получаем сообщение от бот2
            bot2_message = await userbot_client.get_messages(
                bot2_entity,
                ids=bot2_message_id
            )
            
            if bot2_message:
                # Отправляем код реплаем
                await userbot_client.send_message(
                    bot2_entity,
                    code,
                    reply_to=bot2_message
                )
                logger.info(f"Код отправлен в бот2 для номера {phone}")
                return True
        except Exception as e:
            logger.error(f"Ошибка отправки кода в бот2 для {phone}: {e}")
    
    return False


async def start_userbot():
    """Запуск юзербота"""
    global userbot_task, is_running
    
    if is_running:
        logger.warning("Юзербот уже запущен")
        return
    
    try:
        await init_userbot()
        is_running = True
        
        # Запускаем цикл взятия номеров
        userbot_task = asyncio.create_task(numbers_fetch_loop())
        
        logger.info("Юзербот запущен")
    except Exception as e:
        logger.error(f"Ошибка запуска юзербота: {e}")
        is_running = False
        raise


async def stop_userbot():
    """Остановка юзербота"""
    global userbot_task, is_running
    
    if not is_running:
        logger.warning("Юзербот не запущен")
        return
    
    is_running = False
    
    # Отменяем задачу
    if userbot_task:
        userbot_task.cancel()
        try:
            await userbot_task
        except asyncio.CancelledError:
            pass
        userbot_task = None
    
    # Очищаем таймеры
    for phone, data in list(active_numbers.items()):
        if data.get('timeout_task'):
            data['timeout_task'].cancel()
    
    active_numbers.clear()
    number_id_to_phone.clear()
    
    await cleanup_userbot()
    logger.info("Юзербот остановлен")
