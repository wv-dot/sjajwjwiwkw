from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from typing import List, Dict
import config


def user_main_menu() -> InlineKeyboardMarkup:
    """Главное меню пользователя"""
    keyboard = [
        [InlineKeyboardButton(text="📱 Добавить номер", callback_data="add_number")],
        [InlineKeyboardButton(text="📋 Мои номера", callback_data="my_numbers")],
        [InlineKeyboardButton(text="💵 Вывод", callback_data="withdraw")],
        [InlineKeyboardButton(text="❓ Помощь", url=config.HELP_LINK)]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def cancel_keyboard() -> InlineKeyboardMarkup:
    """Кнопка отмены"""
    keyboard = [[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def admin_panel(is_active: bool, is_owner: bool = False) -> InlineKeyboardMarkup:
    """Админ-панель"""
    status_text = "✅ Работа идет" if is_active else "❌ Работа не идет"
    status_callback = "stop_work" if is_active else "start_work"

    keyboard = [
        [InlineKeyboardButton(text=status_text, callback_data=status_callback)],
        [InlineKeyboardButton(text="📊 Получить отчет", callback_data="admin_report")],
        [InlineKeyboardButton(text="📢 Сделать рассылку", callback_data="broadcast")],
        [InlineKeyboardButton(text="🔨 Забанить юзера", callback_data="ban_user")],
        [InlineKeyboardButton(text="🔓 Разбанить юзера", callback_data="unban_user")],
        [InlineKeyboardButton(text="🗑️ Очистить очередь", callback_data="clear_queue")]
    ]
    
    if is_owner:
        keyboard.append([InlineKeyboardButton(text="💰 Установить цену", callback_data="set_price")])
        keyboard.append([InlineKeyboardButton(text="👥 Управление админами", callback_data="manage_admins")])
        keyboard.append([InlineKeyboardButton(text="📢 Обязательная подписка", callback_data="manage_subscription")])

    keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_main")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def date_selection_keyboard(dates: List[str], prefix: str) -> InlineKeyboardMarkup:
    """Выбор даты для отчётов"""
    keyboard = []
    for i in range(0, len(dates), 2):
        row = []
        if i < len(dates):
            row.append(InlineKeyboardButton(text=dates[i], callback_data=f"{prefix}_{dates[i]}"))
        if i + 1 < len(dates):
            row.append(InlineKeyboardButton(text=dates[i + 1], callback_data=f"{prefix}_{dates[i + 1]}"))
        keyboard.append(row)

    keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_admin")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def action_keyboard(number_id: int) -> InlineKeyboardMarkup:
    """Клавиатура действий админа после получения кода"""
    keyboard = [
        [
            InlineKeyboardButton(text="✅ Встал", callback_data=f"success_{number_id}"),
            InlineKeyboardButton(text="❌ Нев. код", callback_data=f"invalid_code_{number_id}")
        ],
        [
            InlineKeyboardButton(text="🚫 Фрод", callback_data=f"fraud_{number_id}"),
            InlineKeyboardButton(text="📞 Занят", callback_data=f"busy_{number_id}")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def confirm_broadcast_keyboard() -> InlineKeyboardMarkup:
    """Подтверждение рассылки"""
    keyboard = [
        [InlineKeyboardButton(text="✅ Отправить", callback_data="confirm_broadcast")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def user_numbers_keyboard(numbers: List[Dict]) -> InlineKeyboardMarkup:
    """Клавиатура с номерами пользователя в виде кнопок"""
    keyboard = []
    for num in numbers:
        phone = num['phone_number']
        position = num['position_in_queue']
        number_id = num['id']
        # Форматируем номер для отображения
        display_phone = f"+7 ({phone[1:4]}) {phone[4:7]}-{phone[7:9]}-{phone[9:]}"
        keyboard.append([InlineKeyboardButton(
            text=f"#{position} ⏳ {display_phone}",
            callback_data=f"show_number_{number_id}"
        )])
    keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_main")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def number_actions_keyboard(number_id: int) -> InlineKeyboardMarkup:
    """Клавиатура действий с номером"""
    keyboard = [
        [InlineKeyboardButton(text="🗑️ Удалить номер из очереди", callback_data=f"delete_number_{number_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="my_numbers")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def manage_admins_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура управления админами"""
    keyboard = [
        [InlineKeyboardButton(text="➕ Добавить админа", callback_data="add_admin")],
        [InlineKeyboardButton(text="➖ Снять с админки", callback_data="remove_admin")],
        [InlineKeyboardButton(text="📋 Список админов", callback_data="list_admins")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_admin")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def initial_request_keyboard(number_id: int) -> InlineKeyboardMarkup:
    """Клавиатура при взятии номера — только запрос кода"""
    keyboard = [
        [InlineKeyboardButton(text="📱 Запросить код", callback_data=f"request_code_{number_id}")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def waiting_code_keyboard(number_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏱ Время вышло", callback_data=f"timeout_{number_id}")]
    ])


def request_code_user_keyboard(number_id: int) -> InlineKeyboardMarkup:
    """Клавиатура для пользователя при запросе кода: отмена"""
    keyboard = [
        [InlineKeyboardButton(text="❌ Отменить номер", callback_data="cancel_action")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def invalid_code_user_keyboard(number_id: int) -> InlineKeyboardMarkup:
    """Клавиатура для пользователя при неверном коде (ожидание нового кода)"""
    keyboard = [
        [InlineKeyboardButton(text="❌ Отменить номер", callback_data=f"delete_number_{number_id}")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def subscription_required_keyboard(channel_username: str | None) -> InlineKeyboardMarkup:
    keyboard = []
    if channel_username:
        keyboard.append([InlineKeyboardButton(text="📢 Подписаться на канал", url=f"https://t.me/{channel_username.lstrip('@')}")])
    keyboard.append([InlineKeyboardButton(text="✅ Проверить подписку", callback_data="check_subscription")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def manage_subscription_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(text="➕ Установить/изменить канал", callback_data="set_subscription_channel")],
        [InlineKeyboardButton(text="➖ Отключить подписку", callback_data="remove_subscription_channel")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_admin")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)