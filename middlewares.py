import logging
from typing import Callable, Dict, Any, Awaitable

from aiogram import BaseMiddleware
from aiogram.types import Message, CallbackQuery

import config
from database import Database
from keyboards import subscription_required_keyboard

logger = logging.getLogger(__name__)

db = Database(config.DB_PATH)


class UpdateUserInfoMiddleware(BaseMiddleware):
    """Middleware для обновления информации о пользователе (username, first_name, last_name) при каждом сообщении"""
    async def __call__(
        self,
        handler: Callable[[Message | CallbackQuery, Dict[str, Any]], Awaitable[Any]],
        event: Message | CallbackQuery,
        data: Dict[str, Any],
    ) -> Any:
        # Обновляем информацию о пользователе при каждом сообщении или callback
        if event.from_user:
            try:
                db.update_user_info(
                    user_id=event.from_user.id,
                    username=event.from_user.username,
                    first_name=event.from_user.first_name,
                    last_name=event.from_user.last_name
                )
            except Exception as e:
                logger.error(f"Ошибка обновления информации о пользователе {event.from_user.id}: {e}")
        
        return await handler(event, data)


class SubscriptionMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Message | CallbackQuery, Dict[str, Any]], Awaitable[Any]],
        event: Message | CallbackQuery,
        data: Dict[str, Any],
    ) -> Any:
        user_id = event.from_user.id

        if db.is_admin_in_db(user_id) or user_id in config.OWNER_IDS:
            return await handler(event, data)

        channel_id = db.get_subscription_channel()
        if not channel_id:
            return await handler(event, data)

        bot = data["bot"]

        channel_username = None
        try:
            chat = await bot.get_chat(channel_id)
            channel_username = chat.username
        except Exception as e:
            logger.error(f"Ошибка получения канала {channel_id}: {e}")

        try:
            member = await bot.get_chat_member(channel_id, user_id)
            if member.status in ("member", "administrator", "creator"):
                return await handler(event, data)
        except Exception:
            pass

        keyboard = subscription_required_keyboard(channel_username)

        if isinstance(event, Message):
            await event.answer(
                config.MESSAGES.SUBSCRIPTION_REQUIRED,
                reply_markup=keyboard,
                disable_notification=True
            )
        elif isinstance(event, CallbackQuery):
            try:
                await event.message.edit_text(
                    config.MESSAGES.SUBSCRIPTION_REQUIRED,
                    reply_markup=keyboard
                )
            except Exception:
                await event.message.answer(
                    config.MESSAGES.SUBSCRIPTION_REQUIRED,
                    reply_markup=keyboard
                )
            await event.answer()

        return None