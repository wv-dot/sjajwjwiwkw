import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

import config
from database import Database
from handlers import router
from utils import initialize_owners
from middlewares import SubscriptionMiddleware, UpdateUserInfoMiddleware

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    db = Database(config.DB_PATH)

    await db.initialize()
    
    # Инициализируем овнеров из config.OWNER_IDS
    await initialize_owners()
    logger.info("Овнеры инициализированы")

    bot = Bot(token=config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    # Добавляем middleware для обновления информации о пользователе (должен быть первым)
    dp.message.outer_middleware(UpdateUserInfoMiddleware())
    dp.callback_query.outer_middleware(UpdateUserInfoMiddleware())
    # Добавляем middleware для проверки подписки
    dp.message.outer_middleware(SubscriptionMiddleware())
    dp.callback_query.outer_middleware(SubscriptionMiddleware())

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())