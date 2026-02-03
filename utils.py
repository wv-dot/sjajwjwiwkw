import re
import logging
import os
import tempfile
import asyncio
from typing import Optional
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment

import config
from database import Database

logger = logging.getLogger(__name__)
db = Database(config.DB_PATH)

def validate_and_normalize_phone(phone: str) -> Optional[str]:
    cleaned = re.sub(r'[^\d]', '', phone)
    if cleaned.startswith('8') and len(cleaned) == 11:
        cleaned = '7' + cleaned[1:]
    if cleaned.startswith('9') and len(cleaned) == 10:
        cleaned = '7' + cleaned
    if not cleaned.startswith('79') or len(cleaned) != 11:
        return None
    return cleaned

def format_phone_display(phone: str) -> str:
    return phone  # просто возвращаем как есть

async def is_admin(user_id: int) -> bool:
    """Проверяет, является ли пользователь админом (проверка в БД)"""
    return await db.is_admin_in_db(user_id)

async def is_owner(user_id: int) -> bool:
    """Проверяет, является ли пользователь овнером (в config.OWNER_IDS или в БД)"""
    if user_id in config.OWNER_IDS:
        return True
    return await db.is_owner_in_db(user_id)

async def initialize_owners():
    """Инициализирует овнеров из config.OWNER_IDS в БД"""
    for owner_id in config.OWNER_IDS:
        try:
            await db.add_admin(owner_id, added_by=0, is_owner=True)
            logger.info(f"Овнер {owner_id} добавлен в БД")
        except Exception as e:
            logger.error(f"Ошибка при добавлении овнера {owner_id}: {e}")

async def generate_excel_report(data: list, date_str: str) -> Optional[str]:
    try:
        wb = Workbook()
        ws = wb.active
        ws.title = f"Отчет {date_str}"
        headers = ["ID", "Username", "Номер", "Статус", "Добавлен", "Обработан", "Код", "Причина"]
        ws.append(headers)
        for cell in ws[1]:
            cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal="center")

        for row in data:
            ws.append([
                row['user_id'],
                row['username'] or "-",
                format_phone_display(row['phone_number']),
                row['status'],
                row['added_at'],
                row['completed_at'] or "-",
                row['code'] or "-",
                row['result_reason'] or "-"
            ])

        path = os.path.join(config.REPORTS_DIR, f"report_{date_str}.xlsx")
        wb.save(path)
        return path
    except Exception as e:
        logger.error(f"Ошибка Excel: {e}")
        return None

async def generate_txt_report(data: list, date_str: str) -> Optional[str]:
    try:
        lines = [f"Отчет за {date_str}", ""]
        for row in data:
            lines.append(f"{format_phone_display(row['phone_number'])} - {row['status']}")
            if row['completed_at']:
                lines.append(f"   Обработан: {row['completed_at']}")
            if row['code']:
                lines.append(f"   Код: {row['code']}")
            if row['result_reason']:
                lines.append(f"   Причина: {row['result_reason']}")
            lines.append("")

        path = tempfile.NamedTemporaryFile(dir=config.TEMP_DIR, suffix=".txt", delete=False).name
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        return path
    except Exception as e:
        logger.error(f"Ошибка TXT: {e}")
        return None

async def send_to_all(bot, text: str):
    users = await db.get_all_users()
    for user_id in users:
        try:
            await bot.send_message(user_id, text)
            await asyncio.sleep(0.033)
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение пользователю {user_id}: {e}")