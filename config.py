# config.py
import os
from dotenv import load_dotenv

load_dotenv()

# БЕЗОПАСНОЕ получение токена
BOT_TOKEN = os.getenv('BOT_TOKEN')
if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN не найден в переменных окружения!")

# Безопасное получение ADMIN_IDS
admin_ids_str = os.getenv('ADMIN_IDS', '')
if not admin_ids_str:
    raise ValueError("❌ ADMIN_IDS не найдены в переменных окружения!")

ADMIN_IDS = [int(id.strip()) for id in admin_ids_str.split(',') if id.strip()]

WORKING_HOURS = list(range(10, 20))
REMINDER_HOURS_BEFORE = 24
NOTIFICATION_CHAT_ID = None

WEEKDAYS = {
    0: "Понедельник",
    1: "Вторник", 
    2: "Среда",
    3: "Четверг",
    4: "Пятница",
    5: "Суббота",
    6: "Воскресенье"
}

TIME_SLOTS = [f"{hour:02d}:{minute:02d}" for hour in range(10, 20) for minute in [0, 30]]

BARBERSHOP_NAME = "Бархат"
CLEANUP_DAYS_OLD = 30

# Настройки базы данных
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///barbershop.db')
TIMEZONE_OFFSET = 3