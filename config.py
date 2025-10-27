# config.py
import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN', '8297051179:AAGHxFTyY2ourq2qmORND-oBN5TaKVYM0uE')

# Получаем ADMIN_IDS из переменной окружения
admin_ids_str = os.getenv('ADMIN_IDS', '156901976')
ADMIN_IDS = [int(id.strip()) for id in admin_ids_str.split(',')]

WORKING_HOURS = list(range(10, 20))
REMINDER_HOURS_BEFORE = 24
NOTIFICATION_CHAT_ID = None

# ИСПРАВЛЕННЫЕ ДНИ НЕДЕЛИ - теперь корректно определяются
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

# Настройки времени
TIMEZONE_OFFSET = 3  # Moscow time UTC+3