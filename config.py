# config.py
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN', "8297051179:AAGHxFTyY2ourq2qmORND-oBN5TaKVYM0uE")
ADMIN_IDS = [int(x) for x in os.getenv('ADMIN_IDS', '156901976').split(',')]

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

# Настройки для Render
PORT = int(os.getenv('PORT', 10000))
WEBHOOK_URL = os.getenv('WEBHOOK_URL', '')

# Логирование настроек
if WEBHOOK_URL:
    print(f"WEBHOOK_URL configured: {WEBHOOK_URL}")
else:
    print("WEBHOOK_URL not set - running in polling mode")
print(f"BOT_TOKEN: {BOT_TOKEN[:10]}...")
print(f"ADMIN_IDS: {ADMIN_IDS}")