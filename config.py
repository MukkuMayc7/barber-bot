# config.py
import os
from dotenv import load_dotenv

load_dotenv()

# Базовые настройки из .env
BOT_TOKEN = os.getenv('BOT_TOKEN', "8297051179:AAGHxFTyY2ourq2qmORND-oBN5TaKVYM0uE")
BASE_ADMIN_IDS = [int(x) for x in os.getenv('ADMIN_IDS', '156901976').split(',')]
WEBHOOK_URL = os.getenv('WEBHOOK_URL', 'https://barber-bot-render.onrender.com')

# Динамический список администраторов (будет храниться в базе данных)
# При старте используем BASE_ADMIN_IDS, затем загружаем из базы
ADMIN_IDS = BASE_ADMIN_IDS.copy()

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
PORT = int(os.environ.get('PORT', 10000))

def update_admin_ids(new_admin_ids):
    """Обновляет список администраторов в runtime"""
    global ADMIN_IDS
    ADMIN_IDS = new_admin_ids

# Логирование настроек при импорте
print("=" * 50)
print("🔄 LOADING CONFIGURATION...")
print(f"🔧 WEBHOOK_URL: {WEBHOOK_URL}")
print(f"🔧 BOT_TOKEN: {BOT_TOKEN[:10]}...")
print(f"🔧 BASE_ADMIN_IDS: {BASE_ADMIN_IDS}")
print(f"🔧 PORT: {PORT}")
print("✅ CONFIGURATION LOADED")
print("=" * 50)