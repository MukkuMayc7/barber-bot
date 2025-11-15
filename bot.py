# bot.py
import logging
import re
import os
import threading
import time
import signal
import sys
import psutil
import fcntl
import atexit
from flask import Flask
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes, ConversationHandler,
    JobQueue
)
from telegram.error import BadRequest, TelegramError, Conflict
from datetime import datetime, timedelta, timezone
import database
import config
import httpx
import asyncio

# 🎯 BACKUP ФУНКЦИИ ДЛЯ RENDER (ЛОКАЛЬНЫЕ ФАЙЛЫ)

def get_database_path():
    """🎯 ФУНКЦИЯ ДЛЯ ПОЛУЧЕНИЯ ПУТИ К БАЗЕ ДАННЫХ"""
    return '/tmp/barbershop.db'

async def backup_database(context: ContextTypes.DEFAULT_TYPE):
    """🎯 ЛОКАЛЬНОЕ РЕЗЕРВНОЕ КОПИРОВАНИЕ БЕЗ УВЕДОМЛЕНИЙ"""
    try:
        logger.info("💾 Запуск автоматического локального backup...")
        
        # Создаем backup через database.py (без уведомлений)
        backup_path = db.create_backup()
        
        if backup_path:
            logger.info(f"✅ Автоматический backup создан: {backup_path}")
        else:
            logger.warning("⚠️ Автоматический backup не создан")
        
        # 🎯 УБИРАЕМ УВЕДОМЛЕНИЯ АДМИНИСТРАТОРАМ
        
    except Exception as e:
        logger.error(f"❌ Ошибка при создании автоматического backup: {e}")

async def check_memory_usage(context: ContextTypes.DEFAULT_TYPE):
    """🎯 МОНИТОРИНГ ИСПОЛЬЗОВАНИЯ ПАМЯТИ БЕЗ УВЕДОМЛЕНИЙ"""
    try:
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        memory_mb = process.memory_info().rss / 1024 / 1024
        
        logger.info(f"📊 Использование памяти: {memory_mb:.1f}MB")
        
        # Предупреждение если близко к лимиту 512MB
        if memory_mb > 400:
            logger.warning("⚠️ Близко к лимиту памяти! Выполняем экстренную очистку...")
            
            # Создаем backup перед очисткой (без уведомлений)
            backup_path = db.create_backup()
            if backup_path:
                logger.info(f"✅ Backup перед очисткой создан: {backup_path}")
            
            # Выполняем экстренную очистку
            deleted_count = db.emergency_cleanup()
            
            logger.info(f"🚨 Экстренная очистка: удалено {deleted_count} записей")
            
            # 🎯 УБИРАЕМ УВЕДОМЛЕНИЯ АДМИНИСТРАТОРАМ
                    
    except Exception as e:
        logger.error(f"❌ Ошибка мониторинга памяти: {e}")

async def show_backup_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🎯 ПОКАЗЫВАЕТ СТАТУС BACKUP ДЛЯ АДМИНИСТРАТОРОВ"""
    user_id = update.effective_user.id
    
    if not db.is_admin(user_id):
        await update.message.reply_text("❌ У вас нет доступа к этой функции")
        return
    
    backup_status = db.get_backup_status()
    backup_files = db.get_backup_files_info()
    
    if not backup_status and not backup_files:
        text = "📊 *Статус Backup*\n\n📭 Нет данных о backup"
    else:
        text = "📊 *Статус Backup*\n\n"
        
        if backup_files:
            text += f"📁 *Backup файлов:* {len(backup_files)}\n\n"
            for i, file_info in enumerate(backup_files[:3], 1):  # Показываем последние 3
                text += f"{i}. 📄 `{os.path.basename(file_info['path'])}`\n"
                text += f"   📏 {file_info['size_kb']} KB | 🕐 {file_info['date']}\n\n"
        
        if backup_status:
            text += "*Последние операции backup:*\n\n"
            for i, status in enumerate(backup_status[:3], 1):
                timestamp, size_kb, success, backup_path, error_message = status
                
                if isinstance(timestamp, str):
                    display_time = timestamp
                else:
                    display_time = timestamp.strftime("%d.%m.%Y %H:%M")
                
                status_icon = "✅" if success else "❌"
                size_info = f" | 📏 {size_kb} KB" if size_kb else ""
                
                text += f"{i}. {status_icon} {display_time}{size_info}\n"
                
                if error_message and not success:
                    text += f"   💬 {error_message[:50]}...\n"
                
                text += "\n"
    
    # Добавляем информацию о системе (ИСПРАВЛЕННАЯ ЧАСТЬ)
    db_path = '/tmp/barbershop.db'  # 🎯 ПРЯМОЙ ПУТЬ БЕЗ ФУНКЦИИ
    if os.path.exists(db_path):
        db_size = os.path.getsize(db_path) / (1024 * 1024)  # MB
        size_info = f"{db_size:.2f} MB"
    else:
        size_info = "не найден"
    
    text += f"💾 *Текущая БД:* {size_info}\n"
    text += f"🔄 *Автовосстановление:* ✅ Включено\n"
    text += f"⏰ *Следующий backup:* через 6 часов\n"
    text += f"📝 *Тип:* Локальные файлы (/tmp/)"
    
    keyboard = [
        [InlineKeyboardButton("🔄 Создать backup сейчас", callback_data="create_backup_now")],
        [InlineKeyboardButton("📊 Статистика БД", callback_data="db_stats")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        query = update.callback_query
        await query.edit_message_text(text, parse_mode='Markdown', reply_markup=reply_markup)
    else:
        await update.message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup)

async def create_backup_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🎯 СОЗДАЕТ BACKUP ПО ТРЕБОВАНИЮ"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not db.is_admin(user_id):
        await query.answer("❌ У вас нет доступа к этой функции", show_alert=True)
        return
    
    # Показываем сообщение о начале backup
    await query.edit_message_text("💾 *Создание локального backup...*\n\nПожалуйста, подождите...", parse_mode='Markdown')
    
    # Создаем backup
    backup_path = db.create_backup()
    
    if backup_path:
        backup_files = db.get_backup_files_info()
        
        text = (
            f"✅ *Локальный backup создан успешно!*\n\n"
            f"📁 Файл: `{os.path.basename(backup_path)}`\n"
            f"📏 Размер: {os.path.getsize(backup_path) / 1024:.1f} KB\n"
            f"⏰ Время: {get_moscow_time().strftime('%d.%m.%Y %H:%M')}\n\n"
            f"📊 *Всего backup файлов:* {len(backup_files)}\n\n"
            f"*Данные будут автоматически восстановлены при перезапуске*"
        )
    else:
        text = (
            f"❌ *Backup не создан*\n\n"
            f"⚠️ Не удалось создать локальный backup\n\n"
            f"*Возможные причины:*\n"
            f"• Ошибка доступа к файлам\n"
            f"• Недостаточно места в /tmp/\n"
            f"• Проблемы с базой данных"
        )
    
    keyboard = [
        [InlineKeyboardButton("📊 Статус backup", callback_data="backup_status")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, parse_mode='Markdown', reply_markup=reply_markup)

async def show_db_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🎯 ПОКАЗЫВАЕТ СТАТИСТИКУ БАЗЫ ДАННЫХ (ИСПРАВЛЕННАЯ)"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not db.is_admin(user_id):
        await query.answer("❌ У вас нет доступа к этой функции", show_alert=True)
        return
    
    try:
        # Получаем статистику
        total_users = db.get_total_users_count()
        active_users = db.get_active_users_count(30)
        
        cursor = db.execute_with_retry('SELECT COUNT(*) FROM appointments')
        total_appointments = cursor.fetchone()[0]
        
        cursor = db.execute_with_retry('SELECT COUNT(*) FROM appointments WHERE appointment_date >= DATE("now")')
        future_appointments = cursor.fetchone()[0]
        
        # Получаем размер БД
        db_path = '/tmp/barbershop.db'
        if os.path.exists(db_path):
            size_mb = os.path.getsize(db_path) / (1024 * 1024)
            size_info = f"{size_mb:.2f} MB"
        else:
            size_info = "не найден"
        
        # Получаем статус backup (ИСПРАВЛЕННАЯ ЧАСТЬ)
        backup_files = db.get_backup_files_info()
        backup_status = db.get_backup_status()
        
        # 🎯 ИСПРАВЛЕНИЕ: ПРАВИЛЬНАЯ ОБРАБОТКА ВРЕМЕНИ
        last_backup_time = "нет данных"
        last_backup_status = "нет данных"
        
        if backup_status:
            last_backup = backup_status[0]
            if last_backup and last_backup[0]:  # timestamp exists
                timestamp = last_backup[0]
                # Проверяем тип timestamp
                if isinstance(timestamp, str):
                    last_backup_time = timestamp
                else:
                    # Если это datetime объект
                    last_backup_time = timestamp.strftime("%d.%m.%Y %H:%M")
                
                last_backup_status = "✅ Успешно" if last_backup[2] else "❌ Ошибка"
        
        text = (
            f"📊 *Статистика Базы Данных*\n\n"
            f"👥 *Пользователи:*\n"
            f"• Всего: {total_users}\n"
            f"• Активных (30 дней): {active_users}\n\n"
            f"📅 *Записи:*\n"
            f"• Всего: {total_appointments}\n"
            f"• Будущих: {future_appointments}\n\n"
            f"💾 *Backup система:*\n"
            f"• Файлов backup: {len(backup_files)}\n"
            f"• Последний backup: {last_backup_time}\n"
            f"• Статус: {last_backup_status}\n"
            f"• Размер БД: {size_info}\n\n"
            f"🛠 *Render Free Tier:*\n"
            f"• Память: 512 MB\n"
            f"• Хранилище: Эфемерное (/tmp/)\n"
            f"• Сон: 15 мин неактивности\n"
            f"• Backup: Локальные файлы"
        )
        
        keyboard = [
            [InlineKeyboardButton("🔄 Создать backup", callback_data="create_backup_now")],
            [InlineKeyboardButton("📊 Статус backup", callback_data="backup_status")],
            [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(text, parse_mode='Markdown', reply_markup=reply_markup)
        
    except Exception as e:
        logger.error(f"❌ Ошибка при получении статистики БД: {e}")
        await query.edit_message_text("❌ Ошибка при получении статистики БД")

async def backup_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🎯 КОМАНДА ДЛЯ РУЧНОЙ ПРОВЕРКИ BACKUP"""
    user_id = update.effective_user.id
    
    if not db.is_admin(user_id):
        await update.message.reply_text("❌ У вас нет доступа к этой команде")
        return
    
    # Показываем статус backup
    await show_backup_status(update, context)

async def check_backup_content(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Проверяет содержимое бэкап файла"""
    user_id = update.effective_user.id
    if not db.is_admin(user_id):
        return

    import sqlite3
    import os
    
    backup_path = "/tmp/barbershop_latest_backup.db"
    current_db_path = "/tmp/barbershop.db"
    
    if not os.path.exists(backup_path):
        await update.message.reply_text("❌ Бэкап файл не существует")
        return
    
    try:
        # Проверяем бэкап файл
        conn_backup = sqlite3.connect(backup_path)
        cursor_backup = conn_backup.cursor()
        
        # Считаем записи в бэкапе
        cursor_backup.execute('SELECT COUNT(*) FROM appointments')
        backup_appointments = cursor_backup.fetchone()[0]
        
        cursor_backup.execute('SELECT COUNT(*) FROM bot_users') 
        backup_users = cursor_backup.fetchone()[0]
        
        conn_backup.close()
        
        # Проверяем текущую БД
        conn_current = sqlite3.connect(current_db_path)
        cursor_current = conn_current.cursor()
        
        cursor_current.execute('SELECT COUNT(*) FROM appointments')
        current_appointments = cursor_current.fetchone()[0]
        
        cursor_current.execute('SELECT COUNT(*) FROM bot_users')
        current_users = cursor_current.fetchone()[0]
        
        conn_current.close()
        
        text = (
            f"🔍 *Диагностика Бэкапа:*\n\n"
            f"📁 *Бэкап файл:* {backup_path}\n"
            f"📊 *Записей в бэкапе:* {backup_appointments}\n"
            f"👥 *Пользователей в бэкапе:* {backup_users}\n\n"
            f"📁 *Текущая БД:* {current_db_path}\n"
            f"📊 *Записей сейчас:* {current_appointments}\n"
            f"👥 *Пользователей сейчас:* {current_users}\n\n"
        )
        
        if backup_appointments > 0 and current_appointments == 0:
            text += "⚠️ *ПРОБЛЕМА:* В бэкапе есть данные, но в текущей БД нет!\n"
            text += "Восстановление не работает!"
        elif backup_appointments == 0:
            text += "⚠️ *ПРОБЛЕМА:* Бэкап файл пустой!"
        else:
            text += "✅ Данные совпадают"
            
        await update.message.reply_text(text, parse_mode='Markdown')
        
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка проверки: {e}")

# Настройка логирования
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)
logging.getLogger('telegram').setLevel(logging.WARNING)
logging.getLogger('apscheduler').setLevel(logging.WARNING)

# Только наши логи в INFO
logging.getLogger(__name__).setLevel(logging.INFO)

# Состояния для ConversationHandler
SERVICE, DATE, TIME, PHONE = range(4)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

db = database.Database()

# Создаем Flask приложение для веб-сервера
web_app = Flask(__name__)

@web_app.route('/')
def home():
    """Главная страница веб-сервера"""
    current_time = database.get_moscow_time().strftime("%Y-%m-%d %H:%M:%S")
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Бот Парикмахерской</title>
        <meta charset="utf-8">
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; text-align: center; }}
            .status {{ color: green; font-weight: bold; }}
            .container {{ max-width: 800px; margin: 0 auto; padding: 20px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🤖 Бот Парикмахерской "Бархат"</h1>
            <p>Статус: <span class="status">Активен ✅</span></p>
            <p>Время сервера: {current_time}</p>
            <p>
                <a href="/health">Проверка здоровья</a> | 
                <a href="/ping">Ping</a> |
                <a href="/status">Статус</a>
            </p>
            <div style="margin-top: 30px; padding: 20px; background: #f5f5f5; border-radius: 10px;">
                <h3>📊 Статистика сервиса</h3>
                <p>• Бот работает в режиме 24/7</p>
                <p>• Автоматические напоминания клиентам</p>
                <p>• Визуальное расписание для администраторов</p>
                <p>• Система управления записями</p>
            </div>
        </div>
    </body>
    </html>
    """

@web_app.route('/health')
def health():
    """Проверка здоровья сервиса"""
    return {
        "status": "healthy",
        "service": "barbershop-bot",
        "timestamp": database.get_moscow_time().isoformat(),
        "database": "connected" if db.conn else "disconnected"
    }

@web_app.route('/ping')
def ping():
    """Простой ping-эндпоинт для self-ping"""
    return "pong"

@web_app.route('/keep-alive')
def keep_alive():
    """Эндпоинт для поддержания активности"""
    logger.info("🔄 Keep-alive request received")
    return {"status": "awake", "timestamp": database.get_moscow_time().isoformat()}

@web_app.route('/status')
def status():
    """Детальный статус сервиса"""
    return {
        "status": "running",
        "service": "barbershop-bot", 
        "timestamp": database.get_moscow_time().isoformat(),
        "bot_restarts": "auto_recovery_enabled",
        "uptime": "24/7_monitoring"
    }

@web_app.route('/deep-health')
def deep_health():
    """Глубокая проверка здоровья"""
    try:
        # Проверяем подключение к базе данных
        db_status = "connected" if db.conn else "disconnected"
        
        # Проверяем токен бота
        bot_token = config.BOT_TOKEN
        bot_info_url = f"https://api.telegram.org/bot{bot_token}/getMe"
        try:
            with httpx.Client(timeout=10) as client:
                bot_response = client.get(bot_info_url)
            bot_status = "active" if bot_response.status_code == 200 else "inactive"
        except Exception:
            bot_status = "connection_error"
        
        return {
            "status": "healthy",
            "database": db_status,
            "telegram_bot": bot_status,
            "timestamp": database.get_moscow_time().isoformat()
        }
    except Exception as e:
        return {"status": "degraded", "error": str(e)}, 500

@web_app.route('/active')
def active():
    """Эндпоинт активности"""
    return {"active": True, "timestamp": database.get_moscow_time().isoformat()}

@web_app.route('/alive')
def alive():
    """Эндпоинт живости"""
    return "ALIVE"

@web_app.route('/ready')
def ready():
    """Эндпоинт готовности"""
    return {"ready": True, "service": "barbershop-bot"}

@web_app.route('/check')
def check():
    """Простой чек-эндпоинт"""
    return "OK"

@web_app.route('/monitor')
def monitor():
    """Эндпоинт мониторига"""
    return {
        "status": "operational",
        "timestamp": database.get_moscow_time().isoformat(),
        "service": "barbershop-bot"
    }

def run_web_server():
    """Запускает веб-сервер в основном потоке"""
    port = int(os.getenv('PORT', 10000))
    logger.info(f"🌐 Starting web server on port {port}")
    
    # Отключаем логирование Werkzeug
    import logging
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)
    
    # ДОБАВЛЯЕМ HEALTHCHECK ДЛЯ RENDER
    @web_app.route('/healthcheck')
    def healthcheck():
        return "OK", 200
    
    logger.info("🚀 Using Flask development server")
    
    try:
        web_app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False, threaded=True)
        logger.info("✅ Web server started successfully")
    except Exception as e:
        logger.error(f"❌ Web server failed to start: {e}")
        raise

def start_enhanced_self_ping():
    """Улучшенная система keep-alive для Render"""
    def enhanced_ping_loop():
        while True:
            try:
                # УВЕЛИЧИМ ЧАСТОТУ: ждем 2 минуты вместо 5
                time.sleep(120)
                
                # 1. Пингуем сами себя через localhost (существующий код)
                port = int(os.getenv('PORT', 5000))
                try:
                    import requests
                    # Проверяем общее здоровье
                    health_url = f"http://localhost:{port}/deep-health"
                    response = requests.get(health_url, timeout=5)
                    
                    if response.status_code == 200:
                        health_data = response.json()
                        if health_data.get('status') == 'healthy':
                            logger.info("✅ Health check: ALL SYSTEMS GO")
                        else:
                            logger.warning(f"⚠️ Health check degraded: {health_data}")
                    else:
                        logger.warning(f"⚠️ Health check failed with status: {response.status_code}")
                        
                except Exception as e:
                    logger.warning(f"⚠️ Health check failed: {e}")

                # Отдельный блок для keep-alive пинга
                try:
                    import requests
                    local_ping = f"http://localhost:{port}/keep-alive"
                    response = requests.get(local_ping, timeout=5)
                    logger.info("✅ Internal self-ping successful")
                except Exception as e:
                    logger.warning(f"⚠️ Internal ping failed: {e}")
                
                # 2. ДОБАВЛЯЕМ: пингуем ВНЕШНИЙ URL Render (новый код)
                try:
                    render_url = os.getenv('RENDER_EXTERNAL_URL', 'https://barber-bot-xg8f.onrender.com')
                    external_ping_urls = [
                        f"{render_url}/",
                        f"{render_url}/ping",
                        f"{render_url}/keep-alive"
                    ]
                    
                    for url in external_ping_urls:
                        response = requests.get(url, timeout=10)
                        if response.status_code == 200:
                            logger.info(f"🌐 Render external ping: {url} - SUCCESS")
                        else:
                            logger.warning(f"🌐 Render external ping: {url} - {response.status_code}")
                            
                except Exception as e:
                    logger.warning(f"🌐 Render external ping failed: {e}")
                
                # 3. Пингуем внешние сервисы (существующий код)
                external_urls = [
                    "https://www.google.com",
                    "https://api.telegram.org", 
                    "https://www.github.com"
                ]
                
                for url in external_urls:
                    try:
                        response = requests.get(url, timeout=10)
                        logger.info(f"🌐 External ping to {url}: {response.status_code}")
                    except Exception as e:
                        logger.warning(f"🌐 External ping failed to {url}: {e}")
                        
            except Exception as e:
                logger.error(f"❌ Self-ping loop error: {e}")
                time.sleep(60)
    
    ping_thread = threading.Thread(target=enhanced_ping_loop, daemon=True)
    ping_thread.start()
    logger.info("🔁 Enhanced self-ping service started")

def signal_handler(signum, frame):
    """Обработчик сигналов для graceful shutdown"""
    logger.info(f"📞 Received signal {signum}, performing graceful shutdown...")
    sys.exit(0)

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Глобальный обработчик ошибок с улучшенной обработкой конфликтов"""
    error = context.error
    
    # Игнорируем ошибку "message is not modified"
    if isinstance(error, BadRequest):
        if "message is not modified" in str(error).lower():
            logger.debug("Message not modified - ignoring")
            return
        elif "chat not found" in str(error).lower():
            logger.warning(f"Chat not found: {error}")
            return
        elif "message to edit not found" in str(error).lower():
            logger.warning(f"Message to edit not found: {error}")
            return
    
    # УЛУЧШЕННАЯ ОБРАБОТКА CONFLICT ОШИБОК
    if isinstance(error, Conflict):
        logger.error(f"❌ CONFLICT: Обнаружен другой запущенный экземпляр бота.")
        logger.info("🔄 Ожидание 10 секунд перед перезапуском...")
        
        # Даем время другому процессу завершиться
        await asyncio.sleep(10)
        
        # Пытаемся аккуратно перезапуститься
        try:
            if 'application' in context.bot_data:
                await context.application.stop()
                await context.application.shutdown()
        except:
            pass
            
        logger.info("🔄 Перезапуск бота...")
        return
    
    # Логируем другие ошибки, но не паникуем
    logger.error(f"Exception while handling an update: {error}", exc_info=error)

def get_moscow_time():
    """Возвращает текущее московское время (UTC+3)"""
    return datetime.now(timezone(timedelta(hours=3)))

def get_moscow_time_from_naive(naive_datetime):
    """Конвертирует наивное datetime в московское время"""
    moscow_tz = timezone(timedelta(hours=3))
    if naive_datetime.tzinfo is None:
        # Если naive datetime, добавляем московский часовой пояс
        return naive_datetime.replace(tzinfo=moscow_tz)
    else:
        # Если уже имеет часовой пояс, конвертируем в московский
        return naive_datetime.astimezone(moscow_tz)

def get_main_keyboard(user_id):
    """Создает основную клавиатуру под сообщением"""
    keyboard = []
    
    if db.is_admin(user_id):
        # Клавиатура для администратора - ИСПРАВЛЕННЫЕ НАЗВАНИЯ
        keyboard = [
            [KeyboardButton("📝 Записать клиента вручную")],
            [KeyboardButton("🗓️ График работы")],
            [KeyboardButton("📋 Мои записи"), KeyboardButton("❌ Отменить запись")],
            [KeyboardButton("📊 Записи сегодня"), KeyboardButton("📅 Записи на неделю"), KeyboardButton("👑 Все записи")],
            [KeyboardButton("📈 Статистика"), KeyboardButton("👥 Управление администраторами")]
        ]
    else:
        # Клавиатура для обычного пользователя
        keyboard = [
            [KeyboardButton("📅 Записаться на стрижку")],
            [KeyboardButton("📋 Мои записи"), KeyboardButton("❌ Отменить запись")],
            [KeyboardButton("🗓️ График работы"), KeyboardButton("ℹ️ О парикмахерской")]
        ]
    
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, is_persistent=True)

def get_phone_keyboard():
    """Создает клавиатуру для ввода телефона"""
    return ReplyKeyboardMarkup([
        [KeyboardButton("📞 Отправить мой номер", request_contact=True)],
        [KeyboardButton("🔙 Назад")]
    ], resize_keyboard=True, one_time_keyboard=True)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    user = update.effective_user
    
    # Добавляем/обновляем пользователя в статистике
    db.add_or_update_user(user.id, user.username, user.first_name, user.last_name)
    
    keyboard = get_main_keyboard(user.id)
    
    welcome_text = (
        f"👋 Добро пожаловать в парикмахерскую *{config.BARBERSHOP_NAME}*, {user.first_name}!\n\n"
        "Я - бот для записи на стрижку. Выберите действие на клавиатуре ниже:\n\n"
    )
    
    if db.is_admin(user.id):
        welcome_text += (
            "📝 *Записать клиента вручную* - запись клиента по телефону или при личной встрече\n"
            "📋 *Мои записи* - записи, внесенные вручную\n"
            "❌ *Отменить запись* - отменить запись\n"
            "👑 *Все записи* - просмотр всех записей\n"
            "📊 *Записи сегодня* - записи на сегодня\n"
            "📈 *Статистика* - статистика пользователей бота\n"
            "🗓️ *График работы* - настройка расписания\n"
            "👥 *Управление администраторами* - управление правами доступа"
        )
    else:
        welcome_text += (
            "📅 *Записаться на стрижку* - выбрать услугу и время\n"
            "📋 *Мои записи* - посмотреть ваши записи\n"
            "❌ *Отменить запись* - отменить вашу запись\n"
            "🗓️ *График работы* - посмотреть расписание работы\n"
            "ℹ️ *О парикмахерской* - информация о нас"
        )
    
    await update.message.reply_text(
        welcome_text,
        reply_markup=keyboard,
        parse_mode='Markdown'
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    # Определяем тип контента и текст
    if update.message.text:
        content_type = "text"
        text = update.message.text
    elif update.message.contact:
        content_type = "contact" 
        text = "КОНТАКТ"
    else:
        content_type = "other"
        text = ""
    
    logger.info(f"🔍 handle_message: пользователь {user_id}, тип: {content_type}, текст: '{text}'")
    logger.info(f"🔍 awaiting_phone: {context.user_data.get('awaiting_phone', 'NOT SET')}")
    logger.info(f"🔍 awaiting_admin_id: {context.user_data.get('awaiting_admin_id', 'NOT SET')}")
    
    # УБРАНА обработка контакта - теперь этим занимается ConversationHandler
    
    # ПЕРВЫЙ приоритет: обработка ввода телефона
    if context.user_data.get('awaiting_phone'):
        logger.info(f"🔍 awaiting_phone=True, передаем в phone_input")
        await phone_input(update, context)
        return
    
    # ВТОРОЙ приоритет: обработка ввода ID администратора
    if context.user_data.get('awaiting_admin_id'):
        await handle_admin_id_input(update, context)
        return
    
    # Обновляем время последней активности пользователя
    user = update.effective_user
    db.add_or_update_user(user.id, user.username, user.first_name, user.last_name)
    
    if db.is_admin(user_id):
        # Обработка для администратора
        if text == "📝 Записать клиента вручную":
            await make_appointment_start(update, context, is_admin=True)
        elif text == "👑 Все записи":
            await show_all_appointments(update, context)
        elif text == "📋 Мои записи":
            await show_admin_manual_appointments(update, context)
        elif text == "📊 Записи сегодня":
            await show_today_appointments_visual(update, context)
        elif text == "📅 Записи на неделю":
            await show_week_appointments(update, context)
        elif text == "📈 Статистика":
            await show_statistics(update, context)
        elif text == "❌ Отменить запись":
            await show_cancel_appointment(update, context)
        elif text == "🗓️ График работы":
            await manage_schedule(update, context)
        elif text == "👥 Управление администраторами":
            await manage_admins(update, context)
        elif text == "🔙 Главное меню":
            await show_main_menu(update, context)
        elif text == "🔙 Назад" and context.user_data.get('awaiting_phone'):
            await date_selected_back(update, context)
        else:
            await update.message.reply_text(
                "Пожалуйста, используйте кнопки ниже для навигации",
                reply_markup=get_main_keyboard(user_id)
            )
    else:
        # Обработка для обычного пользователя
        if text == "📅 Записаться на стрижку":
            await make_appointment_start(update, context, is_admin=False)
        elif text == "📋 Мои записи":
            await show_my_appointments(update, context)
        elif text == "❌ Отменить запись":
            await show_cancel_appointment(update, context)
        elif text == "🗓️ График работы":
            await show_work_schedule(update, context)
        elif text == "ℹ️ О парикмахерской":
            await about_barbershop(update, context)
        elif text == "🔙 Главное меню":
            await show_main_menu(update, context)
        elif text == "🔙 Назад" and context.user_data.get('awaiting_phone'):
            await date_selected_back(update, context)
        else:
            await update.message.reply_text(
                "Пожалуйста, используйте кнопки ниже для навигации",
                reply_markup=get_main_keyboard(user_id)
            )

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает главное меню"""
    if update.callback_query:
        query = update.callback_query
        user_id = query.from_user.id
        await query.edit_message_text(
            f"🏠 *Главное меню {config.BARBERSHOP_NAME}*\n\nВыберите действие на клавиатуре ниже:",
            parse_mode='Markdown'
        )
    else:
        user_id = update.effective_user.id
        await update.message.reply_text(
            f"🏠 *Главное меню {config.BARBERSHOP_NAME}*\n\nВыберите действие на клавиатуре ниже:",
            reply_markup=get_main_keyboard(user_id),
            parse_mode='Markdown'
        )

async def show_work_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает график работы для обычного пользователя"""
    schedule = db.get_week_schedule()
    
    text = f"🗓️ *График работы {config.BARBERSHOP_NAME}*\n\n"
    
    for weekday in range(7):
        day_data = schedule[weekday]
        day_name = config.WEEKDAYS[weekday]
        if day_data[4]:  # is_working
            text += f"✅ {day_name}: {day_data[2]} - {day_data[3]}\n"
        else:
            text += f"❌ {day_name}: выходной\n"
    
    text += "\n📍 *Адрес:* г. Нижнекамск, ул. Корабельная д.29\n"
    text += "📞 *Телефон:* +79178766645"
    
    keyboard = [[InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        query = update.callback_query
        await query.edit_message_text(text, parse_mode='Markdown', reply_markup=reply_markup)
    else:
        await update.message.reply_text(
            text,
            reply_markup=get_main_keyboard(update.effective_user.id),
            parse_mode='Markdown'
        )

async def about_barbershop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обновленная информация о парикмахерской"""
    text = (
        f"ℹ️ *О парикмахерской {config.BARBERSHOP_NAME}*\n\n"
        "✂️ *Наши услуги:*\n"
        "• Мужские стрижки\n"
        "• Женские стрижки\n\n"
        "👩‍💼 *Мастер:* Надежда\n\n"
        "📍 *Адрес:*\n"
        "г. Нижнекамск, ул. Корабельная д.29\n"
        "вход со стороны 7 подъезда\n\n"
        "📞 *Контакты:*\n"
        "Мастер Надежда: +79178766645\n\n"
        "💻 *О разработчике:*\n"
        "Хотите такого же бота для своего бизнеса?\n"
        "Обращайтесь: Айрат +79274608748"
    )
    
    if update.callback_query:
        query = update.callback_query
        keyboard = [[InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text, parse_mode='Markdown', reply_markup=reply_markup)
    else:
        await update.message.reply_text(
            text,
            reply_markup=get_main_keyboard(update.effective_user.id),
            parse_mode='Markdown'
        )

async def show_statistics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает статистику пользователей бота (только для администратора)"""
    user_id = update.effective_user.id
    
    if not db.is_admin(user_id):
        await update.message.reply_text("❌ У вас нет доступа к этой функции")
        return
    
    total_users = db.get_total_users_count()
    active_users = db.get_active_users_count(30)
    
    text = (
        f"📈 *Статистика бота {config.BARBERSHOP_NAME}*\n\n"
        f"👥 *Всего пользователей:* {total_users}\n"
        f"🎯 *Активных за 30 дней:* {active_users}\n\n"
        "*Примечание:* пользователь считается активным, если использовал бота в течение последних 30 дней"
    )
    
    keyboard = [
        [InlineKeyboardButton("📊 Отчет за неделю", callback_data="weekly_report")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        query = update.callback_query
        await query.edit_message_text(text, parse_mode='Markdown', reply_markup=reply_markup)
    else:
        await update.message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup)

async def weekly_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает отчет за прошедшую неделю"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not db.is_admin(user_id):
        await query.answer("❌ У вас нет доступа к этой функции", show_alert=True)
        return
    
    try:
        stats = db.get_weekly_stats()
        
        text = (
            f"📊 *ОТЧЕТ ЗА ПРОШЕДШУЮ НЕДЕЛЮ*\n\n"
            f"📅 *Период:* {stats['start_date']} - {stats['end_date']}\n"
            f"📋 *Всего записей:* {stats['total_appointments']}\n"
        )
        
        if stats['peak_time'] != "Нет данных":
            text += f"⏰ *Пиковое время:* {stats['peak_time']} ({stats['peak_time_count']} записей)\n"
        else:
            text += f"⏰ *Пиковое время:* {stats['peak_time']}\n"
            
        text += (
            f"👥 *Новые клиенты:* {stats['new_clients']}\n"
            f"📞 *Постоянные клиенты:* {stats['regular_clients']}"
        )
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="show_statistics")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(text, parse_mode='Markdown', reply_markup=reply_markup)
            
    except Exception as e:
        logger.error(f"Ошибка при формировании отчета: {e}")
        error_text = "❌ Ошибка при формировании отчета. Попробуйте позже."
        await query.edit_message_text(error_text)

async def make_appointment_start(update: Update, context: ContextTypes.DEFAULT_TYPE, is_admin=False):
    """Начало процесса записи"""
    context.user_data.clear()
    context.user_data['is_admin_manual'] = is_admin
    
    keyboard = [
        [InlineKeyboardButton("💇‍♂️ Мужская стрижка", callback_data="service_Мужская стрижка")],
        # [InlineKeyboardButton("💇‍♀️ Женская стрижка", callback_data="service_Женская стрижка")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if is_admin:
        text = "📝 *Запись клиента вручную*\n\n✂️ Выберите услугу:"
    else:
        text = "✂️ Выберите услугу:"
    
    if update.callback_query:
        query = update.callback_query
        await query.edit_message_text(text, reply_markup=reply_markup)
    else:
        await update.message.reply_text(text, reply_markup=reply_markup)

async def service_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора услуги"""
    query = update.callback_query
    service = query.data.split("_")[1]
    context.user_data['service'] = service
    
    keyboard = []
    today = get_moscow_time().date()
    current_time = get_moscow_time().time()
    
    days_shown = 0
    i = 0
    
    while days_shown < 7 and i < 30:
        date = today + timedelta(days=i)
        date_str = date.strftime("%Y-%m-%d")
        display_date = date.strftime("%d.%m.%Y")
        weekday = date.weekday()
        day_name = config.WEEKDAYS[weekday]
        
        schedule = db.get_work_schedule(weekday)
        if schedule and schedule[0][4]:
            start_time, end_time = schedule[0][2], schedule[0][3]
            
            if is_date_available(date, current_time, start_time, end_time, i):
                keyboard.append([InlineKeyboardButton(
                    f"{day_name} {display_date}", 
                    callback_data=f"date_{date_str}"
                )])
                days_shown += 1
        
        i += 1
    
    if not keyboard:
        keyboard = [[InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("К сожалению, нет доступных рабочих дней 😔", reply_markup=reply_markup)
        return
    
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="make_appointment")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    is_admin_manual = context.user_data.get('is_admin_manual', False)
    if is_admin_manual:
        text = f"📝 *Запись клиента вручную*\n\n💇 Услуга: *{service}*\n\n📅 Выберите дату:"
    else:
        text = f"💇 Услуга: *{service}*\n\n📅 Выберите дату:"
    
    await query.edit_message_text(
        text,
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

def is_date_available(date, current_time, start_time, end_time, days_ahead):
    """Проверяет, доступна ли дата для записи с учетом текущего времени"""
    if days_ahead == 0:
        start_dt = datetime.strptime(start_time, "%H:%M").time()
        end_dt = datetime.strptime(end_time, "%H:%M").time()
        
        if current_time >= end_dt:
            return False
        
        last_slot_time = (datetime.strptime(end_time, "%H:%M") - timedelta(minutes=30)).time()
        if current_time >= last_slot_time:
            return False
        
        if current_time >= start_dt:
            return True
    
    return True

async def date_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора даты"""
    query = update.callback_query
    
    if 'service' not in context.user_data:
        await query.edit_message_text(
            "❌ Ошибка: услуга не выбрана. Пожалуйста, начните запись заново.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]])
        )
        return
    
    date = query.data.split("_")[1]
    context.user_data['date'] = date
    
    available_slots = db.get_available_slots(date)
    
    today = get_moscow_time().date()
    selected_date = datetime.strptime(date, "%Y-%m-%d").date()
    current_time = get_moscow_time().time()
    
    if selected_date == today:
        weekday = selected_date.weekday()
        schedule = db.get_work_schedule(weekday)
        if schedule and schedule[0][4]:
            start_time, end_time = schedule[0][2], schedule[0][3]
            available_slots = filter_available_slots(available_slots, current_time, start_time, end_time)
    
    if not available_slots:
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=f"service_{context.user_data['service']}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("На эту дату нет свободных мест 😔", reply_markup=reply_markup)
        return
    
    keyboard = []
    for slot in available_slots:
        keyboard.append([InlineKeyboardButton(slot, callback_data=f"time_{slot}")])
    
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"service_{context.user_data['service']}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    selected_date_obj = datetime.strptime(date, "%Y-%m-%d").date()
    weekday = selected_date_obj.weekday()
    day_name = config.WEEKDAYS[weekday]
    display_date = selected_date_obj.strftime("%d.%m.%Y")
    
    is_admin_manual = context.user_data.get('is_admin_manual', False)
    if is_admin_manual:
        text = f"📝 *Запись клиента вручную*\n\n💇 Услуга: *{context.user_data['service']}*\n\n📅 Дата: *{day_name} {display_date}*\n\n⏰ Выберите время:"
    else:
        text = f"📅 Дата: *{day_name} {display_date}*\n\n⏰ Выберите время:"
    
    await query.edit_message_text(
        text,
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

def filter_available_slots(slots, current_time, start_time, end_time):
    """Фильтрует доступные слоты с учетом текущего времени"""
    filtered_slots = []
    
    for slot in slots:
        slot_time = datetime.strptime(slot, "%H:%M").time()
        
        if slot_time > current_time:
            start_dt = datetime.strptime(start_time, "%H:%M").time()
            end_dt = datetime.strptime(end_time, "%H:%M").time()
            
            if start_dt <= slot_time < end_dt:
                filtered_slots.append(slot)
    
    return filtered_slots

async def time_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора времени - переходим к вводу телефона"""
    query = update.callback_query
    time = query.data.split("_")[1]
    context.user_data['time'] = time
    context.user_data['awaiting_phone'] = True
    
    logger.info(f"🎯 TIME_SELECTED: время {time}, awaiting_phone=True")
    
    is_admin_manual = context.user_data.get('is_admin_manual', False)
    
    if is_admin_manual:
        text = (
            "📝 *Запись клиента вручную*\n\n"
            "📞 *Введите номер телефона клиента:*\n\n"
            "*Формат:* +7XXXXXXXXXX или 8XXXXXXXXXX\n"
            "*Пример:* +79123456789 или 89123456789\n\n"
            "Или нажмите кнопку ниже, чтобы отправить номер автоматически:"
        )
    else:
        text = (
            "📞 *Для записи введите Ваш номер телефона*\n\n"
            "*Формат:* +7XXXXXXXXXX или 8XXXXXXXXXX\n"
            "*Пример:* +79123456789 или 89123456789\n\n"
            "Или нажмите кнопку ниже, чтобы отправить номер автоматически:"
        )
    
    phone_keyboard = get_phone_keyboard()
    
    await query.message.reply_text(
        text,
        parse_mode='Markdown',
        reply_markup=phone_keyboard
    )
    
    logger.info(f"🎯 Возвращаем состояние PHONE: {PHONE}")
    return PHONE

async def date_selected_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Возврат к выбору даты при нажатии 'Назад' во время ввода телефона"""
    context.user_data['awaiting_phone'] = False
    
    date = context.user_data['date']
    available_slots = db.get_available_slots(date)
    
    today = get_moscow_time().date()
    selected_date = datetime.strptime(date, "%Y-%m-%d").date()
    current_time = get_moscow_time().time()
    
    if selected_date == today:
        weekday = selected_date.weekday()
        schedule = db.get_work_schedule(weekday)
        if schedule and schedule[0][4]:
            start_time, end_time = schedule[0][2], schedule[0][3]
            available_slots = filter_available_slots(available_slots, current_time, start_time, end_time)
    
    keyboard = []
    for slot in available_slots:
        keyboard.append([InlineKeyboardButton(slot, callback_data=f"time_{slot}")])
    
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"service_{context.user_data['service']}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    selected_date_obj = datetime.strptime(date, "%Y-%m-%d").date()
    weekday = selected_date_obj.weekday()
    day_name = config.WEEKDAYS[weekday]
    display_date = selected_date_obj.strftime("%d.%m.%Y")
    
    is_admin_manual = context.user_data.get('is_admin_manual', False)
    if is_admin_manual:
        text = f"📝 *Запись клиента вручную*\n\n💇 Услуга: *{context.user_data['service']}*\n\n📅 Дата: *{day_name} {display_date}*\n\n⏰ Выберите время:"
    else:
        text = f"📅 Дата: *{day_name} {display_date}*\n\n⏰ Выберите время:"
    
    await update.message.reply_text(
        text,
        parse_mode='Markdown',
        reply_markup=reply_markup
    )
    
    return ConversationHandler.END

async def phone_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"🔍 phone_input ВЫЗВАН для пользователя {update.effective_user.id}")
    
    if update.message.contact:
        phone = update.message.contact.phone_number
        logger.info(f"📞 Получен контакт: {phone}")
        normalized_phone = normalize_phone(phone)
        logger.info(f"✅ Контакт нормализован: {normalized_phone}")
        
    else:
        phone = update.message.text.strip()
        logger.info(f"📞 Получен текст: {phone}")
        
        if not is_valid_phone(phone):
            logger.info(f"❌ Невалидный телефон: {phone}")
            phone_keyboard = get_phone_keyboard()
            
            is_admin_manual = context.user_data.get('is_admin_manual', False)
            if is_admin_manual:
                text = (
                    "❌ Неверный формат номера телефона.\n\n"
                    "📞 *Введите номер телефона клиента:*\n\n"
                    "*Формат:* +7XXXXXXXXXX или 8XXXXXXXXXX\n"
                    "*Пример:* +79123456789 или 89123456789\n\n"
                    "Пожалуйста, введите номер еще раз:"
                )
            else:
                text = (
                    "❌ Неверный формат номера телефона.\n\n"
                    "📞 *Для записи введите Ваш номер телефона*\n\n"
                    "*Формат:* +7XXXXXXXXXX или 8XXXXXXXXXX\n"
                    "*Пример:* +79123456789 или 89123456789\n\n"
                    "Пожалуйста, введите номер еще раз:"
                )
            
            await update.message.reply_text(
                text,
                parse_mode='Markdown',
                reply_markup=phone_keyboard
            )
            return PHONE
        
        normalized_phone = normalize_phone(phone)
        logger.info(f"✅ Текст нормализован: {normalized_phone}")
    
    context.user_data['phone'] = normalized_phone
    logger.info(f"✅ Телефон сохранен в user_data: {normalized_phone}")
    
    user = update.effective_user
    user_data = context.user_data

    is_admin_manual = context.user_data.get('is_admin_manual', False)
    logger.info(f"🔧 is_admin_manual: {is_admin_manual}")

    try:
        logger.info("🔄 Пытаемся создать запись в БД...")
        
        # 🎯 ИСПРАВЛЕНИЕ: Всегда используем валидный user_id
        appointment_user_id = user.id  # ✅ Всегда берем ID текущего пользователя
        appointment_user_name = "Администратор (ручная запись)" if is_admin_manual else user.full_name
        appointment_username = "admin_manual" if is_admin_manual else user.username
        
        appointment_id = db.add_appointment(
            user_id=appointment_user_id,
            user_name=appointment_user_name,
            user_username=appointment_username,
            phone=normalized_phone,
            service=user_data['service'],
            date=user_data['date'],
            time=user_data['time']
        )
        logger.info(f"✅ Запись создана с ID: {appointment_id}")

        # ДОБАВЛЕНО: Расчет day_name и display_date перед использованием
        selected_date_obj = datetime.strptime(user_data['date'], "%Y-%m-%d").date()
        weekday = selected_date_obj.weekday()
        day_name = config.WEEKDAYS[weekday]
        display_date = selected_date_obj.strftime("%d.%m.%Y")

        # ИСПРАВЛЕНО: Один вызов schedule_appointment_reminders вместо двух
        if not is_admin_manual:
            await schedule_appointment_reminders(
                context, 
                appointment_id, 
                user_data['date'], 
                user_data['time'], 
                user.id
            )
            logger.info(f"🎯 Напоминания запланированы для записи #{appointment_id}")
        else:
            logger.info(f"⏩ Пропуск планирования напоминаний для ручной записи администратора #{appointment_id}")
        
        await send_new_appointment_notification(
            context, 
            user_name=appointment_user_name,
            user_username=appointment_username,
            phone=normalized_phone,
            service=user_data['service'],
            date=f"{day_name} {display_date}",
            time=user_data['time'],
            appointment_id=appointment_id,
            is_manual=is_admin_manual
        )
        
        await check_duplicate_appointments(context)
        
        main_keyboard = get_main_keyboard(user.id)
        
        if is_admin_manual:
            success_text = (
                f"✅ *Клиент успешно записан в {config.BARBERSHOP_NAME}!*\n\n"
                f"💇 Услуга: {user_data['service']}\n"
                f"📅 Дата: {day_name} {display_date}\n"
                f"⏰ Время: {user_data['time']}\n"
                f"📞 Телефон: {normalized_phone}\n\n"
                f"Запись внесена вручную администратором"
            )
        else:
            success_text = (
                f"✅ *Запись в {config.BARBERSHOP_NAME} успешно создана!*\n\n"
                f"💇 Услуга: {user_data['service']}\n"
                f"📅 Дата: {day_name} {display_date}\n"
                f"⏰ Время: {user_data['time']}\n"
                f"📞 Телефон: {normalized_phone}\n\n"
                f"Ждём вас в парикмахерской! 🏃‍♂️"
            )
        
        await update.message.reply_text(
            success_text,
            parse_mode='Markdown',
            reply_markup=main_keyboard
        )
        
    except Exception as e:
        logger.error(f"Ошибка при создании записи: {e}")
        main_keyboard = get_main_keyboard(user.id)
        
        if "уже занято" in str(e):
            await update.message.reply_text(
                "❌ Это время уже занято другим клиентом. Пожалуйста, выберите другое время.",
                reply_markup=main_keyboard
            )
        else:
            await update.message.reply_text(
                "❌ Произошла ошибка при создании записи. Пожалуйста, попробуйте еще раз.",
                reply_markup=main_keyboard
            )
    
    context.user_data.clear()
    context.user_data['awaiting_phone'] = False
    logger.info(f"✅ phone_input завершен, awaiting_phone установлен в False")
    
    return ConversationHandler.END

def add_appointment(self, user_id, user_name, user_username, phone, service, date, time):
    """Добавляет новую запись С АВТОМАТИЧЕСКИМ BACKUP"""
    try:
        # Проверяем, не занято ли время
        cursor = self.execute_with_retry('''
            SELECT COUNT(*) FROM appointments 
            WHERE appointment_date = ? AND appointment_time = ?
        ''', (date, time))
        
        if cursor.fetchone()[0] > 0:
            raise Exception("Это время уже занято другим клиентом")
        
        cursor = self.execute_with_retry('''
            INSERT INTO appointments (user_id, user_name, user_username, phone, service, appointment_date, appointment_time)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (user_id, user_name, user_username, phone, service, date, time))
        
        appointment_id = cursor.lastrowid
        
        self.execute_with_retry('''
            INSERT OR REPLACE INTO schedule (date, time, available)
            VALUES (?, ?, ?)
        ''', (date, time, False))
        
        self.conn.commit()
        
        # 🎯 АВТОМАТИЧЕСКИЙ BACKUP ПОСЛЕ КАЖДОЙ НОВОЙ ЗАПИСИ
        if self.backup_enabled:
            logger.info(f"💾 Автоматический backup после создания записи #{appointment_id}")
            self.create_backup()
        
        return appointment_id
        
    except Exception as e:
        logger.error(f"❌ Ошибка БД в add_appointment: {e}")
        raise

async def schedule_appointment_reminders(context: ContextTypes.DEFAULT_TYPE, appointment_id: int, date: str, time: str, user_id: int):
    """Планирует напоминания для новой записи сразу при создании"""
    cursor = None
    try:
        logger.info(f"🎯 Планирование напоминаний для записи #{appointment_id}")
        
        current_moscow = get_moscow_time()
        
        # Создаем datetime объект в московском времени
        appointment_naive = datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M")
        appointment_moscow = appointment_naive.replace(tzinfo=timezone(timedelta(hours=3)))
        
        logger.info(f"📅 Время записи: {appointment_moscow.strftime('%d.%m.%Y %H:%M')} MSK")
        
        # 24-часовое напоминание
        reminder_24h_moscow = appointment_moscow - timedelta(hours=24)
        time_until_24h = (reminder_24h_moscow - current_moscow).total_seconds()
        
        if time_until_24h > 0:
            # Сохраняем в БД
            cursor = db.execute_with_retry('''
                INSERT INTO scheduled_reminders (appointment_id, reminder_type, scheduled_time)
                VALUES (?, ?, ?)
                ON CONFLICT (appointment_id, reminder_type) DO UPDATE SET
                scheduled_time = excluded.scheduled_time,
                sent = FALSE
            ''', (appointment_id, '24h', reminder_24h_moscow))
            
            # Конвертируем в UTC для job_queue
            reminder_24h_utc = reminder_24h_moscow.astimezone(timezone.utc)
            
            # Планируем задачу
            context.job_queue.run_once(
                callback=send_single_24h_reminder,
                when=reminder_24h_utc,
                data={'appointment_id': appointment_id, 'user_id': user_id},
                name=f"24h_reminder_{appointment_id}"
            )
            logger.info(f"✅ Запланировано 24h напоминание для #{appointment_id}")
        else:
            logger.info(f"⏩ 24h напоминание для #{appointment_id} пропущено (время прошло)")
        
        # 1-часовое напоминание
        reminder_1h_moscow = appointment_moscow - timedelta(hours=1)
        time_until_1h = (reminder_1h_moscow - current_moscow).total_seconds()
        
        if time_until_1h > 0:
            # Сохраняем в БД
            cursor = db.execute_with_retry('''
                INSERT INTO scheduled_reminders (appointment_id, reminder_type, scheduled_time)
                VALUES (?, ?, ?)
                ON CONFLICT (appointment_id, reminder_type) DO UPDATE SET
                scheduled_time = excluded.scheduled_time,
                sent = FALSE
            ''', (appointment_id, '1h', reminder_1h_moscow))
            
            # Конвертируем в UTC для job_queue
            reminder_1h_utc = reminder_1h_moscow.astimezone(timezone.utc)
            
            # Планируем задачу
            context.job_queue.run_once(
                callback=send_single_1h_reminder,
                when=reminder_1h_utc,
                data={'appointment_id': appointment_id, 'user_id': user_id},
                name=f"1h_reminder_{appointment_id}"
            )
            logger.info(f"✅ Запланировано 1h напоминание для #{appointment_id}")
        else:
            logger.info(f"⏩ 1h напоминание для #{appointment_id} пропущено (время прошло)")
            
        db.conn.commit()
        logger.info(f"✅ Все напоминания для записи #{appointment_id} запланированы")
            
    except Exception as e:
        logger.error(f"❌ Ошибка при планировании напоминаний для #{appointment_id}: {e}")
        import traceback
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
    finally:
        # Всегда закрываем курсор
        if cursor:
            cursor.close()

async def send_single_24h_reminder(context: ContextTypes.DEFAULT_TYPE):
    """Отправляет одно 24-часовое напоминание для конкретной записи"""
    try:
        job = context.job
        appointment_id = job.data['appointment_id']
        user_id = job.data['user_id']
        
        logger.info(f"🔔 [24h] START отправка напоминания для #{appointment_id}, user_id: {user_id}")
        
        moscow_time = get_moscow_time()
        logger.info(f"⏰ [24h] Отправка напоминания для записи #{appointment_id} пользователю {user_id} в {moscow_time.strftime('%d.%m.%Y %H:%M')} MSK")
        
        cursor = db.execute_with_retry('''
            SELECT user_name, user_username, phone, service, appointment_date, appointment_time 
            FROM appointments WHERE id = ?
        ''', (appointment_id,))
        result = cursor.fetchone()
        
        if not result:
            logger.error(f"❌ Запись #{appointment_id} не найдена для напоминания")
            return
        
        user_name, user_username, phone, service, date, time = result
        
        # Пропускаем напоминания для ручных записей администратора
        if user_name == "Администратор":
            logger.info(f"⏩ Пропуск 24h напоминания для записи администратора #{appointment_id}")
            cursor = db.execute_with_retry('''
                UPDATE scheduled_reminders 
                SET sent = TRUE 
                WHERE appointment_id = ? AND reminder_type = '24h'
            ''', (appointment_id,))
            db.conn.commit()
            return
        
        appointment_date = datetime.strptime(date, "%Y-%m-%d").date()
        weekday = appointment_date.weekday()
        day_name = config.WEEKDAYS[weekday]
        display_date = appointment_date.strftime("%d.%m.%Y")
        
        text = (
            f"⏰ *Напоминание о записи в {config.BARBERSHOP_NAME}!*\n\n"
            f"Напоминаем, что завтра у вас запись:\n\n"
            f"💇 Услуга: {service}\n"
            f"📅 Дата: {day_name} {display_date}\n"
            f"⏰ Время: {time}\n\n"
            f"📍 *Адрес:* г. Нижнекамск, ул. Корабельная д.29\n"
            f"📞 *Телефон:* +79178766645\n\n"
            f"*Ждём вас!* ✂️"
        )
        
        await context.bot.send_message(chat_id=user_id, text=text, parse_mode='Markdown')
        
        cursor = db.execute_with_retry('''
            UPDATE scheduled_reminders 
            SET sent = TRUE 
            WHERE appointment_id = ? AND reminder_type = '24h'
        ''', (appointment_id,))
        db.conn.commit()
        
        logger.info(f"✅ 24h напоминание отправлено пользователю {user_id} для записи #{appointment_id}")
        
    except BadRequest as e:
        if "chat not found" in str(e).lower():
            logger.warning(f"⚠️ Chat not found for user {user_id}, skipping 24h reminder")
            cursor = db.execute_with_retry('''
                UPDATE scheduled_reminders 
                SET sent = TRUE 
                WHERE appointment_id = ? AND reminder_type = '24h'
            ''', (appointment_id,))
            db.conn.commit()
        else:
            logger.error(f"❌ BadRequest при отправке 24h напоминания: {e}")
    except Exception as e:
        logger.error(f"❌ Ошибка отправки 24h напоминания для записи #{appointment_id}: {e}")
        import traceback
        logger.error(f"❌ Traceback: {traceback.format_exc()}")

async def send_single_1h_reminder(context: ContextTypes.DEFAULT_TYPE):
    """Отправляет одно 1-часовое напоминание для конкретной записи"""
    try:
        job = context.job
        appointment_id = job.data['appointment_id']
        user_id = job.data['user_id']
        
        logger.info(f"🔔 [1h] START отправка напоминания для #{appointment_id}, user_id: {user_id}")
        
        moscow_time = get_moscow_time()
        logger.info(f"⏰ [1h] Отправка напоминания для записи #{appointment_id} пользователю {user_id} в {moscow_time.strftime('%d.%m.%Y %H:%M')} MSK")
        
        cursor = db.execute_with_retry('''
            SELECT user_name, user_username, phone, service, appointment_date, appointment_time 
            FROM appointments WHERE id = ?
        ''', (appointment_id,))
        result = cursor.fetchone()
        
        if not result:
            logger.error(f"❌ Запись #{appointment_id} не найдена для напоминания")
            return
        
        user_name, user_username, phone, service, date, time = result
        
        # Пропускаем напоминания для ручных записей администратора
        if user_name == "Администратор":
            logger.info(f"⏩ Пропуск 1h напоминания для записи администратора #{appointment_id}")
            cursor = db.execute_with_retry('''
                UPDATE scheduled_reminders 
                SET sent = TRUE 
                WHERE appointment_id = ? AND reminder_type = '1h'
            ''', (appointment_id,))
            db.conn.commit()
            return
        
        appointment_date = datetime.strptime(date, "%Y-%m-%d").date()
        weekday = appointment_date.weekday()
        day_name = config.WEEKDAYS[weekday]
        display_date = appointment_date.strftime("%d.%m.%Y")
        
        text = (
            f"⏰ *Скоро встреча в {config.BARBERSHOP_NAME}!*\n\n"
            f"Напоминаем, что через 1 час у вас запись:\n\n"
            f"💇 Услуга: {service}\n"
            f"📅 Дата: {day_name} {display_date}\n"
            f"⏰ Время: {time}\n\n"
            f"📍 *Адрес:* г. Нижнекамск, ул. Корабельная д.29\n"
            f"📞 *Телефон:* +79178766645\n\n"
            f"*Не опаздывайте!* 🏃‍♂️"
        )
        
        await context.bot.send_message(chat_id=user_id, text=text, parse_mode='Markdown')
        
        cursor = db.execute_with_retry('''
            UPDATE scheduled_reminders 
            SET sent = TRUE 
            WHERE appointment_id = ? AND reminder_type = '1h'
        ''', (appointment_id,))
        db.conn.commit()
        
        logger.info(f"✅ 1h напоминание отправлено пользователю {user_id} для записи #{appointment_id}")
        
    except BadRequest as e:
        if "chat not found" in str(e).lower():
            logger.warning(f"⚠️ Chat not found for user {user_id}, skipping 1h reminder")
            cursor = db.execute_with_retry('''
                UPDATE scheduled_reminders 
                SET sent = TRUE 
                WHERE appointment_id = ? AND reminder_type = '1h'
            ''', (appointment_id,))
            db.conn.commit()
        else:
            logger.error(f"❌ BadRequest при отправке 1h напоминания: {e}")
    except Exception as e:
        logger.error(f"❌ Ошибка отправки 1h напоминания для записи #{appointment_id}: {e}")
        import traceback
        logger.error(f"❌ Traceback: {traceback.format_exc()}")

async def debug_jobs(context: ContextTypes.DEFAULT_TYPE):
    """Отладочная функция для проверки запланированных задач"""
    try:
        job_queue = context.job_queue
        jobs = job_queue.jobs()
        
        logger.info("📋 === JOB QUEUE DEBUG ===")
        logger.info(f"📋 Всего задач в очереди: {len(jobs)}")
        
        now_moscow = get_moscow_time()
        logger.info(f"🕐 Текущее время MSK: {now_moscow}")
        
        for i, job in enumerate(jobs):
            job_time = job.next_t
            if job_time:
                job_time_moscow = job_time.astimezone(timezone(timedelta(hours=3)))
                time_until = job_time_moscow - now_moscow
                minutes_until = time_until.total_seconds() / 60
                
                logger.info(f"📋 Задача #{i+1}: {job.name}")
                logger.info(f"   ⏰ Время выполнения: {job_time_moscow}")
                logger.info(f"   ⏳ Осталось минут: {minutes_until:.1f}")
                if hasattr(job, 'data'):
                    logger.info(f"   📝 Данные: {job.data}")
        
        logger.info("📋 === END JOB DEBUG ===")
        
    except Exception as e:
        logger.error(f"❌ Ошибка в debug_jobs: {e}")

async def restore_scheduled_reminders(context: ContextTypes.DEFAULT_TYPE):
    """Восстанавливает запланированные напоминания из БД при запуске бота"""
    try:
        cursor = db.execute_with_retry('''
            SELECT sr.appointment_id, sr.reminder_type, sr.scheduled_time, a.user_id 
            FROM scheduled_reminders sr
            JOIN appointments a ON sr.appointment_id = a.id
            WHERE sr.sent = FALSE AND sr.scheduled_time > CURRENT_TIMESTAMP
            ORDER BY sr.scheduled_time
        ''')
        
        reminders = cursor.fetchall()
        logger.info(f"🔄 Восстановление {len(reminders)} напоминаний из БД")
        
        current_moscow = get_moscow_time()
        restored_count = 0
        
        for appointment_id, reminder_type, scheduled_time, user_id in reminders:
            try:
                # Убеждаемся, что время в правильном часовом поясе
                if scheduled_time.tzinfo is None:
                    scheduled_time = scheduled_time.replace(tzinfo=timezone(timedelta(hours=3)))
                
                # Конвертируем в UTC для job_queue
                scheduled_utc = scheduled_time.astimezone(timezone.utc)
                time_until_reminder = (scheduled_time - current_moscow).total_seconds()
                
                logger.info(f"🔍 Восстановление {reminder_type} напоминания для #{appointment_id}")
                logger.info(f"   ⏰ Запланировано на: {scheduled_time.strftime('%d.%m.%Y %H:%M')} MSK")
                logger.info(f"   ⏳ Осталось секунд: {time_until_reminder}")
                
                if time_until_reminder > 0:
                    if reminder_type == '24h':
                        context.job_queue.run_once(
                            callback=send_single_24h_reminder,
                            when=scheduled_utc,
                            data={'appointment_id': appointment_id, 'user_id': user_id},
                            name=f"24h_reminder_{appointment_id}"
                        )
                        restored_count += 1
                        logger.info(f"✅ Восстановлено 24h напоминание для #{appointment_id}")
                    elif reminder_type == '1h':
                        context.job_queue.run_once(
                            callback=send_single_1h_reminder,
                            when=scheduled_utc,
                            data={'appointment_id': appointment_id, 'user_id': user_id},
                            name=f"1h_reminder_{appointment_id}"
                        )
                        restored_count += 1
                        logger.info(f"✅ Восстановлено 1h напоминание для #{appointment_id}")
                else:
                    logger.info(f"⏩ Пропущено восстановление {reminder_type} напоминания для #{appointment_id} (время прошло)")
                    # Помечаем как отправленное, чтобы больше не восстанавливать
                    cursor = db.execute_with_retry('''
                        UPDATE scheduled_reminders 
                        SET sent = TRUE 
                        WHERE appointment_id = ? AND reminder_type = ?
                    ''', (appointment_id, reminder_type))
                    db.conn.commit()
                
            except Exception as e:
                logger.error(f"❌ Ошибка при восстановлении напоминания для записи #{appointment_id}: {e}")
        
        logger.info(f"✅ Всего восстановлено напоминаний: {restored_count}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при восстановлении напоминаний из БД: {e}")
        import traceback
        logger.error(f"❌ Traceback: {traceback.format_exc()}")

def cancel_scheduled_reminders(context: ContextTypes.DEFAULT_TYPE, appointment_id: int):
    """Удаляет запланированные напоминания для отмененной записи"""
    try:
        job_queue = context.job_queue
        removed_count = 0
        
        # Удаляем 24h напоминание
        job_24h_name = f"24h_reminder_{appointment_id}"
        job_24h = job_queue.get_jobs_by_name(job_24h_name)
        if job_24h:
            job_24h[0].schedule_removal()
            removed_count += 1
            logger.info(f"✅ Удалено 24h напоминание для записи #{appointment_id}")
        else:
            logger.info(f"ℹ️ 24h напоминание не найдено для записи #{appointment_id}")
        
        # Удаляем 1h напоминание
        job_1h_name = f"1h_reminder_{appointment_id}"
        job_1h = job_queue.get_jobs_by_name(job_1h_name)
        if job_1h:
            job_1h[0].schedule_removal()
            removed_count += 1
            logger.info(f"✅ Удалено 1h напоминание для записи #{appointment_id}")
        else:
            logger.info(f"ℹ️ 1h напоминание не найдено для записи #{appointment_id}")
            
        # 🎯 ИСПРАВЛЕННЫЙ SQL-ЗАПРОС (убрал %s, заменил на ?)
        cursor = db.conn.cursor()
        cursor.execute('''
            DELETE FROM scheduled_reminders 
            WHERE appointment_id = ? AND sent = FALSE
        ''', (appointment_id,))
        db.conn.commit()
        
        logger.info(f"🎯 Удалено напоминаний из БД для записи #{appointment_id}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при удалении напоминаний для записи #{appointment_id}: {e}")

async def show_admin_manual_appointments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает записи, внесенные администратором вручную"""
    user_id = update.effective_user.id
    
    if not db.is_admin(user_id):
        await update.message.reply_text("❌ У вас нет доступа к этой функции")
        return
    
    all_appointments = db.get_all_appointments()
    manual_appointments = [appt for appt in all_appointments if appt[1] == "Администратор"]
    
    if not manual_appointments:
        keyboard = [[InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if update.callback_query:
            query = update.callback_query
            await query.edit_message_text(
                "📭 Нет записей, внесенных вручную",
                reply_markup=reply_markup
            )
        else:
            await update.message.reply_text(
                "📭 Нет записей, внесенных вручную",
                reply_markup=reply_markup
            )
        return
    
    text = "📋 *Записи, внесенные вручную:*\n\n"
    keyboard = []
    
    for appt in manual_appointments:
        appt_id, user_name, username, phone, service, date, time = appt
        selected_date_obj = datetime.strptime(date, "%Y-%m-%d").date()
        weekday = selected_date_obj.weekday()
        day_name = config.WEEKDAYS[weekday]
        display_date = selected_date_obj.strftime("%d.%m.%Y")
        text += f"🆔 #{appt_id}\n"
        text += f"💇 {service}\n"
        text += f"📅 {day_name} {display_date} ⏰ {time}\n"
        text += f"📞 {phone}\n"
        text += "─" * 20 + "\n"
        keyboard.append([InlineKeyboardButton(
            f"❌ Отменить #{appt_id}", 
            callback_data=f"cancel_admin_{appt_id}"
        )])
    
    keyboard.append([InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        query = update.callback_query
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')

async def show_my_appointments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает записи текущего пользователя"""
    user_id = update.effective_user.id
    
    appointments = db.get_user_appointments(user_id)
    
    if not appointments:
        keyboard = [[InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if update.callback_query:
            query = update.callback_query
            await query.edit_message_text(
                "📭 У вас нет активных записей",
                reply_markup=reply_markup
            )
        else:
            await update.message.reply_text(
                "📭 У вас нет активных записей",
                reply_markup=reply_markup
            )
        return
    
    text = "📋 *Ваши записи:*\n\n"
    keyboard = []
    
    for appt in appointments:
        appt_id, service, date, time = appt
        selected_date_obj = datetime.strptime(date, "%Y-%m-%d").date()
        weekday = selected_date_obj.weekday()
        day_name = config.WEEKDAYS[weekday]
        display_date = selected_date_obj.strftime("%d.%m.%Y")
        text += f"🆔 #{appt_id}\n"
        text += f"💇 {service}\n"
        text += f"📅 {day_name} {display_date} ⏰ {time}\n"
        text += "─" * 20 + "\n"
        keyboard.append([InlineKeyboardButton(
            f"❌ Отменить #{appt_id}", 
            callback_data=f"cancel_{appt_id}"
        )])
    
    keyboard.append([InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        query = update.callback_query
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')

async def show_cancel_appointment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает записи для отмены"""
    user_id = update.effective_user.id
    
    if db.is_admin(user_id):
        all_appointments = db.get_all_appointments()
        appointments = [appt for appt in all_appointments if appt[1] == "Администратор" or str(appt[0]) == str(user_id)]
    else:
        appointments = db.get_user_appointments(user_id)
    
    if not appointments:
        keyboard = [[InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if update.callback_query:
            query = update.callback_query
            await query.edit_message_text(
                "📭 У вас нет записей для отмены",
                reply_markup=reply_markup
            )
        else:
            await update.message.reply_text(
                "📭 У вас нет записей для отмены",
                reply_markup=reply_markup
            )
        return
    
    text = "❌ *Отмена записи*\n\nВыберите запись для отмены:\n\n"
    keyboard = []
    
    for appt in appointments:
        if db.is_admin(user_id):
            appt_id, user_name, username, phone, service, date, time = appt
        else:
            appt_id, service, date, time = appt
            
        selected_date_obj = datetime.strptime(date, "%Y-%m-%d").date()
        weekday = selected_date_obj.weekday()
        day_name = config.WEEKDAYS[weekday]
        display_date = selected_date_obj.strftime("%d.%m.%Y")
        
        if db.is_admin(user_id):
            button_text = f"❌ #{appt_id} - {day_name} {display_date} {time}"
            callback_data = f"cancel_admin_{appt_id}"
        else:
            button_text = f"❌ #{appt_id} - {day_name} {display_date} {time}"
            callback_data = f"cancel_{appt_id}"
            
        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
    
    keyboard.append([InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        query = update.callback_query
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')

async def show_all_appointments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает все записи с телефонами (администратор)"""
    user_id = update.effective_user.id
    
    if not db.is_admin(user_id):
        await update.message.reply_text("❌ У вас нет доступа к этой функции")
        return
    
    appointments = db.get_all_appointments()
    
    if not appointments:
        keyboard = [[InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if update.callback_query:
            query = update.callback_query
            await query.edit_message_text("📭 Нет активных записей", reply_markup=reply_markup)
        else:
            await update.message.reply_text("📭 Нет активных записей", reply_markup=reply_markup)
        return
    
    text = f"👑 *Все записи {config.BARBERSHOP_NAME}:*\n\n"
    keyboard = []
    
    for appt in appointments:
        appt_id, user_name, username, phone, service, date, time = appt
        selected_date_obj = datetime.strptime(date, "%Y-%m-%d").date()
        weekday = selected_date_obj.weekday()
        day_name = config.WEEKDAYS[weekday]
        display_date = selected_date_obj.strftime("%d.%m.%Y")
        username_display = f"(@{username})" if username and username != "admin_manual" else ""
        manual_indicator = " 📝" if user_name == "Администратор" else ""
        text += f"🆔 #{appt_id}\n"
        text += f"👤 {user_name}{manual_indicator} {username_display}\n"
        text += f"📞 {phone}\n"
        text += f"💇 {service}\n"
        text += f"📅 {day_name} {display_date} ⏰ {time}\n"
        text += "─" * 20 + "\n"
        keyboard.append([InlineKeyboardButton(
            f"❌ Отменить #{appt_id}", 
            callback_data=f"cancel_admin_{appt_id}"
        )])
    
    keyboard.append([InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        query = update.callback_query
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')

async def show_today_appointments_visual(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает расписание на сегодня в новом визуальном формате"""
    try:
        user_id = update.effective_user.id
        
        if not db.is_admin(user_id):
            if update.callback_query:
                await update.callback_query.answer("❌ У вас нет доступа к этой функции", show_alert=True)
            else:
                await update.message.reply_text("❌ У вас нет доступа к этой функции")
            return
        
        appointments = db.get_today_appointments()
        
        moscow_time = get_moscow_time()
        today = moscow_time.date()
        today_str = today.strftime("%d.%m.%Y")
        current_time = moscow_time.time()
        
        weekday = today.weekday()
        work_schedule = db.get_work_schedule(weekday)
        
        if not work_schedule or not work_schedule[0][4]:
            keyboard = [[InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            if update.callback_query:
                query = update.callback_query
                await query.edit_message_text(
                    f"📅 {today_str} - выходной день",
                    reply_markup=reply_markup
                )
            else:
                await update.message.reply_text(
                    f"📅 {today_str} - выходной день",
                    reply_markup=reply_markup
                )
            return
        
        start_time = work_schedule[0][2]
        end_time = work_schedule[0][3]
        all_slots = db.generate_time_slots(start_time, end_time)
        
        booked_slots = {}
        for user_name, phone, service, time in appointments:
            if phone.startswith('+7'):
                formatted_phone = f"***{phone[-4:]}" if len(phone) >= 11 else phone
            elif phone.startswith('8'):
                formatted_phone = f"***{phone[-4:]}" if len(phone) >= 11 else phone
            else:
                formatted_phone = phone
            
            name_parts = user_name.split()
            if len(name_parts) >= 2:
                short_name = f"{name_parts[0]} {name_parts[1][0]}."
            else:
                short_name = user_name
            
            booked_slots[time] = {
                'name': short_name,
                'phone': formatted_phone,
                'full_name': user_name,
                'full_phone': phone,
                'service': service
            }
        
        header = f"📅 *{today_str}* | {len(appointments)}/{len(all_slots)} занято\n\n"
        
        schedule_text = ""
        total_booked = 0

        for slot in all_slots:
            slot_time = datetime.strptime(slot, "%H:%M").time()
            is_past = slot_time < current_time
            
            if slot in booked_slots:
                client_info = booked_slots[slot]
                safe_name = client_info['name'].replace('*', '\\*').replace('_', '\\_').replace('`', '\\`')
                safe_phone = client_info['phone'].replace('*', '\\*').replace('_', '\\_').replace('`', '\\`')
                
                if is_past:
                    schedule_text += f"⏰ *{slot}* ─── ⏳(Прошло)👤 {safe_name}\n"
                else:
                    schedule_text += f"⏰ *{slot}* ─── 👤 {safe_name}\n"
                total_booked += 1
            else:
                if is_past:
                    schedule_text += f"⏰ *{slot}* ─── ⏳ Прошло\n"
                else:
                    schedule_text += f"⏰ *{slot}* ─── ✅ Свободно\n"

        schedule_text += f"\n💡 Быстрые действия:\n"
        schedule_text += f"• Нажмите '🔄 Обновить' для актуального расписания\n"
        schedule_text += f"• Нажмите '📞 Все контакты' для просмотра всех номеров\n"
        schedule_text += f"• Для отмены записи используйте кнопку '❌ Отменить запись' в главном меню"
        
        full_text = header + schedule_text
        
        keyboard = [
            [InlineKeyboardButton("🔄 Обновить", callback_data="refresh_today")],
            [InlineKeyboardButton("📞 Все контакты", callback_data="all_contacts")],
            [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if update.callback_query:
            query = update.callback_query
            try:
                await query.edit_message_text(full_text, parse_mode='Markdown', reply_markup=reply_markup)
            except BadRequest as e:
                if "message is not modified" in str(e).lower():
                    logger.debug("Message not modified in show_today_appointments_visual - ignoring")
                else:
                    raise
        else:
            await update.message.reply_text(full_text, parse_mode='Markdown', reply_markup=reply_markup)
            
    except Exception as e:
        logger.error(f"❌ Ошибка в show_today_appointments_visual: {e}")
        if update.callback_query:
            await update.callback_query.edit_message_text("❌ Ошибка при загрузке расписания")
        else:
            await update.message.reply_text("❌ Ошибка при загрузке расписания")

async def show_week_appointments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает выбор дней недели для просмотра записей"""
    user_id = update.effective_user.id
    
    if not db.is_admin(user_id):
        if update.callback_query:
            await update.callback_query.answer("❌ У вас нет доступа к этой функции", show_alert=True)
        else:
            await update.message.reply_text("❌ У вас нет доступа к этой функции")
        return
    
    keyboard = []
    today = get_moscow_time().date()
    current_time = get_moscow_time().time()
    
    days_shown = 0
    i = 0
    
    while days_shown < 7 and i < 30:
        date = today + timedelta(days=i)
        date_str = date.strftime("%Y-%m-%d")
        display_date = date.strftime("%d.%m.%Y")
        weekday = date.weekday()
        day_name = config.WEEKDAYS[weekday]
        
        schedule = db.get_work_schedule(weekday)
        if schedule and schedule[0][4]:
            start_time, end_time = schedule[0][2], schedule[0][3]
            
            if is_date_available_for_view(date, current_time, start_time, end_time, i):
                appointments_count = get_appointments_count_for_date(date_str)
                total_slots = len(db.generate_time_slots(start_time, end_time))
                
                keyboard.append([InlineKeyboardButton(
                    f"📅 {day_name} {display_date} ({appointments_count}/{total_slots})", 
                    callback_data=f"week_day_{date_str}"
                )])
                days_shown += 1
        
        i += 1
    
    if not keyboard:
        keyboard = [[InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if update.callback_query:
            query = update.callback_query
            await query.edit_message_text("На этой неделе нет рабочих дней с записями 😔", reply_markup=reply_markup)
        else:
            await update.message.reply_text("На этой неделе нет рабочих дней с записями 😔", reply_markup=reply_markup)
        return
    
    keyboard.append([InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = "📅 *Записи на неделю*\n\nВыберите день для просмотра записей:"
    
    if update.callback_query:
        query = update.callback_query
        await query.edit_message_text(text, parse_mode='Markdown', reply_markup=reply_markup)
    else:
        await update.message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup)

async def show_day_appointments_visual(update: Update, context: ContextTypes.DEFAULT_TYPE, date_str: str):
    """Показывает расписание на выбранный день в визуальном формате"""
    try:
        user_id = update.effective_user.id
        
        if not db.is_admin(user_id):
            if update.callback_query:
                await update.callback_query.answer("❌ У вас нет доступа к этой функции", show_alert=True)
            else:
                await update.message.reply_text("❌ У вас нет доступа к этой функции")
            return
        
        all_appointments = db.get_all_appointments()
        day_appointments = [appt for appt in all_appointments if appt[5] == date_str]
        
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        weekday = date_obj.weekday()
        day_name = config.WEEKDAYS[weekday]
        display_date = date_obj.strftime("%d.%m.%Y")
        
        work_schedule = db.get_work_schedule(weekday)
        
        if not work_schedule or not work_schedule[0][4]:
            keyboard = [[InlineKeyboardButton("🔙 Назад к неделе", callback_data="week_appointments")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            if update.callback_query:
                query = update.callback_query
                await query.edit_message_text(
                    f"📅 {day_name} {display_date} - выходной день",
                    reply_markup=reply_markup
                )
            else:
                await update.message.reply_text(
                    f"📅 {day_name} {display_date} - выходной день",
                    reply_markup=reply_markup
                )
            return
        
        start_time = work_schedule[0][2]
        end_time = work_schedule[0][3]
        all_slots = db.generate_time_slots(start_time, end_time)
        
        booked_slots = {}
        for appt in day_appointments:
            appt_id, user_name, username, phone, service, date, time = appt
            
            if phone.startswith('+7'):
                formatted_phone = f"***{phone[-4:]}" if len(phone) >= 11 else phone
            elif phone.startswith('8'):
                formatted_phone = f"***{phone[-4:]}" if len(phone) >= 11 else phone
            else:
                formatted_phone = phone
            
            name_parts = user_name.split()
            if len(name_parts) >= 2:
                short_name = f"{name_parts[0]} {name_parts[1][0]}."
            else:
                short_name = user_name
            
            booked_slots[time] = {
                'name': short_name,
                'phone': formatted_phone,
                'full_name': user_name,
                'full_phone': phone,
                'service': service,
                'appt_id': appt_id
            }
        
        header = f"📅 *{day_name} {display_date}* | {len(day_appointments)}/{len(all_slots)} занято\n\n"
        
        schedule_text = ""
        total_booked = 0

        for slot in all_slots:
            if slot in booked_slots:
                client_info = booked_slots[slot]
                safe_name = client_info['name'].replace('*', '\\*').replace('_', '\\_').replace('`', '\\`')
                safe_phone = client_info['phone'].replace('*', '\\*').replace('_', '\\_').replace('`', '\\`')
                schedule_text += f"⏰ *{slot}* ─── 👤 {safe_name}\n"
                total_booked += 1
            else:
                schedule_text += f"⏰ *{slot}* ─── ✅ Свободно\n"

        schedule_text += f"\n💡 Быстрые действия:\n"
        schedule_text += f"• Нажмите '🔄 Обновить' для актуального расписания\n"
        schedule_text += f"• Нажмите '📞 Все контакты' для просмотра всех номеров\n"
        schedule_text += f"• Для отмена записи используйте кнопку '❌ Отменить запись' в главном меню"
        
        full_text = header + schedule_text
        
        keyboard = [
            [InlineKeyboardButton("🔄 Обновить", callback_data=f"refresh_day_{date_str}")],
            [InlineKeyboardButton("📞 Все контакты", callback_data=f"day_contacts_{date_str}")],
            [InlineKeyboardButton("🔙 Назад к неделе", callback_data="week_appointments")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if update.callback_query:
            query = update.callback_query
            try:
                await query.edit_message_text(full_text, parse_mode='Markdown', reply_markup=reply_markup)
            except BadRequest as e:
                if "message is not modified" in str(e).lower():
                    logger.debug("Message not modified in show_day_appointments_visual - ignoring")
                else:
                    raise
        else:
            await update.message.reply_text(full_text, parse_mode='Markdown', reply_markup=reply_markup)
            
    except Exception as e:
        logger.error(f"Ошибка в show_day_appointments_visual: {e}")
        if update.callback_query:
            await update.callback_query.edit_message_text("❌ Ошибка при загрузке расписания")
        else:
            await update.message.reply_text("❌ Ошибка при загрузке расписания")

async def show_day_contacts(update: Update, context: ContextTypes.DEFAULT_TYPE, date_str: str):
    """Показывает все контакты на выбранный день с полными номерами"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not db.is_admin(user_id):
        await query.answer("❌ У вас нет доступа к этой функции", show_alert=True)
        return
    
    all_appointments = db.get_all_appointments()
    day_appointments = [appt for appt in all_appointments if appt[5] == date_str]
    
    date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
    weekday = date_obj.weekday()
    day_name = config.WEEKDAYS[weekday]
    display_date = date_obj.strftime("%d.%m.%Y")
    
    if not day_appointments:
        text = f"📞 Контакты на {day_name} {display_date}\n\n📭 Нет записей на этот день"
    else:
        text = f"📞 Контакты на {day_name} {display_date}\n\n"
        
        for i, appt in enumerate(day_appointments, 1):
            appt_id, user_name, username, phone, service, date, time = appt
            text += f"{i}. ⏰ {time} - 👤 {user_name}\n"
            text += f"   📞 {phone}\n"
            text += f"   💇 {service}\n"
            text += f"   🆔 #{appt_id}\n"
            text += "   ──────────────────\n"
    
    keyboard = [
        [InlineKeyboardButton("📅 Назад к расписанию", callback_data=f"week_day_{date_str}")],
        [InlineKeyboardButton("🔙 Назад к неделе", callback_data="week_appointments")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        await query.edit_message_text(text, reply_markup=reply_markup)
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message not modified in show_day_contacts - ignoring")
        else:
            raise

def is_date_available_for_view(date, current_time, start_time, end_time, days_ahead):
    """Проверяет, можно ли показывать день для просмотра записей"""
    if days_ahead > 0:
        return True
    
    if days_ahead == 0:
        start_dt = datetime.strptime(start_time, "%H:%M").time()
        end_dt = datetime.strptime(end_time, "%H:%M").time()
        
        if current_time >= end_dt:
            return False
        
        return True
    
    return False

def get_appointments_count_for_date(date_str):
    """Получает количество записей на указанную дату"""
    try:
        all_appointments = db.get_all_appointments()
        count = 0
        for appt in all_appointments:
            if appt[5] == date_str:
                count += 1
        return count
    except:
        return 0

async def show_all_contacts_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает все контакты на сегодня с полными номерами"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not db.is_admin(user_id):
        await query.answer("❌ У вас нет доступа к этой функции", show_alert=True)
        return
    
    appointments = db.get_today_appointments()
    today = get_moscow_time().date()
    today_str = today.strftime("%d.%m.%Y")
    
    if not appointments:
        text = f"📞 Контакты на {today_str}\n\n📭 Нет записей на сегодня"
    else:
        text = f"📞 Контакты на {today_str}\n\n"
        
        for i, (user_name, phone, service, time) in enumerate(appointments, 1):
            text += f"{i}. ⏰ {time} - 👤 {user_name}\n"
            text += f"   📞 {phone}\n"
            text += f"   💇 {service}\n"
            text += "   ──────────────────\n"
    
    keyboard = [
        [InlineKeyboardButton("📅 Назад к расписанию", callback_data="show_today_visual")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        await query.edit_message_text(text, reply_markup=reply_markup)
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message not modified in show_all_contacts_today - ignoring")
        else:
            raise

async def handle_schedule_actions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик действий с расписанием"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not db.is_admin(user_id):
        await query.answer("❌ У вас нет доступа к этой функции", show_alert=True)
        return
    
    action_data = query.data
    
    if action_data.startswith("call_"):
        slot_time = action_data[5:]
        await show_phone_number(update, context, slot_time)
    
    elif action_data.startswith("edit_"):
        slot_time = action_data[5:]
        await edit_appointment(update, context, slot_time)
    
    elif action_data.startswith("cancel_slot_"):
        slot_time = action_data[12:]
        await cancel_slot_appointment(update, context, slot_time)
    
    elif action_data == "refresh_today":
        await show_today_appointments_visual(update, context)
    
    elif action_data == "all_contacts":
        await show_all_contacts_today(update, context)
    
    elif action_data == "show_today_visual":
        await show_today_appointments_visual(update, context)

async def show_phone_number(update: Update, context: ContextTypes.DEFAULT_TYPE, slot_time: str):
    """Показывает полный номер телефона для звонка"""
    query = update.callback_query
    today = get_moscow_time().date().strftime("%Y-%m-%d")
    
    appointments = db.get_today_appointments()
    target_appointment = None
    
    for user_name, phone, service, time in appointments:
        if time == slot_time:
            target_appointment = (user_name, phone, service, time)
            break
    
    if not target_appointment:
        await query.answer("❌ Запись не найдена", show_alert=True)
        return
    
    user_name, phone, service, time = target_appointment
    
    text = (
        f"📞 ЗВОНОК КЛИЕНТУ\n\n"
        f"👤 Имя: {user_name}\n"
        f"📞 Телефон: {phone}\n"
        f"💇 Услуга: {service}\n"
        f"⏰ Время: {slot_time}\n\n"
        f"Нажмите на номер, чтобы скопировать: `{phone}`"
    )
    
    keyboard = [
        [InlineKeyboardButton("✅ Позвонил", callback_data=f"called_{slot_time}")],
        [InlineKeyboardButton("📅 Назад к расписанию", callback_data="show_today_visual")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        await query.edit_message_text(text, parse_mode='Markdown', reply_markup=reply_markup)
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message not modified in show_phone_number - ignoring")
        else:
            raise

async def cancel_slot_appointment(update: Update, context: ContextTypes.DEFAULT_TYPE, slot_time: str):
    """Отмена записи через расписание"""
    query = update.callback_query
    today = get_moscow_time().date().strftime("%Y-%m-%d")
    
    appointments = db.get_today_appointments()
    target_appointment = None
    
    for user_name, phone, service, time in appointments:
        if time == slot_time:
            target_appointment = (user_name, phone, service, time)
            break
    
    if not target_appointment:
        await query.answer("❌ Запись не найдена", show_alert=True)
        return
    
    user_name, phone, service, time = target_appointment
    
    context.user_data['cancel_slot_data'] = {
        'slot_time': slot_time,
        'user_name': user_name,
        'phone': phone,
        'service': service,
        'date': today
    }
    
    text = (
        f"❌ ОТМЕНА ЗАПИСИ\n\n"
        f"👤 Имя: {user_name}\n"
        f"📞 Телефон: {phone}\n"
        f"💇 Услуга: {service}\n"
        f"⏰ Время: {slot_time}\n\n"
        f"Вы уверены, что хотите отменить эту запись?"
    )
    
    keyboard = [
        [InlineKeyboardButton("✅ Да, отменить", callback_data="confirm_cancel_slot")],
        [InlineKeyboardButton("❌ Нет, вернуться", callback_data="show_today_visual")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        await query.edit_message_text(text, reply_markup=reply_markup)
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message not modified in cancel_slot_appointment - ignoring")
        else:
            raise

async def confirm_cancel_slot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение отмены записи через расписание"""
    query = update.callback_query
    
    if 'cancel_slot_data' not in context.user_data:
        await query.answer("❌ Данные устарели", show_alert=True)
        return
    
    cancel_data = context.user_data['cancel_slot_data']
    
    appointments = db.get_all_appointments()
    appointment_id = None
    
    for appt in appointments:
        appt_id, user_name, username, phone, service, date, time = appt
        if (date == cancel_data['date'] and time == cancel_data['slot_time'] and 
            user_name == cancel_data['user_name']):
            appointment_id = appt_id
            break
    
    if not appointment_id:
        await query.answer("❌ Запись не найдена", show_alert=True)
        return
    
    appointment = db.cancel_appointment(appointment_id)
    if appointment:
        await notify_client_about_cancellation(context, appointment)
        await notify_admin_about_cancellation(context, appointment, query.from_user.id, is_admin=True)
        
        text = f"✅ Запись на {cancel_data['slot_time']} отменена"
    else:
        text = "❌ Ошибка при отмене записи"
    
    context.user_data.pop('cancel_slot_data', None)
    
    keyboard = [
        [InlineKeyboardButton("📅 Обновить расписание", callback_data="show_today_visual")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        await query.edit_message_text(text, reply_markup=reply_markup)
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message not modified in confirm_cancel_slot - ignoring")
        else:
            raise

async def edit_appointment(update: Update, context: ContextTypes.DEFAULT_TYPE, slot_time: str):
    """Редактирование записи через расписание"""
    query = update.callback_query
    
    text = (
        f"✏️ РЕДАКТИРОВАНИЕ ЗАПИСИ\n\n"
        f"Функция редактирования записи на {slot_time} в разработке.\n\n"
        f"Сейчас вы можете:\n"
        f"• Отменить запись и создать новую\n"
        f"• Связаться с клиентом для переноса"
    )
    
    keyboard = [
        [InlineKeyboardButton("❌ Отменить запись", callback_data=f"cancel_slot_{slot_time}")],
        [InlineKeyboardButton("📞 Позвонить клиенту", callback_data=f"call_{slot_time}")],
        [InlineKeyboardButton("📅 Назад к расписанию", callback_data="show_today_visual")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        await query.edit_message_text(text, reply_markup=reply_markup)
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message not modified in edit_appointment - ignoring")
        else:
            raise

async def called_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение звонка клиенту"""
    query = update.callback_query
    slot_time = query.data[7:]
    
    text = f"✅ Отмечено: звонок клиенту на {slot_time} выполнен"
    
    keyboard = [
        [InlineKeyboardButton("📅 Назад к расписанию", callback_data="show_today_visual")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        await query.edit_message_text(text, reply_markup=reply_markup)
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message not modified in called_confirmation - ignoring")
        else:
            raise

async def manage_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Управление графиком работы"""
    user_id = update.effective_user.id
    
    if not db.is_admin(user_id):
        await update.message.reply_text("❌ У вас нет доступа к этой функции")
        return
    
    schedule = db.get_week_schedule()
    
    text = "🗓️ *График работы*\n\n"
    
    for weekday in range(7):
        day_data = schedule[weekday]
        day_name = config.WEEKDAYS[weekday]
        if day_data[4]:
            text += f"✅ {day_name}: {day_data[2]} - {day_data[3]}\n"
        else:
            text += f"❌ {day_name}: выходной\n"
    
    keyboard = []
    for weekday in range(7):
        day_name = config.WEEKDAYS[weekday]
        keyboard.append([InlineKeyboardButton(
            f"📅 {day_name}", 
            callback_data=f"schedule_day_{weekday}"
        )])
    
    keyboard.append([InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        query = update.callback_query
        await query.edit_message_text(text, parse_mode='Markdown', reply_markup=reply_markup)
    else:
        await update.message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup)

async def manage_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Управление администраторами"""
    user_id = update.effective_user.id
    
    if not db.is_admin(user_id):
        if update.callback_query:
            await update.callback_query.answer("❌ У вас нет доступа к этой функции", show_alert=True)
        else:
            await update.message.reply_text("❌ У вас нет доступа к этой функции")
        return
    
    keyboard = [
        [InlineKeyboardButton("📋 Список администраторов", callback_data="admin_list")],
        [InlineKeyboardButton("➕ Добавить администратора", callback_data="admin_add")],
        [InlineKeyboardButton("➖ Удалить администратора", callback_data="admin_remove")],
        # 🎯 ДОБАВЛЯЕМ КНОПКУ BACKUP
        [InlineKeyboardButton("💾 Управление backup", callback_data="backup_status")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        query = update.callback_query
        await query.edit_message_text(
            "👥 *Управление администраторами*\n\nВыберите действие:",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    else:
        await update.message.reply_text(
            "👥 *Управление администраторами*\n\nВыберите действие:",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )

async def show_admin_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список администраторов"""
    query = update.callback_query
    user_id = query.from_user.id
    
    logger.info(f"🔄 show_admin_list вызван для пользователя {user_id}")
    
    if not db.is_admin(user_id):
        logger.warning(f"❌ Пользователь {user_id} не администратор")
        await query.answer("❌ У вас нет доступа к этой функции", show_alert=True)
        return
    
    admins = db.get_all_admins()
    logger.info(f"📊 Найдено администраторов в БД: {len(admins)}")
    
    if not admins:
        text = "📭 Список администраторов пуст"
    else:
        text = "👑 *Список администраторов:*\n\n"
        protected_count = 0
        
        for admin in admins:
            admin_id, username, first_name, last_name, added_at, added_by = admin
            display_name = f"{first_name} {last_name}".strip()
            if username and username != 'system':
                display_name += f" (@{username})"
            
            added_date = added_at.strftime("%d.%m.%Y") if isinstance(added_at, datetime) else added_at
            
            protection_indicator = " 🔒" if admin_id in config.PROTECTED_ADMINS else ""
            
            text += f"🆔 *ID:* {admin_id}{protection_indicator}\n"
            text += f"👤 *Имя:* {display_name}\n"
            text += f"📅 *Добавлен:* {added_date}\n"
            text += "─" * 20 + "\n"
            
            if admin_id in config.PROTECTED_ADMINS:
                protected_count += 1
        
        if protected_count > 0:
            text += f"\n🔒 *{protected_count} защищенный(ых) администратор(ов)*"
        
        logger.info(f"📋 Сформирован список из {len(admins)} администраторов")
    
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="manage_admins")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        await query.edit_message_text(
            text,
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
        logger.info(f"✅ Список администраторов отправлен пользователю {user_id}")
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message not modified in show_admin_list - ignoring")
        else:
            logger.error(f"❌ Ошибка при отправке списка администраторов: {e}")
            raise

async def add_admin_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало процесса добавления администратора"""
    query = update.callback_query
    user_id = query.from_user.id
    
    logger.info(f"🔄 add_admin_start вызван пользователем {user_id}")
    
    if not db.is_admin(user_id):
        logger.warning(f"❌ Пользователь {user_id} не администратор")
        await query.answer("❌ У вас нет доступа к этой функции", show_alert=True)
        return
    
    context.user_data['awaiting_admin_id'] = True
    logger.info(f"✅ awaiting_admin_id установлен в True для пользователя {user_id}")
    
    keyboard = [
        [InlineKeyboardButton("🔙 Назад", callback_data="manage_admins")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        await query.edit_message_text(
            "➕ *Добавление администратора*\n\n"
            "Введите ID пользователя:\n\n"
            "*Как получить ID пользователя?*\n"
            "1. Попросите пользователя написать боту @userinfobot\n"
            "2. Или перешлите любое сообщение от пользователя боту @userinfobot\n"
            "3. Бот покажет ID пользователя\n\n"
            "*Введите числовой ID:*",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
        logger.info(f"✅ Сообщение для ввода ID отправлено пользователю {user_id}")
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message not modified in add_admin_start - ignoring")
        else:
            logger.error(f"❌ Ошибка при отправке сообщения: {e}")
            raise

async def remove_admin_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало процесса удаления администратора"""
    query = update.callback_query
    user_id = query.from_user.id
    
    logger.info(f"🔄 remove_admin_start вызван пользователем {user_id}")
    
    if not db.is_admin(user_id):
        logger.warning(f"❌ Пользователь {user_id} не администратор")
        await query.answer("❌ У вас нет доступа к этой функции", show_alert=True)
        return
    
    admins = db.get_all_admins()
    logger.info(f"📊 Найдено администраторов: {len(admins)}")
    
    if len(admins) <= 1:
        logger.warning(f"❌ Попытка удалить последнего администратора")
        await query.answer("❌ Нельзя удалить последнего администратора", show_alert=True)
        return
    
    keyboard = []
    protected_count = 0
    
    for admin in admins:
        admin_id, username, first_name, last_name, added_at, added_by = admin
        
        try:
            if hasattr(config, 'PROTECTED_ADMINS') and admin_id in config.PROTECTED_ADMINS:
                protected_count += 1
                continue
        except AttributeError:
            pass
            
        display_name = f"{first_name} {last_name}".strip()
        if username and username != 'system':
            display_name += f" (@{username})"
        
        keyboard.append([InlineKeyboardButton(
            f"➖ {display_name} (ID: {admin_id})",
            callback_data=f"admin_remove_confirm_{admin_id}"
        )])
        logger.info(f"📋 Добавлен администратор в список: {display_name} (ID: {admin_id})")
    
    if not keyboard:
        await query.answer("❌ Нет администраторов для удаления", show_alert=True)
        return
    
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="manage_admins")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        text = "➖ *Удаление администратора*\n\nВыберите администратора для удаления:"
        if protected_count > 0:
            text += f"\n\n*Примечание:* {protected_count} защищенный(ых) администратор(ов) скрыты"
            
        await query.edit_message_text(
            text,
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
        logger.info(f"✅ Список администраторов для удаления отправлен пользователю {user_id}")
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message not modified in remove_admin_start - ignoring")
        else:
            logger.error(f"❌ Ошибка при отправке сообщения: {e}")
            raise

async def remove_admin_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, admin_id: int):
    """Подтверждение удаления администратора"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not db.is_admin(user_id):
        await query.answer("❌ У вас нет доступа к этой функции", show_alert=True)
        return
    
    admin_info = db.get_admin_info(admin_id)
    if not admin_info:
        await query.answer("❌ Администратор не найден", show_alert=True)
        return
    
    admin_id, username, first_name, last_name, added_at, added_by = admin_info
    display_name = f"{first_name} {last_name}".strip()
    if username and username != 'system':
        display_name += f" (@{username})"
    
    keyboard = [
        [InlineKeyboardButton("✅ Да, удалить", callback_data=f"admin_remove_final_{admin_id}")],
        [InlineKeyboardButton("❌ Нет, отмена", callback_data="manage_admins")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        await query.edit_message_text(
            f"⚠️ *Подтверждение удаления*\n\n"
            f"Вы действительно хотите удалить администратора?\n\n"
            f"👤 *Имя:* {display_name}\n"
            f"🆔 *ID:* {admin_id}\n\n"
            f"*Внимание:* После удаления пользователь потеряет доступ к админ-панели.",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message not modified in remove_admin_confirm - ignoring")
        else:
            raise

async def remove_admin_final(update: Update, context: ContextTypes.DEFAULT_TYPE, admin_id: int):
    """Финальное удаление администратора С BACKUP"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not db.is_admin(user_id):
        await query.answer("❌ У вас нет доступа к этой функции", show_alert=True)
        return
    
    if admin_id in config.PROTECTED_ADMINS:
        logger.warning(f"🚫 Попытка удалить защищенного администратора {admin_id}")
        await query.answer("❌ Нельзя удалить защищенного администратора", show_alert=True)
        return
    
    if admin_id == user_id:
        await query.answer("❌ Нельзя удалить самого себя", show_alert=True)
        return
    
    deleted = db.remove_admin(admin_id)
    
    if deleted:
        text = f"✅ Администратор с ID {admin_id} удален"
        logger.info(f"✅ Администратор {admin_id} удален пользователем {user_id}")
        
        # 🎯 СОЗДАЕМ BACKUP ПОСЛЕ ИЗМЕНЕНИЯ АДМИНИСТРАТОРОВ
        backup_path = db.create_backup()
        if backup_path:
            logger.info(f"💾 Backup создан после удаления администратора {admin_id}")
    else:
        text = "❌ Администратор не найден"
    
    keyboard = [[InlineKeyboardButton("🔙 Назад к управлению", callback_data="manage_admins")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        await query.edit_message_text(text, reply_markup=reply_markup)
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message not modified in remove_admin_final - ignoring")
        else:
            raise

async def handle_admin_id_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ввода ID администратора С BACKUP"""
    user_id = update.effective_user.id
    text = update.message.text.strip()
    
    logger.info(f"🔄 handle_admin_id_input ВЫЗВАН для пользователя {user_id}")
    logger.info(f"📥 Текст сообщения: '{text}'")
    logger.info(f"🔍 awaiting_admin_id: {context.user_data.get('awaiting_admin_id', 'NOT SET')}")
    
    if not context.user_data.get('awaiting_admin_id'):
        logger.info("❌ awaiting_admin_id = False, пропускаем обработку")
        await handle_message(update, context)
        return
    
    context.user_data['awaiting_admin_id'] = False
    logger.info(f"📥 Получен ID для добавления администратора: '{text}' от пользователя {user_id}")
    
    try:
        new_admin_id = int(text)
        logger.info(f"🔢 Преобразован ID: {new_admin_id}")
        
        if db.is_admin(new_admin_id):
            logger.warning(f"⚠️ Пользователь {new_admin_id} уже администратор")
            await update.message.reply_text(
                "❌ Этот пользователь уже является администратором",
                reply_markup=get_main_keyboard(user_id)
            )
            return
        
        username = "unknown"
        first_name = "Пользователь"
        last_name = f"ID {new_admin_id}"
        
        try:
            logger.info(f"🔍 Получаем информацию о пользователе {new_admin_id}")
            chat_member = await context.bot.get_chat_member(new_admin_id, new_admin_id)
            username = chat_member.user.username or "unknown"
            first_name = chat_member.user.first_name or "Пользователь"
            last_name = chat_member.user.last_name or f"ID {new_admin_id}"
            logger.info(f"✅ Информация получена: {first_name} {last_name} (@{username})")
        except Exception as e:
            logger.warning(f"⚠️ Не удалось получить информацию о пользователе {new_admin_id}: {e}")
            logger.info("ℹ️ Используем значения по умолчанию для имени пользователя")
        
        logger.info(f"➕ Добавляем администратора {new_admin_id} в БД")
        
        success = db.add_admin(new_admin_id, username, first_name, last_name, user_id)
        
        if success:
            display_name = f"{first_name} {last_name}".strip()
            if username and username != 'unknown':
                display_name += f" (@{username})"
            
            logger.info(f"✅ Администратор {new_admin_id} успешно добавлен")
            
            # 🎯 СОЗДАЕМ BACKUP ПОСЛЕ ДОБАВЛЕНИЯ АДМИНИСТРАТОРА
            backup_path = db.create_backup()
            if backup_path:
                logger.info(f"💾 Backup создан после добавления администратора {new_admin_id}")
            
            await update.message.reply_text(
                f"✅ *Новый администратор добавлен!*\n\n"
                f"👤 *Имя:* {display_name}\n"
                f"🆔 *ID:* {new_admin_id}\n\n"
                f"Пользователь получил доступ к админ-панели.\n\n"
                f"*Примечание:* Пользователь должен начать диалог с ботом (@{context.bot.username}), чтобы бот мог отправлять ему уведомления.",
                parse_mode='Markdown',
                reply_markup=get_main_keyboard(user_id)
            )
        else:
            logger.error(f"❌ Ошибка при добавлении администратора {new_admin_id} в БД")
            await update.message.reply_text(
                "❌ Ошибка при добавлении администратора в базу данных. Попробуйте еще раз.",
                reply_markup=get_main_keyboard(user_id)
            )
        
    except ValueError:
        logger.error(f"❌ Неверный формат ID: '{text}'")
        await update.message.reply_text(
            "❌ Неверный формат ID. Введите числовой ID пользователя:",
            reply_markup=get_main_keyboard(user_id)
        )
    except Exception as e:
        logger.error(f"❌ Общая ошибка при добавлении администратора: {e}")
        await update.message.reply_text(
            "❌ Ошибка при добавлении администратора. Проверьте правильность ID и попробуйте еще раз.",
            reply_markup=get_main_keyboard(user_id)
        )

async def schedule_day_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора дня недели для настройки графика"""
    query = update.callback_query
    weekday = int(query.data.split("_")[2])
    context.user_data['schedule_weekday'] = weekday
    
    current_schedule = db.get_work_schedule(weekday)
    day_name = config.WEEKDAYS[weekday]
    
    keyboard = [
        [InlineKeyboardButton("✅ Рабочий день", callback_data=f"schedule_working_{weekday}")],
        [InlineKeyboardButton("❌ Выходной", callback_data=f"schedule_off_{weekday}")],
        [InlineKeyboardButton("🔙 Назад", callback_data="manage_schedule")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if current_schedule and len(current_schedule) > 0:
        schedule_data = current_schedule[0]
        start_time, end_time, is_working = schedule_data[2], schedule_data[3], schedule_data[4]
        status = "рабочий" if is_working else "выходной"
        current_info = f"\n\n*Текущие настройки:* {status}"
        if is_working:
            current_info += f" ({start_time} - {end_time})"
    else:
        current_info = "\n\n*Настройки не заданы*"
    
    try:
        await query.edit_message_text(
            f"📅 Настройка графика для *{day_name}*{current_info}\n\nВыберите тип дня:",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message not modified in schedule_day_selected - ignoring")
        else:
            raise

async def schedule_working_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора рабочего дня"""
    query = update.callback_query
    weekday = int(query.data.split("_")[2])
    context.user_data['schedule_weekday'] = weekday
    day_name = config.WEEKDAYS[weekday]
    
    keyboard = []
    times = [f"{hour:02d}:00" for hour in range(8, 18)]
    
    row = []
    for i, time in enumerate(times):
        row.append(InlineKeyboardButton(time, callback_data=f"schedule_start_{time}"))
        if (i + 1) % 3 == 0 or i == len(times) - 1:
            keyboard.append(row)
            row = []
    
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"schedule_day_{weekday}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        await query.edit_message_text(
            f"⏰ Выберите время *начала* работы для {day_name}:",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message not modified in schedule_working_selected - ignoring")
        else:
            raise

async def schedule_start_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора времени начала работы"""
    query = update.callback_query
    start_time = query.data.split("_")[2]
    context.user_data['schedule_start'] = start_time
    weekday = context.user_data['schedule_weekday']
    day_name = config.WEEKDAYS[weekday]
    
    keyboard = []
    start_hour = int(start_time.split(":")[0])
    times = [f"{hour:02d}:00" for hour in range(start_hour + 1, 21)]
    
    row = []
    for i, time in enumerate(times):
        row.append(InlineKeyboardButton(time, callback_data=f"schedule_end_{time}"))
        if (i + 1) % 3 == 0 or i == len(times) - 1:
            keyboard.append(row)
            row = []
    
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"schedule_working_{weekday}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        await query.edit_message_text(
            f"⏰ Выберите время *окончания* работы для {day_name}:\n*Начало:* {start_time}",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message not modified in schedule_start_selected - ignoring")
        else:
            raise

async def schedule_end_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора времени окончания работы с проверкой конфликтов"""
    query = update.callback_query
    end_time = query.data.split("_")[2]
    start_time = context.user_data['schedule_start']
    weekday = context.user_data['schedule_weekday']
    day_name = config.WEEKDAYS[weekday]
    
    conflicting_appointments = db.get_conflicting_appointments(weekday, start_time, end_time, True)
    
    if conflicting_appointments:
        context.user_data['pending_schedule'] = {
            'weekday': weekday,
            'start_time': start_time,
            'end_time': end_time,
            'is_working': True
        }
        context.user_data['conflicting_appointments'] = conflicting_appointments
        
        await show_schedule_conflict_warning(update, context, conflicting_appointments, day_name)
        return
    
    db.set_work_schedule(weekday, start_time, end_time, True)
    
    # 🎯 СОЗДАЕМ BACKUP ПОСЛЕ ИЗМЕНЕНИЯ ГРАФИКА РАБОТЫ
    backup_path = db.create_backup()
    if backup_path:
        logger.info(f"💾 Backup создан после установки графика для {day_name}")
    
    keyboard = [[InlineKeyboardButton("🔙 Назад к графику", callback_data="manage_schedule")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        await query.edit_message_text(
            f"✅ График для *{day_name}* обновлен!\n🕐 *Время работы:* {start_time} - {end_time}",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message not modified in schedule_end_selected - ignoring")
        else:
            raise

async def schedule_off_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора выходного дня с проверкой конфликтов"""
    query = update.callback_query
    weekday = int(query.data.split("_")[2])
    day_name = config.WEEKDAYS[weekday]
    
    conflicting_appointments = db.get_conflicting_appointments(weekday, "10:00", "20:00", False)
    
    if conflicting_appointments:
        context.user_data['pending_schedule'] = {
            'weekday': weekday,
            'start_time': "10:00",
            'end_time': "20:00", 
            'is_working': False
        }
        context.user_data['conflicting_appointments'] = conflicting_appointments
        
        await show_schedule_conflict_warning(update, context, conflicting_appointments, day_name)
        return
    
    db.set_work_schedule(weekday, "10:00", "20:00", False)
    
    # 🎯 СОЗДАЕМ BACKUP ПОСЛЕ ИЗМЕНЕНИЯ ГРАФИКА РАБОТЫ
    backup_path = db.create_backup()
    if backup_path:
        logger.info(f"💾 Backup создан после установки выходного для {day_name}")
    
    keyboard = [[InlineKeyboardButton("🔙 Назад к графику", callback_data="manage_schedule")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        await query.edit_message_text(
            f"✅ *{day_name}* установлен как выходной день", 
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message not modified in schedule_off_selected - ignoring")
        else:
            raise

async def show_schedule_conflict_warning(update: Update, context: ContextTypes.DEFAULT_TYPE, conflicting_appointments, day_name):
    """Показывает предупреждение о конфликтующих записях"""
    query = update.callback_query
    
    appointments_by_date = {}
    for appt in conflicting_appointments:
        appt_id, user_id, user_name, phone, service, date, time = appt
        if date not in appointments_by_date:
            appointments_by_date[date] = []
        appointments_by_date[date].append((time, user_name, service, appt_id))
    
    text = f"⚠️ *ВНИМАНИЕ: Обнаружены конфликтующие записи!*\n\n"
    text += f"📅 *День недели:* {day_name}\n"
    text += f"👥 *Количество записей:* {len(conflicting_appointments)}\n\n"
    
    for date, appointments in appointments_by_date.items():
        selected_date_obj = datetime.strptime(date, "%Y-%m-%d").date()
        weekday = selected_date_obj.weekday()
        date_day_name = config.WEEKDAYS[weekday]
        display_date = selected_date_obj.strftime("%d.%m.%Y")
        text += f"*{date_day_name} {display_date}:*\n"
        for time, user_name, service, appt_id in appointments:
            text += f"• {time} - {user_name} ({service}) #{appt_id}\n"
        text += "\n"
    
    text += "*Выберите действие:*"
    
    keyboard = [
        [InlineKeyboardButton("✅ Отменить конфликтующие записи", callback_data="schedule_cancel_appointments")],
        [InlineKeyboardButton("❌ Отменить изменение графика", callback_data="schedule_cancel_changes")],
        [InlineKeyboardButton("🔙 Назад к графику", callback_data="manage_schedule")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        await query.edit_message_text(
            text,
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message not modified in show_schedule_conflict_warning - ignoring")
        else:
            raise

async def handle_schedule_cancel_appointments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик отмены конфликтующих записей С BACKUP"""
    query = update.callback_query
    
    if 'pending_schedule' not in context.user_data or 'conflicting_appointments' not in context.user_data:
        await query.answer("❌ Данные о конфликтах устарели", show_alert=True)
        return
    
    pending_schedule = context.user_data['pending_schedule']
    conflicting_appointments = context.user_data['conflicting_appointments']
    
    appointment_ids = [appt[0] for appt in conflicting_appointments]
    
    canceled_appointments = db.cancel_appointments_by_ids(appointment_ids)
    
    db.set_work_schedule(
        pending_schedule['weekday'],
        pending_schedule['start_time'],
        pending_schedule['end_time'],
        pending_schedule['is_working']
    )
    
    await notify_clients_about_schedule_change(context, canceled_appointments, pending_schedule)
    
    context.user_data.pop('pending_schedule', None)
    context.user_data.pop('conflicting_appointments', None)
    
    day_name = config.WEEKDAYS[pending_schedule['weekday']]
    
    if pending_schedule['is_working']:
        schedule_info = f"{pending_schedule['start_time']} - {pending_schedule['end_time']}"
    else:
        schedule_info = "выходной"
    
    # 🎯 СОЗДАЕМ BACKUP ПОСЛЕ ИЗМЕНЕНИЯ ГРАФИКА РАБОТЫ
    backup_path = db.create_backup()
    if backup_path:
        logger.info(f"💾 Backup создан после изменения графика работы для {day_name}")
    
    keyboard = [[InlineKeyboardButton("🔙 Назад к графику", callback_data="manage_schedule")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        await query.edit_message_text(
            f"✅ *График обновлен!*\n\n"
            f"📅 *{day_name}:* {schedule_info}\n"
            f"❌ *Отменено записей:* {len(canceled_appointments)}\n\n"
            f"Клиенты получили уведомления об отмене.",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message not modified in handle_schedule_cancel_appointments - ignoring")
        else:
            raise

async def handle_schedule_cancel_changes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик отмены изменений графика"""
    query = update.callback_query
    
    context.user_data.pop('pending_schedule', None)
    context.user_data.pop('conflicting_appointments', None)
    
    keyboard = [[InlineKeyboardButton("🔙 Назад к графику", callback_data="manage_schedule")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        await query.edit_message_text(
            "❌ *Изменения графика отменены*\n\n"
            "Расписание осталось без изменений.",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message not modified in handle_schedule_cancel_changes - ignoring")
        else:
            raise

async def notify_clients_about_schedule_change(context: ContextTypes.DEFAULT_TYPE, canceled_appointments, new_schedule):
    """Уведомляет клиентов об отмене записей из-за изменения графика"""
    day_name = config.WEEKDAYS[new_schedule['weekday']]
    
    if new_schedule['is_working']:
        reason = f"изменение графика работы ({new_schedule['start_time']} - {new_schedule['end_time']})"
    else:
        reason = "выходной день"
    
    for appointment in canceled_appointments:
        user_id, user_name, phone, service, date, time = appointment
        
        if user_id == 0 or user_id is None or user_name == "Администратор":
            logger.info(f"Пропуск уведомления для ручной записи администратора: user_id={user_id}")
            continue
            
        selected_date_obj = datetime.strptime(date, "%Y-%m-%d").date()
        weekday = selected_date_obj.weekday()
        date_day_name = config.WEEKDAYS[weekday]
        display_date = selected_date_obj.strftime("%d.%m.%Y")
        
        text = (
            f"❌ *Запись отменена*\n\n"
            f"💇 {service}\n"
            f"📅 {date_day_name} {display_date}\n"
            f"⏰ {time}\n\n"
            f"*Причина:* {reason}\n\n"
            f"Запишитесь на другое время."
        )
        
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=text,
                parse_mode='Markdown'
            )
            logger.info(f"Уведомление об отмене отправлено клиенту {user_id}")
        except BadRequest as e:
            if "chat not found" in str(e).lower():
                logger.warning(f"Chat not found for user {user_id}, skipping notification")
            else:
                logger.error(f"BadRequest при отправке уведомления клиенту {user_id}: {e}")
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления клиенту {user_id}: {e}")

async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для остановки бота (только для администраторов)"""
    user_id = update.effective_user.id
    if not db.is_admin(user_id):
        await update.message.reply_text("❌ У вас нет прав для этой команды")
        return
    
    await update.message.reply_text("🛑 Останавливаю бота...")
    logger.info("🛑 Bot остановлен по команде администратора")
    os._exit(0)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик inline кнопок"""
    query = update.callback_query
    
    try:
        await query.answer()
    except Exception as e:
        logger.warning(f"Ошибка при ответе на callback query: {e}")
    
    logger.info(f"🔄 button_handler: {query.data} от пользователя {query.from_user.id}")
    
    # 🎯 BACKUP ОБРАБОТЧИКИ
    if query.data == "backup_status":
        await show_backup_status(update, context)
    elif query.data == "create_backup_now":
        await create_backup_now(update, context)
    elif query.data == "db_stats":
        await show_db_stats(update, context)
    
    # ОСНОВНЫЕ ОБРАБОТЧИКИ
    elif query.data == "main_menu":
        await show_main_menu(update, context)
    elif query.data == "make_appointment":
        user_id = query.from_user.id
        is_admin = db.is_admin(user_id)
        await make_appointment_start(update, context, is_admin=is_admin)
    
    # АДМИН-ПАНЕЛЬ
    elif query.data == "manage_admins":
        await manage_admins(update, context)
    elif query.data == "admin_list":
        await show_admin_list(update, context)
    elif query.data == "admin_add":
        await add_admin_start(update, context)
    elif query.data == "admin_remove":
        await remove_admin_start(update, context)
    
    elif query.data.startswith("admin_remove_confirm_"):
        try:
            admin_id = int(query.data.split("_")[3])
            await remove_admin_confirm(update, context, admin_id)
        except (ValueError, IndexError):
            await query.answer("❌ Ошибка при обработке запроса", show_alert=True)
    
    elif query.data.startswith("admin_remove_final_"):
        try:
            admin_id = int(query.data.split("_")[3])
            await remove_admin_final(update, context, admin_id)
        except (ValueError, IndexError):
            await query.answer("❌ Ошибка при обработке запроса", show_alert=True)
    
    # ЗАПИСИ И РАСПИСАНИЕ
    elif query.data.startswith("service_"):
        await service_selected(update, context)
    elif query.data.startswith("date_"):
        await date_selected(update, context)
    elif query.data.startswith("time_"):
        await time_selected(update, context)
    elif query.data.startswith("cancel_"):
        if query.data.startswith("cancel_admin_"):
            try:
                appointment_id = int(query.data.split("_")[2])
                await cancel_appointment(update, context, appointment_id)
            except (ValueError, IndexError):
                await query.edit_message_text("❌ Ошибка: неверный ID записи")
        else:
            try:
                appointment_id = int(query.data.split("_")[1])
                await cancel_appointment(update, context, appointment_id)
            except (ValueError, IndexError):
                await query.edit_message_text("❌ Ошибка: неверный ID записи")
    
    # СТАТИСТИКА И ОТЧЕТЫ
    elif query.data == "weekly_report":
        await weekly_report(update, context)
    elif query.data == "show_statistics":
        await show_statistics(update, context)
    
    # ГРАФИК РАБОТЫ
    elif query.data.startswith("schedule_day_"):
        await schedule_day_selected(update, context)
    elif query.data.startswith("schedule_working_"):
        await schedule_working_selected(update, context)
    elif query.data.startswith("schedule_off_"):
        await schedule_off_selected(update, context)
    elif query.data.startswith("schedule_start_"):
        await schedule_start_selected(update, context)
    elif query.data.startswith("schedule_end_"):
        await schedule_end_selected(update, context)
    elif query.data == "manage_schedule":
        await manage_schedule(update, context)
    elif query.data == "schedule_cancel_appointments":
        await handle_schedule_cancel_appointments(update, context)
    elif query.data == "schedule_cancel_changes":
        await handle_schedule_cancel_changes(update, context)
    
    # ВИЗУАЛЬНОЕ РАСПИСАНИЕ
    elif query.data.startswith("call_"):
        await handle_schedule_actions(update, context)
    elif query.data.startswith("edit_"):
        await handle_schedule_actions(update, context)
    elif query.data.startswith("cancel_slot_"):
        await handle_schedule_actions(update, context)
    elif query.data == "refresh_today":
        await handle_schedule_actions(update, context)
    elif query.data == "all_contacts":
        await handle_schedule_actions(update, context)
    elif query.data == "show_today_visual":
        await handle_schedule_actions(update, context)
    
    elif query.data == "week_appointments":
        await show_week_appointments(update, context)
    elif query.data.startswith("week_day_"):
        date_str = query.data[9:]
        await show_day_appointments_visual(update, context, date_str)
    elif query.data.startswith("refresh_day_"):
        date_str = query.data[12:]
        await show_day_appointments_visual(update, context, date_str)
    elif query.data.startswith("day_contacts_"):
        date_str = query.data[13:]
        await show_day_contacts(update, context, date_str)
    elif query.data.startswith("called_"):
        await called_confirmation(update, context)
    elif query.data == "confirm_cancel_slot":
        await confirm_cancel_slot(update, context)
    
    else:
        logger.warning(f"⚠️ Неизвестный callback_data: {query.data}")

async def cancel_appointment(update: Update, context: ContextTypes.DEFAULT_TYPE, appointment_id: int):
    """Обработчик отмены записи С BACKUP"""
    query = update.callback_query
    user_id = query.from_user.id
    
    is_admin_cancel = query.data.startswith("cancel_admin_")
    
    if is_admin_cancel:
        if db.is_admin(user_id):
            cancel_scheduled_reminders(context, appointment_id)
            
            appointment = db.cancel_appointment(appointment_id)
            if appointment:
                try:
                    await query.edit_message_text(f"✅ Запись #{appointment_id} отменена администратором")
                except BadRequest as e:
                    if "message is not modified" in str(e).lower():
                        logger.debug("Message not modified in cancel_appointment - ignoring")
                    else:
                        raise
                await notify_client_about_cancellation(context, appointment)
                await notify_admin_about_cancellation(context, appointment, user_id, is_admin=True)
                
                # 🎯 СОЗДАЕМ BACKUP ПОСЛЕ ОТМЕНЫ ЗАПИСИ
                backup_path = db.create_backup()
                if backup_path:
                    logger.info(f"💾 Backup создан после отмены записи #{appointment_id}")
            else:
                try:
                    await query.edit_message_text("❌ Запись не найдена")
                except BadRequest as e:
                    if "message is not modified" in str(e).lower():
                        logger.debug("Message not modified in cancel_appointment - ignoring")
                    else:
                        raise
        else:
            await query.answer("У вас нет прав для отмены этой записи", show_alert=True)
    else:
        cancel_scheduled_reminders(context, appointment_id)
        
        appointment = db.cancel_appointment(appointment_id, user_id)
        if appointment:
            try:
                await query.edit_message_text(f"✅ Ваша запись #{appointment_id} отменена")
            except BadRequest as e:
                if "message is not modified" in str(e).lower():
                    logger.debug("Message not modified in cancel_appointment - ignoring")
                else:
                    raise
            await notify_admin_about_cancellation(context, appointment, user_id, is_admin=False)
            
            # 🎯 СОЗДАЕМ BACKUP ПОСЛЕ ОТМЕНЫ ЗАПИСИ
            backup_path = db.create_backup()
            if backup_path:
                logger.info(f"💾 Backup создан после отмены записи #{appointment_id}")
        else:
            await query.answer("Запись не найдена или у вас нет прав для её отмены", show_alert=True)

async def notify_client_about_cancellation(context: ContextTypes.DEFAULT_TYPE, appointment):
    """Уведомляет клиента об отмене записи"""
    user_id, user_name, phone, service, date, time = appointment
    
    if user_id == 0 or user_id is None or user_name == "Администратор":
        logger.info(f"Пропуск уведомления для ручной записи администратора: user_id={user_id}")
        return
        
    selected_date_obj = datetime.strptime(date, "%Y-%m-%d").date()
    weekday = selected_date_obj.weekday()
    day_name = config.WEEKDAYS[weekday]
    display_date = selected_date_obj.strftime("%d.%m.%Y")
    
    text = (
        f"❌ *Ваша запись в {config.BARBERSHOP_NAME} отменена администратором*\n\n"
        f"💇 Услуга: {service}\n"
        f"📅 Дата: {day_name} {display_date}\n"
        f"⏰ Время: {time}\n\n"
        "Приносим извинения за неудобства. Пожалуйста, запишитесь на другое время."
    )
    
    try:
        await context.bot.send_message(
            chat_id=user_id,
            text=text,
            parse_mode='Markdown'
        )
        logger.info(f"Уведомление об отмене отправлено клиенту {user_id}")
    except BadRequest as e:
        if "chat not found" in str(e).lower():
            logger.warning(f"Chat not found for user {user_id}, skipping notification")
        else:
            logger.error(f"BadRequest при отправке уведомления клиенту {user_id}: {e}")
    except Exception as e:
        logger.error(f"Ошибка отправки уведомления клиенту {user_id}: {e}")

async def notify_admin_about_cancellation(context: ContextTypes.DEFAULT_TYPE, appointment, cancelled_by_user_id, is_admin=False):
    """Уведомляет администраторов об отмене записи"""
    user_id, user_name, phone, service, date, time = appointment
    selected_date_obj = datetime.strptime(date, "%Y-%m-%d").date()
    weekday = selected_date_obj.weekday()
    day_name = config.WEEKDAYS[weekday]
    display_date = selected_date_obj.strftime("%d.%m.%Y")
    
    if is_admin:
        text = (
            f"❌ *Администратор отменил запись в {config.BARBERSHOP_NAME}*\n\n"
            f"👤 Клиент: {user_name}\n"
            f"📞 Телефон: {phone}\n"
            f"💇 Услуга: {service}\n"
            f"📅 Дата: {day_name} {display_date}\n"
            f"⏰ Время: {time}"
        )
    else:
        text = (
            f"❌ *Клиент отменил запись в {config.BARBERSHOP_NAME}*\n\n"
            f"👤 Клиент: {user_name}\n"
            f"📞 Телефон: {phone}\n"
            f"💇 Услуга: {service}\n"
            f"📅 Дата: {day_name} {display_date}\n"
            f"⏰ Время: {time}"
        )
    
    notification_chats = db.get_notification_chats()
    for chat_id in notification_chats:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode='Markdown'
            )
            logger.info(f"Уведомление об отмене отправлено администратору в чат {chat_id}")
        except BadRequest as e:
            if "chat not found" in str(e).lower():
                logger.warning(f"Chat not found for admin chat {chat_id}, skipping notification")
            else:
                logger.error(f"BadRequest при отправке уведомления об отмене в чат {chat_id}: {e}")
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления об отмене в чат {chat_id}: {e}")

async def send_new_appointment_notification(context: ContextTypes.DEFAULT_TYPE, user_name, user_username, phone, service, date, time, appointment_id, is_manual=False):
    """Отправляет уведомление о новой записи с номером телефона"""
    notification_chats = db.get_notification_chats()
    
    if not notification_chats:
        logger.info("Нет настроенных чатов для уведомлений")
        return
    
    manual_indicator = " 📝 (ручная запись)" if is_manual else ""
    
    text = (
        f"🆕 *Новая запись!*{manual_indicator}\n\n"
        f"👤 *Клиент:* {user_name}\n"
        f"📞 *Телефон:* {phone}\n"
        f"💇 *Услуга:* {service}\n"
        f"📅 *Дата:* {date}\n"
        f"⏰ *Время:* {time}\n"
        f"🆔 *ID записи:* #{appointment_id}"
    )
    
    for chat_id in notification_chats:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode='Markdown'
            )
            logger.info(f"Уведомление о новой записи отправлено в чат {chat_id}")
        except BadRequest as e:
            if "chat not found" in str(e).lower():
                logger.warning(f"Chat not found for admin chat {chat_id}, skipping notification")
            else:
                logger.error(f"BadRequest при отправке уведомления в чат {chat_id}: {e}")
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления в чат {chat_id}: {e}")

async def check_duplicate_appointments(context: ContextTypes.DEFAULT_TYPE):
    """Проверяет и уведомляет о дублирующихся записях"""
    try:
        duplicates = db.check_duplicate_appointments()
        
        if duplicates:
            for date, time, count in duplicates:
                appointments = db.get_appointments_by_datetime(date, time)
                
                selected_date_obj = datetime.strptime(date, "%Y-%m-%d").date()
                weekday = selected_date_obj.weekday()
                day_name = config.WEEKDAYS[weekday]
                display_date = selected_date_obj.strftime("%d.%m.%Y")
                
                text = (
                    f"⚠️ *ВНИМАНИЕ: Обнаружены дублирующиеся записи!*\n\n"
                    f"📅 Дата: {day_name} {display_date}\n"
                    f"⏰ Время: {time}\n"
                    f"👥 Количество записей: {count}\n\n"
                    f"*Список клиентов:*\n"
                )
                
                for appt_id, user_name, phone, service in appointments:
                    text += f"• {user_name} ({phone}) - {service} (#{appt_id})\n"
                
                text += f"\n*Рекомендуется связаться с клиентами и перенести записи*"
                
                await send_admin_notification(context, text)
    except Exception as e:
        logger.error(f"❌ Ошибка при проверке дублирующихся записей: {e}")

async def send_admin_notification(context: ContextTypes.DEFAULT_TYPE, text):
    """Отправляет уведомление всем администраторам"""
    notification_chats = db.get_notification_chats()
    
    for chat_id in notification_chats:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode='Markdown'
            )
            logger.info(f"Уведомление отправлено администратору в чат {chat_id}")
        except BadRequest as e:
            if "chat not found" in str(e).lower():
                logger.warning(f"Chat not found for admin chat {chat_id}, skipping notification")
            else:
                logger.error(f"BadRequest при отправке уведомления администратору в чат {chat_id}: {e}")
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления администратору в чат {chat_id}: {e}")

def is_valid_phone(phone):
    """Проверяет валидность номера телефона"""
    cleaned = re.sub(r'[^\d+]', '', phone)
    
    if cleaned.startswith('+7') and len(cleaned) == 12:
        return True
    elif cleaned.startswith('8') and len(cleaned) == 11:
        return True
    elif cleaned.startswith('7') and len(cleaned) == 11:
        return True
    elif len(cleaned) == 10:
        return True
    
    return False

def normalize_phone(phone):
    """Нормализует номер телефона к формату +7XXXXXXXXXX"""
    cleaned = re.sub(r'[^\d]', '', phone)
    
    if cleaned.startswith('8') and len(cleaned) == 11:
        return '+7' + cleaned[1:]
    elif cleaned.startswith('7') and len(cleaned) == 11:
        return '+' + cleaned
    elif len(cleaned) == 10:
        return '+7' + cleaned
    else:
        return phone

async def send_daily_schedule(context: ContextTypes.DEFAULT_TYPE):
    """Отправка ежедневного расписания администраторам"""
    try:
        cleanup_result = db.cleanup_completed_appointments()
        
        if cleanup_result['total_deleted'] > 0:
            logger.info(f"Автоочистка перед расписанием: удалено {cleanup_result['total_deleted']} записей")
        
        appointments = db.get_today_appointments()
        notification_chats = db.get_notification_chats()
        
        if not notification_chats:
            logger.info("Нет настроенных чатов для ежедневного расписания")
            return
        
        if not appointments:
            text = f"📅 На сегодня в {config.BARBERSHOP_NAME} записей нет"
        else:
            text = f"📅 *Записи на сегодня в {config.BARBERSHOP_NAME}:*\n\n"
            for user_name, phone, service, time in appointments:
                manual_indicator = " 📝" if user_name == "Администратор" else ""
                text += f"⏰ *{time}* - {user_name}{manual_indicator} ({phone}): {service}\n"
        
        for chat_id in notification_chats:
            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode='Markdown'
                )
                logger.info(f"Ежедневное расписание отправлено в чат {chat_id}")
            except BadRequest as e:
                if "chat not found" in str(e).lower():
                    logger.warning(f"Chat not found for admin chat {chat_id}, skipping daily schedule")
                else:
                    logger.error(f"BadRequest при отправке расписания в чат {chat_id}: {e}")
            except Exception as e:
                logger.error(f"Ошибка отправки расписания в чат {chat_id}: {e}")
    except Exception as e:
        logger.error(f"❌ Ошибка при подготовке ежедневного расписания: {e}")

async def check_duplicates_daily(context: ContextTypes.DEFAULT_TYPE):
    """Ежедневная проверка дублирующихся записей"""
    try:
        cleanup_result = db.cleanup_completed_appointments()
        
        if cleanup_result['total_deleted'] > 0:
            logger.info(f"Автоочистка перед проверкой дубликатов: удалено {cleanup_result['total_deleted']} записей")
        
        await check_duplicate_appointments(context)
    except Exception as e:
        logger.error(f"❌ Ошибка при проверке дубликатов: {e}")

async def cleanup_completed_appointments_daily(context: ContextTypes.DEFAULT_TYPE):
    """Удаляет записи старше 7 дней в 00:00 MSK"""
    try:
        seven_days_ago = (get_moscow_time() - timedelta(days=7)).strftime("%Y-%m-%d")
        
        cursor = db.execute_with_retry('DELETE FROM appointments WHERE appointment_date < ?', (seven_days_ago,))
        deleted_appointments = cursor.rowcount
        
        db.execute_with_retry('DELETE FROM schedule WHERE date < ?', (seven_days_ago,))
        
        db.conn.commit()
        
        logger.info(f"✅ Ежедневная очистка: удалено {deleted_appointments} записей старше 7 дней")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при ежедневной очистке: {e}")

async def cleanup_old_data(context: ContextTypes.DEFAULT_TYPE):
    """🎯 УЛУЧШЕННАЯ ОЧИСТКА ДЛЯ RENDER"""
    try:
        # Используем улучшенную функцию из БД
        cleanup_result = db.automatic_cleanup()  # 🎯 ИСПОЛЬЗУЕМ НОВУЮ ФУНКЦИЮ
        
        logger.info(f"✅ Очистка БД: удалено {cleanup_result['total_deleted']} записей")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при очистке пользователей: {e}")

async def cleanup_duplicate_reminders(context: ContextTypes.DEFAULT_TYPE):
    """Очищает дублирующиеся напоминания"""
    try:
        cursor = db.execute_with_retry('''
            DELETE FROM scheduled_reminders 
            WHERE id NOT IN (
                SELECT MIN(id) 
                FROM scheduled_reminders 
                GROUP BY appointment_id, reminder_type, sent
            )
        ''')
        deleted_count = cursor.rowcount
        db.conn.commit()
        
        if deleted_count > 0:
            logger.info(f"🧹 Очищено {deleted_count} дублирующихся напоминаний")
            
    except Exception as e:
        logger.error(f"❌ Ошибка при очистке дублирующихся напоминаний: {e}")

async def cleanup_old_reminders(context: ContextTypes.DEFAULT_TYPE):
    """Очищает старые отправленные напоминания"""
    try:
        seven_days_ago = (get_moscow_time() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
        cursor = db.execute_with_retry('''
            DELETE FROM scheduled_reminders 
            WHERE sent = TRUE AND scheduled_time < ?
        ''', (seven_days_ago,))
        deleted_count = cursor.rowcount
        db.conn.commit()
        
        if deleted_count > 0:
            logger.info(f"✅ Очищено {deleted_count} старых напоминаний")
            
    except Exception as e:
        logger.error(f"❌ Ошибка при очистке старых напоминаний: {e}")

async def debug_timezones(context: ContextTypes.DEFAULT_TYPE):
    """Отладочная функция для проверки временных зон"""
    try:
        now_utc = datetime.now(timezone.utc)
        now_moscow = get_moscow_time()
        
        logger.info("🕐 === TIMEZONE DEBUG ===")
        logger.info(f"🕐 UTC время: {now_utc.strftime('%d.%m.%Y %H:%M')}")
        logger.info(f"🕐 MSK время: {now_moscow.strftime('%d.%m.%Y %H:%M')}")
        logger.info(f"🕐 Разница: {now_moscow - now_utc}")
        
        jobs = context.job_queue.jobs()
        logger.info(f"📋 Запланировано задач: {len(jobs)}")
        
        for job in jobs:
            if hasattr(job, 'next_t') and job.next_t:
                job_time_utc = job.next_t
                job_time_moscow = job_time_utc.astimezone(timezone(timedelta(hours=3)))
                logger.info(f"📋 Задача '{job.name}': {job_time_moscow.strftime('%d.%m.%Y %H:%M')} MSK")
        
        logger.info("🕐 === END TIMEZONE DEBUG ===")
        
    except Exception as e:
        logger.error(f"❌ Ошибка в debug_timezones: {e}")

async def optimize_database(context: ContextTypes.DEFAULT_TYPE):
    """Регулярная оптимизация SQLite базы"""
    try:
        # ПЕРЕД каждой операцией проверяем соединение
        db.check_connection()
        cursor = db.execute_with_retry('PRAGMA optimize')
        db.conn.commit()
        logger.info("✅ База данных оптимизирована")
    except Exception as e:
        logger.error(f"❌ Ошибка оптимизации БД: {e}")
        # Пытаемся восстановить соединение
        try:
            db.reconnect()
        except:
            pass

async def backup_database(context: ContextTypes.DEFAULT_TYPE):
    """🎯 РУЧНОЕ РЕЗЕРВНОЕ КОПИРОВАНИЕ (только по команде администратора)"""
    try:
        logger.info("💾 Запуск ручного backup по команде администратора...")
        
        # Создаем backup через database.py
        backup_path = db.create_backup()
        
        if backup_path:
            logger.info(f"✅ Ручной backup создан: {backup_path}")
            
            # Уведомляем администратора
            text = (
                f"💾 *Ручной backup создан успешно!*\n\n"
                f"📁 Файл: `{os.path.basename(backup_path)}`\n"
                f"📏 Размер: {os.path.getsize(backup_path) / 1024:.1f} KB\n"
                f"⏰ Время: {get_moscow_time().strftime('%d.%m.%Y %H:%M')}\n\n"
                f"*Данные будут автоматически восстановлены при перезапуске*"
            )
        else:
            logger.warning("⚠️ Ручной backup не создан")
            text = (
                f"❌ *Backup не создан*\n\n"
                f"⚠️ Не удалось создать локальный backup\n\n"
                f"*Возможные причины:*\n"
                f"• Ошибка доступа к файлам\n"
                f"• Недостаточно места в /tmp/\n"
                f"• Проблемы с базой данных"
            )
        
        # Отправляем уведомление только тому администратору, который вызвал backup
        if context.job and 'user_id' in context.job.data:
            admin_id = context.job.data['user_id']
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=text,
                    parse_mode='Markdown'
                )
            except Exception as e:
                logger.error(f"❌ Ошибка отправки уведомления администратору {admin_id}: {e}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при создании ручного backup: {e}")

async def check_database_size(context: ContextTypes.DEFAULT_TYPE):
    """🎯 УЛУЧШЕННАЯ ПРОВЕРКА РАЗМЕРА БД ДЛЯ RENDER"""
    try:
        # Получаем правильный путь к БД
        db_path = get_database_path()  # 🎯 ИСПОЛЬЗУЕМ ФУНКЦИЮ ИЗ database.py
        
        # Проверяем существование файла
        if not os.path.exists(db_path):
            logger.error(f"❌ Файл БД не найден: {db_path}")
            # Пытаемся пересоздать таблицы
            try:
                db.create_tables()
                logger.info("✅ Таблицы БД пересозданы")
            except Exception as e:
                logger.error(f"❌ Ошибка пересоздания таблиц: {e}")
            return
        
        # Проверяем размер
        size = os.path.getsize(db_path) / (1024 * 1024)  # Размер в MB
        logger.info(f"📊 Размер базы данных: {size:.2f} MB")
        
        # Проверяем целостность БД
        try:
            cursor = db.execute_with_retry('PRAGMA integrity_check')
            integrity = cursor.fetchone()[0]
            if integrity == 'ok':
                logger.info("✅ Целостность БД: OK")
            else:
                logger.error(f"❌ Проблемы с целостностью БД: {integrity}")
        except Exception as e:
            logger.error(f"❌ Ошибка проверки целостности: {e}")
        
        # 🎯 АВТОМАТИЧЕСКАЯ ОЧИСТКА ЕСЛИ БД СЛИШКОМ БОЛЬШАЯ
        if size > 5:  # 5MB лимит для бесплатного Render
            try:
                cleanup_result = db.automatic_cleanup()  # 🎯 ИСПОЛЬЗУЕМ НОВУЮ ФУНКЦИЮ
                logger.info(f"🧹 Автоочистка: удалено {cleanup_result['total_deleted']} записей")
                
                if cleanup_result['total_deleted'] > 0:
                    # Проверяем размер после очистки
                    new_size = os.path.getsize(db_path) / (1024 * 1024)
                    logger.info(f"📊 Размер БД после очистки: {new_size:.2f} MB")
                    
            except Exception as e:
                logger.error(f"❌ Ошибка автоочистки: {e}")
        
        # Предупреждение если больше 3MB
        if size > 3:
            text = f"⚠️ *Размер БД:* {size:.2f} MB\n*Рекомендация:* Автоматическая очистка выполнена"
            notification_chats = db.get_notification_chats()
            for chat_id in notification_chats:
                try:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=text,
                        parse_mode='Markdown'
                    )
                except Exception as e:
                    logger.error(f"Ошибка отправки уведомления: {e}")
            
    except Exception as e:
        logger.error(f"❌ Ошибка проверки БД: {e}")

async def keep_database_alive(context: ContextTypes.DEFAULT_TYPE):
    """Поддерживает соединение с БД активным на Render"""
    try:
        # Простой запрос чтобы БД не "засыпала"
        cursor = db.execute_with_retry('SELECT 1')
        logger.debug("✅ Соединение с БД активно")
    except Exception as e:
        logger.warning(f"⚠️ Проблема с соединением БД, переподключаемся: {e}")
        try:
            db.reconnect()
            logger.info("✅ Переподключение к БД успешно")
        except Exception as reconnect_error:
            logger.error(f"❌ Критическая ошибка переподключения: {reconnect_error}")

async def prevent_sleep_mode(context: ContextTypes.DEFAULT_TYPE):
    """🎯 ПРЕДОТВРАЩЕНИЕ РЕЖИМА СНА RENDER"""
    try:
        # Простая операция для поддержания активности
        cursor = db.execute_with_retry('SELECT COUNT(*) FROM appointments WHERE appointment_date >= DATE("now")')
        future_appointments = cursor.fetchone()[0]
        
        logger.debug(f"🔧 Keep-alive: {future_appointments} будущих записей")
        
    except Exception as e:
        logger.warning(f"⚠️ Keep-alive операция провалилась: {e}")
        try:
            db.reconnect()
            logger.info("✅ Переподключение к БД после keep-alive ошибки")
        except Exception as reconnect_error:
            logger.error(f"❌ Критическая ошибка переподключения: {reconnect_error}")

async def scheduled_restart(context: ContextTypes.DEFAULT_TYPE):
    """🎯 АВТОМАТИЧЕСКИЙ ПЕРЕЗАПУСК ДЛЯ RENDER (каждые 80 дней)"""
    try:
        logger.info("🔄 Запланированный перезапуск для обхода 90-дневного лимита Render...")
        
        # Отправляем уведомление администраторам
        text = "🔄 *Плановый перезапуск бота*\n\nБот будет автоматически перезапущен для поддержания стабильной работы."
        notification_chats = db.get_notification_chats()
        for chat_id in notification_chats:
            try:
                await context.bot.send_message(chat_id=chat_id, text=text, parse_mode='Markdown')
            except Exception as e:
                logger.error(f"Ошибка отправки уведомления: {e}")
        
        # Аккуратно останавливаем бота
        await context.application.stop()
        await context.application.shutdown()
        
        # Запускаем новый процесс
        os.execv(sys.executable, [sys.executable] + sys.argv)
        
    except Exception as e:
        logger.error(f"❌ Ошибка при плановом перезапуске: {e}")

def setup_job_queue(application: Application):
    job_queue = application.job_queue

    # 🎯 ПРОВЕРКА БД ПРИ ЗАПУСКЕ
    job_queue.run_once(
        callback=lambda context: asyncio.create_task(check_database_status(context)),
        when=10,
        name="check_db_status"
    )

    # 🎯 ПЛАНОВЫЙ ПЕРЕЗАПУСК КАЖДЫЕ 80 ДНЕЙ
    job_queue.run_repeating(
        scheduled_restart, 
        interval=6912000,  # 80 дней в секундах
        first=86400,       # Первый через 24 часа
        name="scheduled_restart"
    )
    
    # 🎯 ПРЕДОТВРАЩЕНИЕ СНА - КАЖДЫЕ 10 МИНУТ
    job_queue.run_repeating(
        prevent_sleep_mode, 
        interval=600, 
        first=60, 
        name="prevent_sleep"
    )

    # 🎯 МОНИТОРИНГ ПАМЯТИ КАЖДЫЕ 30 МИНУТ
    job_queue.run_repeating(
        check_memory_usage,
        interval=1800,  # 30 минут
        first=60,       # Первый через 1 минуту
        name="check_memory"
    )

    job_queue.run_once(
        callback=lambda context: asyncio.create_task(restore_scheduled_reminders(context)), 
        when=5, 
        name="restore_reminders"
    )
    
    job_queue.run_repeating(optimize_database, interval=3600, first=60, name="optimize_db")
    
    # ИСПРАВЛЕННЫЕ ВРЕМЕНА (в UTC):
    
    # 06:00 UTC = 09:00 MSK - Ежедневное расписание администраторам
    job_queue.run_daily(send_daily_schedule, time=datetime.strptime("06:00", "%H:%M").time(), name="daily_schedule")
    
    # 02:00 UTC = 05:00 MSK - Проверка дубликатов  
    job_queue.run_daily(check_duplicates_daily, time=datetime.strptime("02:00", "%H:%M").time(), name="check_duplicates")
    
    # 21:00 UTC = 00:00 MSK - Очистка неактивных пользователей
    job_queue.run_daily(cleanup_old_data, time=datetime.strptime("21:00", "%H:%M").time(), name="cleanup_old_data")
    
    # 21:00 UTC = 00:00 MSK - Очистка старых записей
    job_queue.run_daily(cleanup_completed_appointments_daily, time=datetime.strptime("21:00", "%H:%M").time(), name="cleanup_old_appointments")
    
    # 22:00 UTC = 01:00 MSK - Очистка старых напоминаний
    job_queue.run_daily(cleanup_old_reminders, time=datetime.strptime("22:00", "%H:%M").time(), name="cleanup_old_reminders")
    
    job_queue.run_once(cleanup_duplicate_reminders, when=10, name="cleanup_duplicate_reminders")

def kill_duplicate_processes():
    """🎯 УЛУЧШЕННАЯ ЗАЩИТА ОТ ДУБЛИРУЮЩИХСЯ ПРОЦЕССОВ"""
    current_pid = os.getpid()
    current_script = os.path.basename(__file__)
    
    killed_count = 0
    for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'create_time']):
        try:
            if (proc.info['pid'] != current_pid and 
                'python' in proc.info['name'].lower() and 
                proc.info['cmdline'] and 
                any('bot.py' in cmd for cmd in proc.info['cmdline'] if cmd)):
                
                # 🎯 УБИВАЕМ ТОЛЬКО СТАРЫЕ ПРОЦЕССЫ
                current_proc = psutil.Process(current_pid)
                if proc.info['create_time'] < current_proc.create_time():
                    logger.info(f"🔄 Найден старый процесс PID {proc.info['pid']}, завершаем...")
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                        killed_count += 1
                        logger.info(f"✅ Процесс PID {proc.info['pid']} завершен")
                    except psutil.TimeoutExpired:
                        proc.kill()
                        logger.warning(f"⚠️ Процесс PID {proc.info['pid']} принудительно завершен")
                
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.TimeoutExpired):
            continue
    
    if killed_count > 0:
        logger.info(f"✅ Завершено {killed_count} старых процессов")

def create_lock_file():
    """Создает файл блокировки для предотвращения дублирующихся запусков"""
    lock_file = '/tmp/barbershop_bot.lock'
    
    try:
        lock_fd = open(lock_file, 'w')
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        
        def cleanup_lock():
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                lock_fd.close()
                if os.path.exists(lock_file):
                    os.unlink(lock_file)
                logger.info("🔓 Lock file очищен")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка при очистке lock file: {e}")
        
        atexit.register(cleanup_lock)
        logger.info("🔒 Lock file создан - дублирующиеся процессы заблокированы")
        return True
        
    except (IOError, OSError):
        logger.error("❌ Бот уже запущен! Завершите предыдущий процесс перед запуском нового.")
        return False

async def check_database_status(context: ContextTypes.DEFAULT_TYPE):
    """🎯 ПРОВЕРКА СОСТОЯНИЯ БАЗЫ ДАННЫХ"""
    try:
        # Проверяем существование файла БД
        db_path = get_database_path()
        db_exists = os.path.exists(db_path)
        
        # Получаем статистику
        cursor = db.execute_with_retry('SELECT COUNT(*) FROM appointments')
        appointments_count = cursor.fetchone()[0]
        
        cursor = db.execute_with_retry('SELECT COUNT(*) FROM bot_users')
        users_count = cursor.fetchone()[0]
        
        # Логируем информацию
        logger.info(f"🔍 ДИАГНОСТИКА БД:")
        logger.info(f"   📁 Путь: {db_path}")
        logger.info(f"   ✅ Файл существует: {db_exists}")
        logger.info(f"   📊 Записей: {appointments_count}")
        logger.info(f"   👥 Пользователей: {users_count}")
        
        # Отправляем уведомление администраторам
        if context:
            text = (
                f"🔍 *Диагностика БД после перезапуска:*\n"
                f"• Файл БД: {'✅ Существует' if db_exists else '❌ Отсутствует'}\n"
                f"• Путь: `{db_path}`\n"
                f"• Записей: {appointments_count}\n"
                f"• Пользователей: {users_count}"
            )
            
            notification_chats = db.get_notification_chats()
            for chat_id in notification_chats:
                try:
                    await context.bot.send_message(chat_id=chat_id, text=text, parse_mode='Markdown')
                except Exception as e:
                    logger.error(f"Ошибка отправки диагностики: {e}")
                    
    except Exception as e:
        logger.error(f"❌ Ошибка диагностики БД: {e}")

def main():
    """Главная функция с улучшенной обработкой ошибок и защитой от конфликтов"""
    
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger('telegram').setLevel(logging.INFO)
    
    if not create_lock_file():
        logger.error("❌ Не удалось создать lock file. Бот уже запущен!")
        sys.exit(1)
    
    kill_duplicate_processes()
    
    logger.info("🚀 Starting Barbershop Bot with enhanced 24/7 support and CONFLICT PROTECTION...")
    
    try:
        import requests
        bot_token = config.BOT_TOKEN
        for i in range(3):
            try:
                response = requests.post(
                    f"https://api.telegram.org/bot{bot_token}/deleteWebhook", 
                    timeout=10
                )
                logger.info(f"✅ Webhook deletion attempt {i+1}: {response.status_code}")
                
                response = requests.post(
                    f"https://api.telegram.org/bot{bot_token}/getUpdates",
                    json={"offset": -1, "limit": 1},
                    timeout=10
                )
                logger.info(f"✅ Updates reset attempt {i+1}")
                time.sleep(2)
            except Exception as e:
                logger.warning(f"⚠️ Webhook cleanup attempt {i+1} failed: {e}")
                
    except Exception as e:
        logger.warning(f"⚠️ Webhook cleanup warning: {e}")
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    web_thread = threading.Thread(target=run_web_server, daemon=True)
    web_thread.start()
    logger.info("🌐 Web server thread started")

    start_enhanced_self_ping()
    logger.info("🔁 Enhanced self-ping service started")

    time.sleep(3)
    
    try:
        port = int(os.getenv('PORT', 10000))
        import requests
        health_url = f"http://localhost:{port}/healthcheck"
        response = requests.get(health_url, timeout=5)
        if response.status_code == 200:
            logger.info(f"✅ Web server confirmed running on port {port}")
        else:
            logger.warning(f"⚠️ Web server responded with status: {response.status_code}")
    except Exception as e:
        logger.error(f"❌ Web server health check failed: {e}")
    
    restart_count = 0
    max_restarts = 10
    
    while restart_count < max_restarts:
        try:
            restart_count += 1
            logger.info(f"🤖 Initializing bot application (restart #{restart_count})...")
            
            try:
                import requests
                bot_token = config.BOT_TOKEN
                response = requests.post(
                    f"https://api.telegram.org/bot{bot_token}/deleteWebhook", 
                    json={"drop_pending_updates": True},
                    timeout=10
                )
                logger.info(f"✅ Final webhook cleanup: {response.status_code}")
                time.sleep(3)
            except Exception as e:
                logger.warning(f"⚠️ Final webhook cleanup failed: {e}")
            
            global db
            try:
                db = database.Database()
                logger.info("✅ Database connection reestablished")
            except Exception as e:
                logger.error(f"❌ Database connection failed: {e}")
                time.sleep(10)
                continue
            
            application = Application.builder().token(config.BOT_TOKEN).build()
            logger.info("✅ Application created")
            
            application.add_error_handler(error_handler)
            logger.info("✅ Error handler added")
            
            conv_handler = ConversationHandler(
                entry_points=[
                    CallbackQueryHandler(time_selected, pattern="^time_"),
                ],
                states={
                    PHONE: [
                        MessageHandler(filters.CONTACT | (filters.TEXT & ~filters.COMMAND), phone_input),
                    ],
                },
                fallbacks=[
                    MessageHandler(filters.Regex("^🔙 Назад$"), date_selected_back),
                    CommandHandler("start", start)
                ],
                per_message=False
            )
            
            application.add_handler(conv_handler)
            logger.info("✅ ConversationHandler added")
            
            application.add_handler(CommandHandler("start", start))
            logger.info("✅ CommandHandler 'start' added")
            
            application.add_handler(CommandHandler("stop", stop_command))
            logger.info("✅ CommandHandler 'stop' added")

            application.add_handler(CommandHandler("backup", backup_info))
            application.add_handler(CommandHandler("backup_info", backup_info))
            logger.info("✅ CommandHandler 'backup' and 'backup_info' added")

            application.add_handler(CommandHandler("check_backup", check_backup_content))
            logger.info("✅ CommandHandler 'check_backup' added")
            
            application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
            logger.info("✅ MessageHandler for text added")
            
            application.add_handler(CallbackQueryHandler(button_handler))
            logger.info("✅ CallbackQueryHandler added")
            
            try:
                setup_job_queue(application)
                logger.info("✅ Job queue setup completed")
            except Exception as e:
                logger.error(f"❌ Job queue setup failed: {e}")
            
            logger.info("🤖 Bot starting in polling mode with Render optimization...")
            
            try:
                import requests
                bot_token = config.BOT_TOKEN
                response = requests.get(f"https://api.telegram.org/bot{bot_token}/getMe", timeout=10)
                if response.status_code == 200:
                    bot_info = response.json()
                    logger.info(f"✅ Bot info: {bot_info['result']['username']} (ID: {bot_info['result']['id']})")
                else:
                    logger.error(f"❌ Bot token validation failed: {response.status_code}")
                    time.sleep(10)
                    continue
            except Exception as e:
                logger.error(f"❌ Bot token validation failed: {e}")
                time.sleep(10)
                continue
            
            application.run_polling(
                poll_interval=3.0,
                timeout=20,
                drop_pending_updates=True,
                allowed_updates=['message', 'callback_query'],
                close_loop=False
            )
            
            logger.info("🤖 Bot stopped normally - restarting...")
            restart_count = 0
            
        except Conflict as e:
            logger.warning(f"⚠️ CONFLICT DETECTED: {e}")
            logger.info("🔄 Waiting 5 seconds before retry...")
            time.sleep(5)
            
        except Exception as e:
            logger.error(f"❌ Bot crashed with error: {e}")
            logger.error(f"❌ Error type: {type(e).__name__}")
            import traceback
            logger.error(f"❌ Traceback: {traceback.format_exc()}")
            
            wait_time = min(5 * restart_count, 30)
            logger.info(f"🔄 Restarting bot in {wait_time} seconds... (restart #{restart_count})")
            time.sleep(wait_time)
            
            import gc
            gc.collect()

    logger.error(f"❌ Maximum restart attempts ({max_restarts}) reached. Exiting.")

if __name__ == "__main__":
    main()