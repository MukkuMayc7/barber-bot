# app.py - FIXED VERSION WITH PROPER INITIALIZATION
import os
import asyncio
from aiohttp import web
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация
BOT_TOKEN = "8297051179:AAGHxFTyY2ourq2qmORND-oBN5TaKVYM0uE"
WEBHOOK_URL = "https://barber-bot-render.onrender.com"
WEBHOOK_PATH = f"/{BOT_TOKEN}"

# Глобальные переменные
bot_initialized = False
application = None
init_start_time = None

async def initialize_bot():
    """Инициализация бота - СИНХРОННАЯ чтобы гарантировать запуск"""
    global bot_initialized, application, init_start_time
    
    init_start_time = time.time()
    logger.info("🚀 STARTING BOT INITIALIZATION...")
    
    try:
        # Импорты внутри функции чтобы избежать циклических зависимостей
        from telegram.ext import Application
        import database
        from bot import setup_handlers
        
        # Инициализируем базу данных
        logger.info("📦 Initializing database...")
        db = database.Database()
        logger.info("✅ Database initialized")
        
        # Создаем приложение бота
        logger.info("🤖 Creating bot application...")
        application = Application.builder().token(BOT_TOKEN).build()
        logger.info("✅ Bot application created")
        
        # Настраиваем обработчики
        logger.info("⚙️ Setting up handlers...")
        setup_handlers(application)
        logger.info("✅ Bot handlers setup completed")
        
        # Устанавливаем вебхук
        logger.info("🌐 Setting webhook...")
        await application.bot.set_webhook(
            f"{WEBHOOK_URL}{WEBHOOK_PATH}",
            drop_pending_updates=True
        )
        logger.info(f"✅ Webhook set: {WEBHOOK_URL}{WEBHOOK_PATH}")
        
        # Инициализируем планировщик
        logger.info("⏰ Starting job queue...")
        from bot import setup_job_queue
        setup_job_queue(application)
        logger.info("✅ Job queue started")
        
        bot_initialized = True
        init_time = time.time() - init_start_time
        logger.info(f"🎉 Bot fully initialized in {init_time:.2f} seconds!")
        
    except Exception as e:
        logger.error(f"💥 Bot initialization failed: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")

async def handle_webhook(request):
    """Обработчик вебхука"""
    global bot_initialized, application
    
    if not bot_initialized:
        logger.warning("⏳ Bot not initialized yet - returning 503")
        return web.Response(
            text="Bot initializing, please try again in a few seconds", 
            status=503
        )
    
    try:
        # Получаем данные обновления
        data = await request.json()
        
        # Обрабатываем через полную систему бота
        from telegram import Update
        update = Update.de_json(data, application.bot)
        await application.process_update(update)
        
        logger.info("✅ Webhook processed successfully")
        return web.Response(text="OK")
        
    except Exception as e:
        logger.error(f"❌ Webhook processing error: {e}")
        return web.Response(text="Error", status=500)

async def health_check(request):
    """Проверка здоровья приложения"""
    global bot_initialized, init_start_time
    
    status_info = {
        "status": "RUNNING" if bot_initialized else "INITIALIZING",
        "bot_initialized": bot_initialized
    }
    
    if init_start_time and not bot_initialized:
        init_time = time.time() - init_start_time
        status_info["initializing_for_seconds"] = f"{init_time:.1f}s"
    
    import json
    return web.Response(
        text=json.dumps(status_info, ensure_ascii=False),
        content_type='application/json'
    )

async def start_server():
    """Запуск сервера с гарантированной инициализацией бота"""
    # Сначала инициализируем бота СИНХРОННО
    logger.info("🎯 Starting bot initialization BEFORE server start...")
    await initialize_bot()
    
    # Затем запускаем сервер
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    app.router.add_get('/health', health_check)
    app.router.add_get('/', health_check)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    port = int(os.environ.get('PORT', 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    
    logger.info(f"🚀 Server started on port {port}")
    logger.info("📱 Bot is ready to receive webhooks!")
    
    # Бесконечный цикл для поддержания работы
    await asyncio.Future()

if __name__ == '__main__':
    # Запускаем все синхронно
    asyncio.run(start_server())