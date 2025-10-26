# app.py - COMPATIBLE WITH PTB 13.15
import os
import asyncio
from aiohttp import web
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация
BOT_TOKEN = "8297051179:AAGHxFTyY2ourq2qmORND-oBN5TaKVYM0uE"
WEBHOOK_URL = "https://barber-bot-render.onrender.com"
WEBHOOK_PATH = f"/{BOT_TOKEN}"

# Глобальные переменные
bot_initialized = False
application = None

async def initialize_bot():
    """Инициализация бота"""
    global bot_initialized, application
    
    try:
        logger.info("🚀 STARTING BOT INITIALIZATION...")
        
        # Импорты внутри функции чтобы избежать циклических зависимостей
        from telegram.ext import Updater
        import database
        from bot import setup_handlers
        
        # Инициализируем базу данных
        logger.info("📦 Initializing database...")
        db = database.Database()
        logger.info("✅ Database initialized")
        
        # Создаем приложение бота (для версии 13.15 используем Updater)
        logger.info("🤖 Creating bot application...")
        application = Updater(token=BOT_TOKEN, use_context=True)
        logger.info("✅ Bot application created")
        
        # Настраиваем обработчики
        logger.info("⚙️ Setting up handlers...")
        setup_handlers(application)
        logger.info("✅ Bot handlers setup completed")
        
        # Устанавливаем вебхук
        logger.info("🌐 Setting webhook...")
        application.bot.set_webhook(
            url=f"{WEBHOOK_URL}{WEBHOOK_PATH}",
            drop_pending_updates=True
        )
        logger.info(f"✅ Webhook set: {WEBHOOK_URL}{WEBHOOK_PATH}")
        
        bot_initialized = True
        logger.info("🎉 Bot fully initialized and ready!")
        
    except Exception as e:
        logger.error(f"💥 Bot initialization failed: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")

async def handle_webhook(request):
    """Обработчик вебхука"""
    global bot_initialized, application
    
    if not bot_initialized:
        logger.warning("⏳ Bot not initialized yet")
        return web.Response(text="Bot initializing", status=503)
    
    try:
        data = await request.json()
        from telegram import Update
        update = Update.de_json(data, application.bot)
        
        # Для версии 13.15 используем process_update через dispatcher
        application.dispatcher.process_update(update)
        
        logger.info("✅ Webhook processed successfully")
        return web.Response(text="OK")
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return web.Response(text="Error", status=500)

async def health_check(request):
    """Проверка здоровья приложения"""
    global bot_initialized
    status = "RUNNING" if bot_initialized else "INITIALIZING"
    return web.Response(text=f"Bot status: {status}")

async def main():
    """Основная функция"""
    # Создаем приложение aiohttp
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    app.router.add_get('/health', health_check)
    app.router.add_get('/', health_check)
    
    # Запускаем инициализацию бота в фоне
    asyncio.create_task(initialize_bot())
    
    # Запускаем сервер
    runner = web.AppRunner(app)
    await runner.setup()
    
    port = int(os.environ.get('PORT', 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    
    logger.info(f"🚀 Server started on port {port}")
    logger.info("📱 Bot is starting up...")
    
    # Бесконечный цикл
    await asyncio.Future()

if __name__ == '__main__':
    asyncio.run(main())