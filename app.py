# app.py - FULL FUNCTIONALITY VERSION
import os
import asyncio
from aiohttp import web
import logging
import aiohttp

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
    """Инициализация полного функционала бота"""
    global bot_initialized, application
    
    try:
        logger.info("🚀 INITIALIZING FULL BOT FUNCTIONALITY...")
        
        # Импортируем необходимые модули
        from telegram.ext import Application
        from bot import setup_handlers
        import database
        
        # Инициализируем базу данных
        db = database.Database()
        logger.info("✅ DATABASE INITIALIZED")
        
        # Создаем приложение бота
        application = Application.builder().token(BOT_TOKEN).build()
        logger.info("✅ BOT APPLICATION CREATED")
        
        # Настраиваем обработчики
        setup_handlers(application)
        logger.info("✅ BOT HANDLERS SETUP COMPLETED")
        
        # Устанавливаем вебхук
        webhook_url = f"{WEBHOOK_URL}{WEBHOOK_PATH}"
        await application.bot.set_webhook(webhook_url, drop_pending_updates=True)
        logger.info(f"✅ WEBHOOK SET: {webhook_url}")
        
        bot_initialized = True
        logger.info("🎉 FULL BOT FUNCTIONALITY INITIALIZED!")
        return True
        
    except Exception as e:
        logger.error(f"💥 BOT INITIALIZATION FAILED: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

async def handle_webhook(request):
    """Обработчик вебхука с полным функционалом"""
    global bot_initialized, application
    
    if not bot_initialized or application is None:
        logger.warning("⚠️ Bot not fully initialized")
        return web.Response(text="Bot initializing", status=503)
    
    try:
        # Получаем данные обновления
        data = await request.json()
        
        # Обрабатываем через полную систему бота
        from telegram import Update
        update = Update.de_json(data, application.bot)
        await application.process_update(update)
        
        logger.info("✅ WEBHOOK PROCESSED WITH FULL FUNCTIONALITY")
        return web.Response(text="OK")
        
    except Exception as e:
        logger.error(f"❌ Webhook processing error: {e}")
        return web.Response(text="Error", status=500)

async def health_check(request):
    status = "RUNNING" if bot_initialized else "INITIALIZING"
    return web.Response(text=f"Bot is {status}!")

async def init_app():
    """Инициализация приложения"""
    logger.info("🌐 INITIALIZING WEB APPLICATION...")
    
    # Инициализируем полный функционал бота
    await initialize_bot()
    
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    app.router.add_get('/health', health_check)
    app.router.add_get('/', health_check)
    
    logger.info("✅ WEB APPLICATION READY")
    return app

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    
    async def start_server():
        app = await init_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', port)
        await site.start()
        logger.info(f"🚀 SERVER STARTED ON PORT {port}")
        logger.info("🎯 FULL BOT FUNCTIONALITY READY!")
        
        while True:
            await asyncio.sleep(3600)
    
    asyncio.run(start_server())