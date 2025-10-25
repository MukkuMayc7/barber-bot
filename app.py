# app.py
import os
import asyncio
from aiohttp import web
import logging
import sys

# Добавляем путь для импортов
sys.path.append(os.path.dirname(__file__))

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Глобальная переменная для бота
bot_application = None

async def init_bot():
    """Инициализация бота один раз при старте"""
    global bot_application
    try:
        from telegram.ext import Application
        import config
        
        logger.info("🚀 INITIALIZING TELEGRAM BOT...")
        bot_application = Application.builder().token(config.BOT_TOKEN).build()
        
        # Настраиваем обработчики
        from bot import setup_handlers
        setup_handlers(bot_application)
        
        # Настраиваем вебхук если указан URL
        if config.WEBHOOK_URL:
            webhook_url = f"{config.WEBHOOK_URL}/8297051179:AAGHxFTyY2ourq2qmORND-oBN5TaKVYM0uE"
            logger.info(f"🔗 SETTING WEBHOOK TO: {webhook_url}")
            await bot_application.bot.set_webhook(webhook_url, drop_pending_updates=True)
            logger.info("✅ WEBHOOK SET SUCCESSFULLY!")
        else:
            logger.warning("⚠️ WEBHOOK_URL NOT SET")
            
        logger.info("✅ BOT INITIALIZED SUCCESSFULLY!")
        return True
        
    except Exception as e:
        logger.error(f"❌ FAILED TO INITIALIZE BOT: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

async def handle_webhook(request):
    """Обработчик вебхука от Telegram"""
    global bot_application
    
    if bot_application is None:
        logger.error("❌ BOT NOT INITIALIZED!")
        return web.Response(text="Bot not ready", status=503)
    
    try:
        # Получаем данные обновления
        data = await request.json()
        from telegram import Update
        update = Update.de_json(data, bot_application.bot)
        
        # Обрабатываем обновление
        await bot_application.process_update(update)
        logger.info("📨 WEBHOOK PROCESSED SUCCESSFULLY")
        return web.Response(text="OK", status=200)
        
    except Exception as e:
        logger.error(f"❌ ERROR PROCESSING WEBHOOK: {e}")
        return web.Response(text="Error", status=500)

async def health_check(request):
    """Проверка здоровья приложения"""
    status = "RUNNING" if bot_application else "INITIALIZING"
    return web.Response(text=f"Bot is {status}!")

async def init_app():
    """Инициализация веб-приложения"""
    logger.info("🌐 INITIALIZING WEB APPLICATION...")
    
    # Инициализируем бота
    success = await init_bot()
    
    app = web.Application()
    
    # Маршруты
    app.router.add_post('/8297051179:AAGHxFTyY2ourq2qmORND-oBN5TaKVYM0uE', handle_webhook)
    app.router.add_get('/health', health_check)
    app.router.add_get('/', health_check)
    
    if success:
        logger.info("✅ WEB APPLICATION READY")
    else:
        logger.error("❌ WEB APPLICATION INITIALIZATION FAILED")
    
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
        logger.info("🎯 BOT IS READY TO RECEIVE WEBHOOKS!")
        
        # Бесконечный цикл
        while True:
            await asyncio.sleep(3600)
    
    asyncio.run(start_server())