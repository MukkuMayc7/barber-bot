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
        
        logger.info("Initializing Telegram bot...")
        bot_application = Application.builder().token(config.BOT_TOKEN).build()
        
        # Настраиваем обработчики
        from bot import setup_handlers
        setup_handlers(bot_application)
        
        # Настраиваем вебхук если указан URL
        if config.WEBHOOK_URL:
            webhook_url = f"{config.WEBHOOK_URL}/8297051179:AAGHxFTyY2ourq2qmORND-oBN5TaKVYM0uE"
            logger.info(f"Setting webhook to: {webhook_url}")
            await bot_application.bot.set_webhook(webhook_url, drop_pending_updates=True)
            logger.info("Webhook set successfully!")
        else:
            logger.info("WEBHOOK_URL not set, running in webhook mode without webhook URL")
            
        logger.info("Bot initialized successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Failed to initialize bot: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

async def handle_webhook(request):
    """Обработчик вебхука от Telegram"""
    global bot_application
    
    if bot_application is None:
        logger.error("Bot not initialized!")
        return web.Response(text="Bot not ready", status=503)
    
    try:
        # Получаем данные обновления
        data = await request.json()
        from telegram import Update
        update = Update.de_json(data, bot_application.bot)
        
        # Обрабатываем обновление
        await bot_application.process_update(update)
        logger.info("Webhook processed successfully")
        return web.Response(text="OK", status=200)
        
    except Exception as e:
        logger.error(f"Error processing webhook: {e}")
        return web.Response(text="Error", status=500)

async def health_check(request):
    """Проверка здоровья приложения"""
    status = "running" if bot_application else "initializing"
    return web.Response(text=f"Bot is {status}!")

async def init_app():
    """Инициализация веб-приложения"""
    logger.info("Initializing web application...")
    
    # Инициализируем бота
    await init_bot()
    
    app = web.Application()
    
    # Маршруты
    app.router.add_post('/8297051179:AAGHxFTyY2ourq2qmORND-oBN5TaKVYM0uE', handle_webhook)
    app.router.add_get('/health', health_check)
    app.router.add_get('/', health_check)
    
    logger.info("Web application routes configured")
    return app

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    
    async def start_server():
        app = await init_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', port)
        await site.start()
        logger.info(f"🚀 Server started on port {port}")
        logger.info("✅ Bot should be ready to receive webhooks!")
        
        # Бесконечный цикл
        while True:
            await asyncio.sleep(3600)
    
    asyncio.run(start_server())