# app.py
import os
import asyncio
from aiohttp import web
import logging
import sys

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Глобальная переменная для бота
application = None

async def setup_bot():
    """Настройка бота и вебхука"""
    global application
    try:
        from telegram.ext import Application
        from bot import setup_handlers
        
        BOT_TOKEN = os.getenv('BOT_TOKEN', '8297051179:AAGHxFTyY2ourq2qmORND-oBN5TaKVYM0uE')
        WEBHOOK_URL = os.getenv('WEBHOOK_URL', 'https://barber-bot-render.onrender.com')
        
        logger.info(f"🔧 Initializing bot with token: {BOT_TOKEN[:10]}...")
        logger.info(f"🔧 Webhook URL: {WEBHOOK_URL}")
        
        # Создаем приложение
        application = Application.builder().token(BOT_TOKEN).build()
        
        # Настраиваем обработчики
        setup_handlers(application)
        
        # Настраиваем вебхук
        webhook_url = f"{WEBHOOK_URL}/{BOT_TOKEN}"
        await application.bot.set_webhook(webhook_url, drop_pending_updates=True)
        logger.info(f"✅ Webhook set to: {webhook_url}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to setup bot: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

async def handle_webhook(request):
    """Обработчик вебхука от Telegram"""
    global application
    
    if application is None:
        logger.error("❌ Bot not initialized!")
        return web.Response(text="Bot not ready", status=503)
    
    try:
        # Получаем данные обновления
        data = await request.json()
        from telegram import Update
        update = Update.de_json(data, application.bot)
        
        # Обрабатываем обновление
        await application.process_update(update)
        logger.info("✅ Webhook processed successfully")
        return web.Response(text="OK", status=200)
        
    except Exception as e:
        logger.error(f"❌ Error processing webhook: {e}")
        return web.Response(text="Error", status=500)

async def health_check(request):
    """Проверка здоровья приложения"""
    status = "RUNNING" if application else "INITIALIZING"
    return web.Response(text=f"Bot is {status}!")

async def init_app():
    """Инициализация веб-приложения"""
    logger.info("🌐 Initializing web application...")
    
    # Настраиваем бота
    await setup_bot()
    
    app = web.Application()
    
    # Маршруты
    BOT_TOKEN = os.getenv('BOT_TOKEN', '8297051179:AAGHxFTyY2ourq2qmORND-oBN5TaKVYM0uE')
    app.router.add_post(f'/{BOT_TOKEN}', handle_webhook)
    app.router.add_get('/health', health_check)
    app.router.add_get('/', health_check)
    
    logger.info("✅ Web application ready")
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
        logger.info("🎯 Bot is ready to receive webhooks!")
        
        # Бесконечный цикл
        while True:
            await asyncio.sleep(3600)
    
    asyncio.run(start_server())