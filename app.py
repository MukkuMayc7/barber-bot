# app.py
import os
import asyncio
from aiohttp import web
import logging

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

async def handle_webhook(request):
    """Обработчик вебхука от Telegram"""
    try:
        # Динамический импорт чтобы избежать ошибок при старте
        from telegram import Update
        from telegram.ext import Application
        import config
        
        # Создаем приложение
        application = Application.builder().token(config.BOT_TOKEN).build()
        
        # Настраиваем обработчики
        from bot import setup_handlers
        setup_handlers(application)
        
        # Обрабатываем обновление
        data = await request.json()
        update = Update.de_json(data, application.bot)
        await application.process_update(update)
        
        logger.info("✅ Webhook processed successfully")
        return web.Response(text="OK", status=200)
        
    except Exception as e:
        logger.error(f"❌ Error processing webhook: {e}")
        return web.Response(text="Error", status=500)

async def health_check(request):
    """Проверка здоровья приложения"""
    return web.Response(text="Bot server is running!")

async def setup_webhook():
    """Настройка вебхука при старте"""
    try:
        from telegram.ext import Application
        import config
        
        if config.WEBHOOK_URL:
            application = Application.builder().token(config.BOT_TOKEN).build()
            webhook_url = f"{config.WEBHOOK_URL}/8297051179:AAGHxFTyY2ourq2qmORND-oBN5TaKVYM0uE"
            
            await application.bot.set_webhook(webhook_url, drop_pending_updates=True)
            logger.info(f"✅ Webhook set to: {webhook_url}")
        else:
            logger.warning("⚠️ WEBHOOK_URL not set")
            
    except Exception as e:
        logger.error(f"❌ Failed to setup webhook: {e}")

async def init_app():
    """Инициализация веб-приложения"""
    logger.info("🌐 Initializing web application...")
    
    # Настраиваем вебхук при старте
    await setup_webhook()
    
    app = web.Application()
    
    # Маршруты
    app.router.add_post('/8297051179:AAGHxFTyY2ourq2qmORND-oBN5TaKVYM0uE', handle_webhook)
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