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
    """Обработчик вебхука для Telegram"""
    try:
        # Импортируем здесь чтобы избежать циклических импортов
        from telegram import Update
        from telegram.ext import Application
        
        # Создаем приложение
        bot_token = os.getenv('BOT_TOKEN', '8297051179:AAGHxFTyY2ourq2qmORND-oBN5TaKVYM0uE')
        application = Application.builder().token(bot_token).build()
        
        # Настраиваем обработчики
        from bot import setup_handlers
        setup_handlers(application)
        
        # Обрабатываем обновление
        data = await request.json()
        update = Update.de_json(data, application.bot)
        await application.process_update(update)
        
        logger.info("Webhook processed successfully")
        return web.Response(text="OK", status=200)
        
    except Exception as e:
        logger.error(f"Error processing webhook: {e}")
        return web.Response(text="Error", status=500)

async def health_check(request):
    """Проверка здоровья приложения"""
    return web.Response(text="Bot is running!")

async def init_app():
    """Инициализация приложения"""
    app = web.Application()
    
    # Маршруты
    app.router.add_post('/8297051179:AAGHxFTyY2ourq2qmORND-oBN5TaKVYM0uE', handle_webhook)
    app.router.add_get('/health', health_check)
    app.router.add_get('/', health_check)
    
    logger.info("Web application initialized")
    return app

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    
    async def start_server():
        app = await init_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', port)
        await site.start()
        logger.info(f"Server started on port {port}")
        
        # Бесконечный цикл
        while True:
            await asyncio.sleep(3600)
    
    asyncio.run(start_server())