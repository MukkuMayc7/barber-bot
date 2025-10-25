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

# Глобальная переменная для приложения
application = None

async def init_bot():
    """Инициализация бота"""
    global application
    try:
        from telegram.ext import Application
        from bot import setup_webhook
        import config
        
        application = Application.builder().token(config.BOT_TOKEN).build()
        await setup_webhook(application)
        logger.info("Bot initialized successfully with webhook")
        return True
    except Exception as e:
        logger.error(f"Error initializing bot: {e}")
        return False

async def handle_webhook(request):
    """Обработчик вебхука для Render"""
    global application
    
    if application is None:
        success = await init_bot()
        if not success:
            return web.Response(text="Bot initialization failed", status=500)
    
    try:
        # Получаем данные обновления
        data = await request.json()
        from telegram import Update
        update = Update.de_json(data, application.bot)
        
        # Обрабатываем обновление
        await application.process_update(update)
        return web.Response(text="OK", status=200)
    except Exception as e:
        logger.error(f"Ошибка обработки вебхука: {e}")
        return web.Response(text="Error", status=500)

async def health_check(request):
    """Проверка здоровья приложения"""
    return web.Response(text="Bot is running!")

async def init_app():
    """Инициализация приложения"""
    # Инициализируем бота при старте
    await init_bot()
    
    app = web.Application()
    
    # Маршруты
    app.router.add_post(f'/8297051179:AAGHxFTyY2ourq2qmORND-oBN5TaKVYM0uE', handle_webhook)
    app.router.add_get('/health', health_check)
    app.router.add_get('/', health_check)
    
    return app

if __name__ == '__main__':
    # Запуск сервера
    port = int(os.environ.get('PORT', 10000))
    
    async def start_server():
        app = await init_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', port)
        await site.start()
        print(f"Server started on port {port}")
        print("Bot should be running with webhook...")
        
        # Бесконечный цикл для поддержания работы
        while True:
            await asyncio.sleep(3600)
    
    asyncio.run(start_server())