# app.py - SIMPLE WORKING VERSION
import os
import asyncio
from aiohttp import web
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация
BOT_TOKEN = "8297051179:AAGHxFTyY2ourq2qmORND-oBN5TaKVYM0uE"
WEBHOOK_URL = "https://barber-bot-render.onrender.com"
WEBHOOK_PATH = f"/{BOT_TOKEN}"

async def setup_webhook():
    """Настройка вебхука при старте"""
    try:
        import aiohttp
        
        # Устанавливаем вебхук через Telegram API
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook"
        data = {
            "url": f"{WEBHOOK_URL}{WEBHOOK_PATH}",
            "drop_pending_updates": True
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=data) as response:
                result = await response.json()
                if result.get('ok'):
                    logger.info(f"✅ WEBHOOK SET: {WEBHOOK_URL}{WEBHOOK_PATH}")
                else:
                    logger.error(f"❌ WEBHOOK FAILED: {result}")
                    
    except Exception as e:
        logger.error(f"❌ Webhook setup error: {e}")

async def handle_webhook(request):
    """Обработчик вебхука"""
    try:
        # Получаем обновление
        data = await request.json()
        logger.info(f"📨 Received update: {data}")
        
        # Здесь будет обработка сообщений
        if 'message' in data and 'text' in data['message']:
            text = data['message']['text']
            chat_id = data['message']['chat']['id']
            
            # Простой ответ
            if text == '/start':
                await send_message(chat_id, "👋 Привет! Я бот парикмахерской 'Бархат'!")
            else:
                await send_message(chat_id, "🤖 Бот работает! Но функционал пока ограничен.")
        
        return web.Response(text="OK")
        
    except Exception as e:
        logger.error(f"❌ Webhook error: {e}")
        return web.Response(text="Error", status=500)

async def send_message(chat_id, text):
    """Отправка сообщения"""
    try:
        import aiohttp
        
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        data = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=data) as response:
                await response.json()
                
    except Exception as e:
        logger.error(f"❌ Send message error: {e}")

async def health_check(request):
    return web.Response(text="✅ Bot Server is RUNNING!")

async def init_app():
    """Инициализация приложения"""
    logger.info("🚀 Starting bot server...")
    
    # Настраиваем вебхук при старте
    await setup_webhook()
    
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    app.router.add_get('/health', health_check)
    app.router.add_get('/', health_check)
    
    return app

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    
    async def start():
        app = await init_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', port)
        await site.start()
        logger.info(f"🎯 Server running on port {port}")
        
        # Keep alive
        while True:
            await asyncio.sleep(3600)
    
    asyncio.run(start())