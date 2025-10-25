# app.py - SIMPLE WORKING VERSION
import os
import asyncio
from aiohttp import web
import logging
import aiohttp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация - ВСТАВЛЯЕМ НАПРЯМУЮ
BOT_TOKEN = "8297051179:AAGHxFTyY2ourq2qmORND-oBN5TaKVYM0uE"
WEBHOOK_URL = "https://barber-bot-render.onrender.com"
WEBHOOK_PATH = f"/{BOT_TOKEN}"

# Глобальная переменная для отслеживания состояния
bot_initialized = False

async def initialize_bot():
    """Инициализация бота - ВСЕ В ОДНОЙ ФУНКЦИИ"""
    global bot_initialized
    
    try:
        logger.info("🚀 STARTING BOT INITIALIZATION...")
        
        # 1. Устанавливаем вебхук
        setwebhook_url = f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook"
        webhook_data = {
            "url": f"{WEBHOOK_URL}{WEBHOOK_PATH}",
            "drop_pending_updates": True
        }
        
        logger.info(f"🔧 Setting webhook to: {WEBHOOK_URL}{WEBHOOK_PATH}")
        
        async with aiohttp.ClientSession() as session:
            async with session.post(setwebhook_url, data=webhook_data) as response:
                result = await response.json()
                if result.get('ok'):
                    logger.info("✅ WEBHOOK SET SUCCESSFULLY!")
                else:
                    logger.error(f"❌ WEBHOOK FAILED: {result}")
                    return False
        
        # 2. Проверяем что вебхук установился
        getwebhook_url = f"https://api.telegram.org/bot{BOT_TOKEN}/getWebhookInfo"
        async with aiohttp.ClientSession() as session:
            async with session.get(getwebhook_url) as response:
                status = await response.json()
                logger.info(f"🔍 WEBHOOK STATUS: {status}")
        
        bot_initialized = True
        logger.info("🎉 BOT INITIALIZATION COMPLETED!")
        return True
        
    except Exception as e:
        logger.error(f"💥 BOT INITIALIZATION FAILED: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

async def handle_webhook(request):
    """Обработчик вебхука"""
    global bot_initialized
    
    if not bot_initialized:
        logger.warning("⚠️ Bot not initialized, but received webhook")
        return web.Response(text="Bot initializing", status=503)
    
    try:
        # Получаем данные
        data = await request.json()
        logger.info(f"📨 Received message")
        
        # Простая обработка
        if 'message' in data and 'text' in data['message']:
            text = data['message']['text']
            chat_id = data['message']['chat']['id']
            
            response_text = "❌ Бот не настроен"
            
            if text == '/start':
                response_text = "🎉 БОТ РАБОТАЕТ! Вы отправили /start"
            elif 'привет' in text.lower():
                response_text = "👋 Привет! Я бот парикмахерской 'Бархат'"
            else:
                response_text = f"🤖 Вы написали: {text}"
            
            # Отправляем ответ
            await send_telegram_message(chat_id, response_text)
        
        return web.Response(text="OK")
        
    except Exception as e:
        logger.error(f"❌ Webhook error: {e}")
        return web.Response(text="Error", status=500)

async def send_telegram_message(chat_id, text):
    """Отправка сообщения в Telegram"""
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        data = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=data) as response:
                result = await response.json()
                if result.get('ok'):
                    logger.info(f"✅ Message sent to {chat_id}")
                else:
                    logger.error(f"❌ Failed to send message: {result}")
                    
    except Exception as e:
        logger.error(f"❌ Send message error: {e}")

async def health_check(request):
    """Проверка здоровья"""
    status = "RUNNING" if bot_initialized else "INITIALIZING"
    return web.Response(text=f"Bot is {status}!")

async def init_app():
    """Инициализация приложения"""
    logger.info("🌐 INITIALIZING WEB APPLICATION...")
    
    # Инициализируем бота СРАЗУ при старте
    await initialize_bot()
    
    # Создаем приложение
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
        logger.info("🎯 BOT IS READY FOR WEBHOOKS!")
        
        # Бесконечный цикл
        while True:
            await asyncio.sleep(3600)
    
    # Запускаем сервер
    asyncio.run(start_server())