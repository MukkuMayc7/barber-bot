# app.py - FIXED VERSION WITH ERROR HANDLING
import os
import asyncio
from aiohttp import web
import logging
import traceback

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = "8297051179:AAGHxFTyY2ourq2qmORND-oBN5TaKVYM0uE"
WEBHOOK_URL = "https://barber-bot-render.onrender.com"
WEBHOOK_PATH = f"/{BOT_TOKEN}"

bot_initialized = False
application = None

async def initialize_bot():
    global bot_initialized, application
    try:
        logger.info("🚀 STARTING BOT INITIALIZATION...")
        
        # 1. База данных
        logger.info("📦 Testing database...")
        import database
        db = database.Database()
        logger.info("✅ Database OK")
        
        # 2. Telegram бот
        logger.info("🤖 Testing Telegram bot...")
        from telegram.ext import Application
        application = Application.builder().token(BOT_TOKEN).build()
        logger.info("✅ Telegram bot OK")
        
        # 3. Обработчики
        logger.info("⚙️ Setting up handlers...")
        from bot import setup_handlers
        setup_handlers(application)
        logger.info("✅ Handlers setup OK")
        
        # 4. Вебхук (с обработкой ошибок)
        logger.info("🌐 Setting webhook...")
        try:
            await application.bot.set_webhook(
                url=f"{WEBHOOK_URL}{WEBHOOK_PATH}",
                drop_pending_updates=True
            )
            logger.info("✅ Webhook set successfully")
        except Exception as e:
            logger.error(f"❌ Webhook failed: {e}")
            # Продолжаем даже если вебхук не установился
            logger.info("🔄 Continuing without webhook...")
        
        bot_initialized = True
        logger.info("🎉 Bot fully initialized!")
        
    except Exception as e:
        logger.error(f"💥 INITIALIZATION FAILED: {e}")
        logger.error(f"TRACEBACK: {traceback.format_exc()}")

async def handle_webhook(request):
    global bot_initialized, application
    if not bot_initialized:
        logger.warning("⏳ Bot not initialized yet")
        return web.Response(text="Bot initializing", status=503)
    
    try:
        data = await request.json()
        from telegram import Update
        update = Update.de_json(data, application.bot)
        await application.process_update(update)
        logger.info("✅ Webhook processed")
        return web.Response(text="OK")
    except Exception as e:
        logger.error(f"❌ Webhook error: {e}")
        return web.Response(text="Error", status=500)

async def health_check(request):
    global bot_initialized
    status = "RUNNING" if bot_initialized else "INITIALIZING"
    return web.Response(text=f"Bot status: {status}")

async def main():
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    app.router.add_get('/health', health_check)
    app.router.add_get('/', health_check)
    
    # Запускаем инициализацию
    asyncio.create_task(initialize_bot())
    
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('PORT', 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    
    logger.info(f"🚀 Server started on port {port}")
    logger.info("📱 Waiting for bot initialization...")
    
    # Бесконечный цикл
    await asyncio.Future()

if __name__ == '__main__':
    asyncio.run(main())