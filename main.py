import os
import asyncio
import logging
from datetime import datetime
from telethon import TelegramClient
import redis.asyncio as redis

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация из переменных окружения
api_id = int(os.environ.get('API_ID'))
api_hash = os.environ.get('API_HASH')
phone_number = os.environ.get('PHONE_NUMBER')
my_user_id = int(os.environ.get('MY_USER_ID'))
redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379')

# Каналы в любом формате
CHANNELS = [
    'https://t.me/turs_sale',
    'https://t.me/vandroukitours',
    'https://t.me/piratesru',
]

# Ключевые слова (простые, без regex)
KEYWORDS = ['гоа', 'goa', 'индия', 'india', 'goi']

def clean_channel(channel):
    """Очищает ссылку на канал"""
    if isinstance(channel, str):
        if 't.me/' in channel:
            channel = channel.split('t.me/')[-1]
        channel = channel.lstrip('@')
    return channel

def check_keywords(text: str) -> bool:
    """Проверяет наличие ключевых слов"""
    if not text:
        return False
    text_lower = text.lower()
    return any(word in text_lower for word in KEYWORDS)

class RedisState:
    def __init__(self, redis_url: str, ttl_days: int = 7):
        self.redis_url = redis_url
        self.ttl_seconds = ttl_days * 24 * 60 * 60
        self.redis = None
    
    async def connect(self):
        """Подключение к Redis"""
        self.redis = await redis.from_url(self.redis_url)
        logger.info("Connected to Redis")
    
    async def disconnect(self):
        """Отключение от Redis"""
        if self.redis:
            await self.redis.close()
    
    async def get_last_id(self, channel: str) -> int:
        """Получить последний обработанный ID"""
        key = f"tg_monitor:last_id:{channel}"
        value = await self.redis.get(key)
        return int(value) if value else 0
    
    async def set_last_id(self, channel: str, message_id: int):
        """Сохранить последний ID с TTL"""
        key = f"tg_monitor:last_id:{channel}"
        await self.redis.setex(key, self.ttl_seconds, message_id)
    
    async def is_duplicate(self, channel: str, message_id: int) -> bool:
        """Проверка на дубликат сообщения"""
        key = f"tg_monitor:msg:{channel}:{message_id}"
        return await self.redis.exists(key)
    
    async def mark_processed(self, channel: str, message_id: int):
        """Отметить сообщение как обработанное"""
        key = f"tg_monitor:msg:{channel}:{message_id}"
        await self.redis.setex(key, self.ttl_seconds, "1")

async def monitor_channels():
    """Основная функция мониторинга"""
    logger.info("Starting monitoring cycle")
    
    # Подключение к Redis
    state = RedisState(redis_url)
    await state.connect()
    
    # Подключение к Telegram
    client = TelegramClient('session', api_id, api_hash)
    await client.start(phone=phone_number)
    
    found_messages = []
    
    for raw_channel in CHANNELS:
        channel = clean_channel(raw_channel)
        
        try:
            logger.info(f"Checking channel: {channel}")
            
            # Получаем последний обработанный ID
            last_id = await state.get_last_id(channel)
            logger.info(f"Last processed ID: {last_id}")
            
            # Получаем новые сообщения
            messages = []
            async for msg in client.iter_messages(channel, limit=30):
                messages.append(msg)
            
            # Обрабатываем от старых к новым
            messages.sort(key=lambda x: x.id)
            
            for msg in messages:
                # Пропускаем старые сообщения
                if msg.id <= last_id:
                    continue
                
                # Проверяем на дубликат
                if await state.is_duplicate(channel, msg.id):
                    continue
                
                # Проверяем ключевые слова
                if msg.text and check_keywords(msg.text):
                    logger.info(f"Match found: {channel} - ID {msg.id}")
                    found_messages.append({
                        'channel': channel,
                        'text': msg.text,
                        'id': msg.id,
                        'date': msg.date.isoformat(),
                        'link': f"https://t.me/{channel}/{msg.id}"
                    })
                
                # Отмечаем как обработанное
                await state.mark_processed(channel, msg.id)
            
            # Обновляем последний ID
            if messages:
                max_id = max(m.id for m in messages)
                await state.set_last_id(channel, max_id)
                logger.info(f"Updated last ID for {channel}: {max_id}")
                
        except Exception as e:
            logger.error(f"Error checking {channel}: {e}")
    
    # Отправка результатов
    if found_messages:
        for msg in found_messages:
            try:
                await client.send_message(
                    my_user_id,
                    f"🔍 **Найдено в {msg['channel']}**\n\n"
                    f"{msg['text'][:500]}...\n\n"
                    f"[Ссылка на пост]({msg['link']})",
                    parse_mode='md'
                )
                logger.info(f"Sent message {msg['id']} to user")
            except Exception as e:
                logger.error(f"Error sending message: {e}")
    else:
        logger.info("No matches found")
    
    # Закрываем соединения
    await state.disconnect()
    await client.disconnect()
    logger.info("Monitoring cycle completed")

async def main():
    """Точка входа"""
    try:
        await monitor_channels()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise

if __name__ == '__main__':
    asyncio.run(main())