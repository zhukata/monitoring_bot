import os
import asyncio
import logging
import re
import redis.asyncio as redis

from datetime import datetime, timedelta
from dateutil import parser
from typing import List, Dict, Any, Tuple, Optional
from telethon import TelegramClient
from dotenv import load_dotenv

load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Конфигурация из переменных окружения
api_id = int(os.environ.get("API_ID"))
api_hash = os.environ.get("API_HASH")
phone_number = os.environ.get("PHONE")
my_user_id = int(os.environ.get("MY_USER_ID"))
redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")

# Каналы для мониторинга (добавь свои)
CHANNELS = [
    'https://t.me/turs_sale',
    'https://t.me/vandroukitours',
    'https://t.me/piratesru',
    'https://t.me/travelbelka',
    ''
]

# ========== НАСТРОЙКИ ПОИСКА ==========
# Города вылета (Москва и окрестности)
DEPARTURE_CITIES = [
    "москва",
    "moscow",
    "msk",
    "mow",
    "внуково",
    "vko",
    "шереметьево",
    "svo",
    "домодедово",
    "dme",
    "жуковский",
    "zIA",
]

# Направления (Индия, Гоа)
DESTINATIONS = [
    "индия",
    "india",
    "ind",
    "гоа",
    "goa",
    "goi",
    "дели",
    "delhi",
    "del",
    "мумбаи",
    "mumbai",
    "bom",
    "кожикоде",
    "ccj",
    "calicut",
]

# Целевая дата (с 12 марта 2026)
TARGET_DATE = datetime(2026, 3, 12)
# Диапазон дат: ищем билеты на даты в интервале [TARGET_DATE, TARGET_DATE + 30 дней]
DATE_RANGE_DAYS = 30

# Ключевые слова для поиска (для быстрой фильтрации)
QUICK_KEYWORDS = (
    DEPARTURE_CITIES
    + DESTINATIONS
    + ["индия", "india", "гоа", "goa", "марта", "march"]
)
# =====================================


def clean_channel(channel):
    """Очищает ссылку на канал"""
    if isinstance(channel, str):
        if "t.me/" in channel:
            channel = channel.split("t.me/")[-1]
        channel = channel.lstrip("@")
    return channel


class RedisState:
    # ... (тот же класс, что и в предыдущем ответе)
    def __init__(self, redis_url: str, ttl_days: int = 7):
        self.redis_url = redis_url
        self.ttl_seconds = ttl_days * 24 * 60 * 60
        self.redis = None

    async def connect(self):
        self.redis = await redis.from_url(self.redis_url)
        logger.info("Connected to Redis")

    async def disconnect(self):
        if self.redis:
            await self.redis.close()

    async def get_last_id(self, channel: str) -> int:
        key = f"tg_monitor:last_id:{channel}"
        value = await self.redis.get(key)
        return int(value) if value else 0

    async def set_last_id(self, channel: str, message_id: int):
        key = f"tg_monitor:last_id:{channel}"
        await self.redis.setex(key, self.ttl_seconds, message_id)

    async def is_duplicate(self, channel: str, message_id: int) -> bool:
        key = f"tg_monitor:msg:{channel}:{message_id}"
        return await self.redis.exists(key)

    async def mark_processed(self, channel: str, message_id: int):
        key = f"tg_monitor:msg:{channel}:{message_id}"
        await self.redis.setex(key, self.ttl_seconds, "1")


class FlightSearchAnalyzer:
    """Анализатор сообщений на наличие билетов в Индию"""

    def __init__(self):
        # Компилируем регулярные выражения для скорости
        self.date_patterns = [
            # 12.03, 12/03, 12.03.25, 12/03/2025
            r"\b(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?\b",
            # 12 марта, 12 мар, 12 March
            r"\b(\d{1,2})\s+(марта?|мар|march?|mar)\b",
            # март 12, March 12
            r"\b(март|march|mar)\s+(\d{1,2})\b",
        ]

        # Собираем все паттерны в один для быстрой проверки
        self.date_regex = re.compile(
            "|".join(self.date_patterns), re.IGNORECASE
        )

        # Паттерны для цен
        self.price_pattern = r"\b(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s?(?:руб|р\.?|₽|rub|usd|\$|eur|€)\b"

    def extract_dates(self, text: str) -> List[datetime]:
        """Извлекает все даты из текста"""
        dates = []

        # Ищем даты в формате ДД.ММ или ДД/ММ
        for match in re.finditer(
            r"\b(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?\b", text
        ):
            day, month, year = match.groups()
            day, month = int(day), int(month)

            # Проверяем, что день и месяц адекватные
            if 1 <= day <= 31 and 1 <= month <= 12:
                # Если год не указан, берем 2025
                if year:
                    year = int(year)
                    if year < 100:
                        year += 2000
                else:
                    year = 2025

                try:
                    date = datetime(year, month, day)
                    dates.append(date)
                except ValueError:
                    pass

        # Ищем даты в формате "12 марта"
        months_ru = {
            "январ": 1,
            "феврал": 2,
            "март": 3,
            "апрел": 4,
            "ма": 5,
            "июн": 6,
            "июл": 7,
            "август": 8,
            "сентябр": 9,
            "октябр": 10,
            "ноябр": 11,
            "декабр": 12,
        }

        for month_name, month_num in months_ru.items():
            pattern = rf"(\d{{1,2}})\s+{month_name}[а-я]*"
            for match in re.finditer(pattern, text.lower()):
                day = int(match.group(1))
                try:
                    date = datetime(2025, month_num, day)
                    dates.append(date)
                except ValueError:
                    pass

        return dates

    def extract_cities(self, text: str) -> Tuple[List[str], List[str]]:
        """Извлекает города вылета и назначения"""
        text_lower = text.lower()

        departure_found = []
        destination_found = []

        # Ищем города вылета
        for city in DEPARTURE_CITIES:
            if city in text_lower:
                departure_found.append(city)

        # Ищем направления
        for dest in DESTINATIONS:
            if dest in text_lower:
                destination_found.append(dest)

        return departure_found, destination_found

    def extract_price(self, text: str) -> Optional[int]:
        """Извлекает цену из текста"""
        prices = []
        for match in re.finditer(self.price_pattern, text, re.IGNORECASE):
            price_str = match.group(1).replace(".", "").replace(",", "")
            try:
                price = int(price_str)
                # Фильтруем адекватные цены (от 1000 до 500000 руб)
                if 1000 <= price <= 500000:
                    prices.append(price)
            except ValueError:
                pass

        return min(prices) if prices else None

    def is_relevant(self, text: str) -> Tuple[bool, Dict[str, Any]]:
        """Проверяет, релевантно ли сообщение"""
        if not text:
            return False, {}

        # Быстрая предварительная проверка (должно быть хотя бы одно ключевое слово)
        text_lower = text.lower()
        if not any(keyword in text_lower for keyword in QUICK_KEYWORDS):
            return False, {}

        # Извлекаем данные
        departure_cities, destinations = self.extract_cities(text)
        dates = self.extract_dates(text)
        price = self.extract_price(text)

        # Проверяем, есть ли вылет из Москвы
        has_departure = len(departure_cities) > 0

        # Проверяем, есть ли Индия/Гоа в назначении
        has_destination = len(destinations) > 0

        # Проверяем даты
        relevant_dates = []
        for date in dates:
            # Проверяем, попадает ли дата в нужный диапазон (с 12 марта)
            if (
                TARGET_DATE
                <= date
                <= TARGET_DATE + timedelta(days=DATE_RANGE_DAYS)
            ):
                relevant_dates.append(date)

        has_relevant_date = len(relevant_dates) > 0

        # Сообщение релевантно, если есть направление в Индию и подходящая дата
        # (город вылета может не указываться, но часто подразумевается Москва)
        is_match = has_destination and has_relevant_date

        if is_match:
            logger.info(
                f"MATCH FOUND! Destinations: {destinations}, Dates: {relevant_dates}"
            )

        return is_match, {
            "departure_cities": departure_cities,
            "destinations": destinations,
            "dates": relevant_dates,
            "all_dates": dates,
            "price": price,
            "has_departure": has_departure,
            "has_destination": has_destination,
            "has_relevant_date": has_relevant_date,
        }


async def monitor_channels():
    """Основная функция мониторинга"""
    logger.info("=" * 50)
    logger.info("Starting flight monitoring cycle")
    logger.info(
        f"Looking for flights from Moscow to India/Goa from {TARGET_DATE.strftime('%d.%m.%Y')}"
    )
    logger.info("=" * 50)

    # Инициализация
    state = RedisState(redis_url)
    await state.connect()

    client = TelegramClient("session", api_id, api_hash)
    await client.start(phone=phone_number)

    analyzer = FlightSearchAnalyzer()
    found_messages = []

    for raw_channel in CHANNELS:
        channel = clean_channel(raw_channel)

        try:
            logger.info(f"📡 Checking channel: {channel}")

            # Получаем последние сообщения (увеличим лимит для лучшего поиска)
            last_id = await state.get_last_id(channel)
            messages = []
            async for msg in client.iter_messages(channel, limit=50):
                messages.append(msg)

            # Обрабатываем от старых к новым
            messages.sort(key=lambda x: x.id)
            new_messages = [m for m in messages if m.id > last_id]

            if new_messages:
                logger.info(
                    f"Found {len(new_messages)} new messages in {channel}"
                )

            for msg in new_messages:
                # Проверяем на дубликат
                if await state.is_duplicate(channel, msg.id):
                    continue

                # Анализируем сообщение
                if msg.text:
                    is_match, details = analyzer.is_relevant(msg.text)

                    if is_match:
                        logger.info(
                            f"✅ RELEVANT FLIGHT FOUND in {channel}: ID {msg.id}"
                        )

                        # Форматируем даты для вывода
                        date_str = ", ".join(
                            [d.strftime("%d.%m") for d in details["dates"]]
                        )
                        dest_str = ", ".join(details["destinations"])
                        price_str = (
                            f"{details['price']}₽"
                            if details["price"]
                            else "Цена не указана"
                        )

                        found_messages.append(
                            {
                                "channel": channel,
                                "text": msg.text,
                                "id": msg.id,
                                "date": msg.date,
                                "link": f"https://t.me/{channel}/{msg.id}",
                                "details": details,
                                "summary": f"📅 {date_str} | ✈️ {dest_str} | 💰 {price_str}",
                            }
                        )

                # Отмечаем как обработанное
                await state.mark_processed(channel, msg.id)

            # Обновляем последний ID
            if messages:
                max_id = max(m.id for m in messages)
                await state.set_last_id(channel, max_id)

        except Exception as e:
            logger.error(f"Error checking {channel}: {e}")

    # Отправка результатов
    if found_messages:
        # Группируем по каналам для лучшей читаемости
        by_channel = {}
        for msg in found_messages:
            if msg["channel"] not in by_channel:
                by_channel[msg["channel"]] = []
            by_channel[msg["channel"]].append(msg)

        for channel, messages in by_channel.items():
            # Отправляем сводку по каналу
            summary_text = f"📢 **Найдено {len(messages)} предложений в канале {channel}**\n\n"

            for i, msg in enumerate(messages, 1):
                summary_text += f"{i}. {msg['summary']}\n"
                summary_text += f"   [Ссылка]({msg['link']})\n\n"

            # Если сообщение слишком длинное, разбиваем
            if len(summary_text) > 4000:
                for msg in messages:
                    await client.send_message(
                        my_user_id,
                        f"✈️ **{msg['channel']}**\n\n"
                        f"{msg['summary']}\n\n"
                        f"{msg['text'][:500]}...\n\n"
                        f"[Ссылка на пост]({msg['link']})",
                        parse_mode="md",
                    )
            else:
                await client.send_message(
                    my_user_id, summary_text, parse_mode="md"
                )

            logger.info(f"Sent {len(messages)} matches from {channel}")
    else:
        logger.info("No matches found in this cycle")
        await client.send_message(
            my_user_id,
            f"🔍 Мониторинг завершен: новых предложений Москва→Индия после {TARGET_DATE.strftime('%d.%m')} не найдено",
        )

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


if __name__ == "__main__":
    asyncio.run(main())
