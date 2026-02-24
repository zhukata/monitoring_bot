import os
import asyncio
import logging
import re
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional
from telethon import TelegramClient
import redis.asyncio as redis
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
phone_number = os.environ.get("PHONE_NUMBER")
my_user_id = int(os.environ.get("MY_USER_ID"))
redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")

# Каналы для мониторинга (УБЕДИСЬ, ЧТО НЕТ ПУСТЫХ СТРОК!)
CHANNELS = [
    "https://t.me/turs_sale",
    "https://t.me/vandroukitours",
    "https://t.me/piratesru",
    "https://t.me/travelbelka",
    "https://t.me/nachemodanah",
]

# ========== НАСТРОЙКИ ПОИСКА ==========
# Города вылета
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
    "zia",
]

# Направления
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
]

# Целевой месяц - МАРТ 2026
TARGET_MONTH = 3  # Март
TARGET_YEAR = 2026

# Если дата не указана, всё равно присылаем? (True = присылаем даже без дат)
SEND_IF_NO_DATE = True

# Ключевые слова для быстрой фильтрации
QUICK_KEYWORDS = (
    DEPARTURE_CITIES + DESTINATIONS + ["индия", "india", "гоа", "goa"]
)
# =====================================


def clean_channel(channel):
    """Очищает ссылку на канал"""
    if not channel:  # Пропускаем пустые каналы
        return None
    if isinstance(channel, str):
        if "t.me/" in channel:
            channel = channel.split("t.me/")[-1]
        channel = channel.lstrip("@")
    return channel


class RedisState:
    """Управление состоянием в Redis"""

    def __init__(self, redis_url: str, ttl_days: int = 7):
        self.redis_url = redis_url
        self.ttl_seconds = ttl_days * 24 * 60 * 60
        self.redis = None

    async def connect(self):
        self.redis = await redis.from_url(self.redis_url)
        logger.info("Connected to Redis")

    async def disconnect(self):
        if self.redis:
            await self.redis.aclose()  # Исправлено: aclose() вместо close()

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
        # Компилируем регулярные выражения
        self.date_patterns = [
            # 05.03.26, 05.03.2026, 05/03/26
            r"(\d{1,2})[./](\d{1,2})[./](\d{2,4})",
            # 05.03, 05/03 (без года)
            r"(\d{1,2})[./](\d{1,2})(?![./\d])",
            # 5 марта, 05 марта, 5 мар, 05 мар
            r"(\d{1,2})\s+(марта?|мар|march?|mar)\b",
            # март 5, March 5
            r"(март|march|mar)\s+(\d{1,2})\b",
        ]

        # Паттерны для цен - расширенные
        self.price_patterns = [
            r"(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s?(?:руб|р\.?|₽)\b",  # 74300P, 51.400 руб
            r"(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s?(?:usd|\$)",  # 14000 рублей (но мы уже взяли рубли)
            r"за\s+(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s*(?:руб|р\.?|₽)",  # за 74300P
            r"(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s*р(?!уб)",  # 74300р
        ]

        # Для быстрой проверки наличия дат вообще
        self.has_date_pattern = re.compile(
            r"\d{1,2}[./]\d{1,2}|\d{1,2}\s+(мар|апр|май|июн|июл|авг|сен|окт|ноя|дек)|(янв|фев|мар|апр|май|июн|июл|авг|сен|окт|ноя|дек)\s+\d{1,2}",
            re.IGNORECASE,
        )

    def extract_dates(self, text: str) -> List[Dict]:
        """Извлекает все даты из текста и определяет их месяц"""
        dates_info = []

        if not text:
            return dates_info

        text_lower = text.lower()

        # 1. Ищем даты в формате ДД.ММ.ГГ или ДД/ММ/ГГГГ
        for match in re.finditer(r"(\d{1,2})[./](\d{1,2})[./](\d{2,4})", text):
            day, month, year = match.groups()
            day, month = int(day), int(month)

            # Нормализуем год
            if len(year) == 2:
                year = 2000 + int(year)
            else:
                year = int(year)

            if 1 <= month <= 12 and 1 <= day <= 31:
                dates_info.append(
                    {
                        "day": day,
                        "month": month,
                        "year": year,
                        "full_date": (
                            datetime(year, month, day)
                            if year and month and day
                            else None
                        ),
                        "original": match.group(0),
                    }
                )

        # 2. Ищем даты в формате ДД.ММ (без года)
        for match in re.finditer(r"(\d{1,2})[./](\d{1,2})(?![./\d])", text):
            day, month = match.groups()
            day, month = int(day), int(month)

            if 1 <= month <= 12 and 1 <= day <= 31:
                dates_info.append(
                    {
                        "day": day,
                        "month": month,
                        "year": TARGET_YEAR,  # Предполагаем целевой год
                        "full_date": (
                            datetime(TARGET_YEAR, month, day)
                            if month and day
                            else None
                        ),
                        "original": match.group(0),
                    }
                )

        # 3. Ищем даты в формате "5 марта"
        months_ru = {
            "январ": 1,
            "феврал": 2,
            "март": 3,
            "апрел": 4,
            "мая": 5,
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
            for match in re.finditer(pattern, text_lower):
                day = int(match.group(1))
                if 1 <= day <= 31:
                    dates_info.append(
                        {
                            "day": day,
                            "month": month_num,
                            "year": TARGET_YEAR,
                            "full_date": datetime(TARGET_YEAR, month_num, day),
                            "original": match.group(0),
                        }
                    )

        # 4. Ищем даты в формате "март 5"
        for month_name, month_num in months_ru.items():
            pattern = rf"{month_name}[а-я]*\s+(\d{{1,2}})"
            for match in re.finditer(pattern, text_lower):
                day = int(match.group(1))
                if 1 <= day <= 31:
                    dates_info.append(
                        {
                            "day": day,
                            "month": month_num,
                            "year": TARGET_YEAR,
                            "full_date": datetime(TARGET_YEAR, month_num, day),
                            "original": match.group(0),
                        }
                    )

        # Убираем дубликаты (оставляем уникальные даты)
        unique_dates = []
        seen = set()
        for d in dates_info:
            key = f"{d.get('day')}-{d.get('month')}-{d.get('year')}"
            if key not in seen and d.get("day") and d.get("month"):
                seen.add(key)
                unique_dates.append(d)

        return unique_dates

    def extract_cities(self, text: str) -> Tuple[List[str], List[str]]:
        """Извлекает города вылета и назначения"""
        if not text:
            return [], []

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
        """Извлекает цену из текста (улучшенная версия)"""
        if not text:
            return None

        prices = []

        for pattern in self.price_patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                price_str = match.group(1)
                # Очищаем от пробелов и заменяем запятые
                price_str = re.sub(r"\s+", "", price_str)
                price_str = price_str.replace(",", ".").replace(" ", "")

                try:
                    # Если есть точка - это десятичный разделитель
                    if "." in price_str:
                        price = int(float(price_str))
                    else:
                        price = int(price_str)

                    # Фильтруем адекватные цены на авиабилеты
                    if 1000 <= price <= 500000:
                        prices.append(price)
                except ValueError:
                    continue

        # Также ищем цены без явного указания валюты, но с "за" и числом
        for match in re.finditer(
            r"за\s+(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)", text, re.IGNORECASE
        ):
            try:
                price_str = match.group(1).replace(",", "").replace(".", "")
                price = int(price_str)
                if 1000 <= price <= 500000:
                    prices.append(price)
            except ValueError:
                pass

        return min(prices) if prices else None

    def has_any_date(self, text: str) -> bool:
        """Проверяет, есть ли в тексте вообще какие-то даты"""
        return bool(self.has_date_pattern.search(text))

    def is_month_match(self, date_info: Dict) -> bool:
        """Проверяет, соответствует ли дата целевому месяцу"""
        return date_info.get("month") == TARGET_MONTH

    def is_relevant(self, text: str) -> Tuple[bool, Dict[str, Any]]:
        """
        Проверяет, релевантно ли сообщение
        Возвращает (релевантно, детали)
        """
        if not text:
            return False, {}

        # Быстрая предварительная проверка
        text_lower = text.lower()
        if not any(keyword in text_lower for keyword in QUICK_KEYWORDS):
            return False, {}

        # Извлекаем данные
        departure_cities, destinations = self.extract_cities(text)
        all_dates = self.extract_dates(text)
        price = self.extract_price(text)

        # Проверяем наличие Индии/Гоа
        has_destination = len(destinations) > 0
        if not has_destination:
            return False, {}

        # Анализируем даты
        target_month_dates = []
        other_dates = []

        for date_info in all_dates:
            if self.is_month_match(date_info):
                target_month_dates.append(date_info)
            else:
                other_dates.append(date_info)

        has_target_month_date = len(target_month_dates) > 0
        has_any_date_in_text = len(all_dates) > 0

        # Логика принятия решения:
        # 1. Если есть даты в целевом месяце - ОК
        # 2. Если дат нет вообще, но SEND_IF_NO_DATE=True - ОК
        # 3. Если даты есть, но ни одна не в целевом месяце - НЕ ОК

        if has_target_month_date:
            # Есть даты в марте - отлично!
            is_match = True
            reason = "target_month_match"
        elif not has_any_date_in_text and SEND_IF_NO_DATE:
            # Дат нет, но мы хотим получать такие сообщения
            is_match = True
            reason = "no_dates"
        elif has_any_date_in_text and not has_target_month_date:
            # Даты есть, но не март - пропускаем
            is_match = False
            reason = "wrong_month"
        else:
            # На всякий случай
            is_match = False
            reason = "unknown"

        if is_match:
            logger.info(
                f"MATCH ({reason}): Destinations: {destinations}, Dates in target: {len(target_month_dates)}"
            )

        return is_match, {
            "departure_cities": departure_cities,
            "destinations": destinations,
            "all_dates": all_dates,
            "target_month_dates": target_month_dates,
            "price": price,
            "has_destination": has_destination,
            "has_target_month_date": has_target_month_date,
            "reason": reason,
        }


async def monitor_channels():
    """Основная функция мониторинга"""
    logger.info("=" * 50)
    logger.info("Starting flight monitoring cycle")
    logger.info(
        f"Looking for flights from Moscow to India/Goa in March {TARGET_YEAR}"
    )
    logger.info(f"SEND_IF_NO_DATE = {SEND_IF_NO_DATE}")
    logger.info("=" * 50)

    # Инициализация
    state = RedisState(redis_url)
    await state.connect()

    client = TelegramClient("session", api_id, api_hash)
    await client.start(phone=phone_number)

    analyzer = FlightSearchAnalyzer()
    found_messages = []

    # Фильтруем пустые каналы
    valid_channels = []
    for ch in CHANNELS:
        cleaned = clean_channel(ch)
        if cleaned:
            valid_channels.append(cleaned)
        else:
            logger.warning(f"Skipping empty channel: '{ch}'")

    for channel in valid_channels:
        try:
            logger.info(f"📡 Checking channel: {channel}")

            # Получаем последние сообщения
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
                        if details["target_month_dates"]:
                            date_str = ", ".join(
                                [
                                    f"{d['day']:02d}.{d['month']:02d}"
                                    for d in details["target_month_dates"]
                                ]
                            )
                        else:
                            date_str = "дата не указана"

                        dest_str = ", ".join(details["destinations"])
                        price_str = (
                            f"{details['price']:,}₽".replace(",", " ")
                            if details["price"]
                            else "цена не указана"
                        )

                        # Короткий превью текста
                        preview = (
                            msg.text[:200] + "..."
                            if len(msg.text) > 200
                            else msg.text
                        )

                        found_messages.append(
                            {
                                "channel": channel,
                                "text": msg.text,
                                "preview": preview,
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
        # Группируем по каналам
        by_channel = {}
        for msg in found_messages:
            if msg["channel"] not in by_channel:
                by_channel[msg["channel"]] = []
            by_channel[msg["channel"]].append(msg)

        for channel, messages in by_channel.items():
            # Отправляем каждое сообщение отдельно (так надежнее)
            for msg in messages:
                # Формируем красивое сообщение
                header = f"✈️ **{channel}**\n"
                header += f"_{msg['summary']}_\n\n"

                # Добавляем превью текста
                full_text = (
                    header
                    + msg["preview"]
                    + f"\n\n[👉 Открыть пост]({msg['link']})"
                )

                await client.send_message(
                    my_user_id,
                    full_text,
                    parse_mode="md",
                    link_preview=False,  # Не показываем превью ссылок
                )

            logger.info(f"Sent {len(messages)} matches from {channel}")
    else:
        logger.info("No matches found in this cycle")
        await client.send_message(
            my_user_id,
            f"🔍 Мониторинг завершен: новых предложений Москва→Индия на март {TARGET_YEAR} не найдено",
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
