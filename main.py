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

# Каналы для мониторинга
CHANNELS = [
    "turs_sale",
    "vandroukitours",
    "piratesru",
    "travelbelka",
    "nachemodanah",
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

# Направления - ТОЛЬКО ПОЛНЫЕ СЛОВА!
DESTINATIONS = [
    r"\bиндия\b",
    r"\bindia\b",
    r"\bгоа\b",
    r"\bgoa\b",
    r"\bдели\b",
    r"\bdelhi\b",
    r"\bdel\b",
    r"\bмумбаи\b",
    r"\bmumbai\b",
    r"\bbom\b",
    r"\bкожикоде\b",
    r"\bcalicut\b",
    r"\bccj\b",
]

# Составляем общий паттерн для поиска
DEST_PATTERN = re.compile("|".join(DESTINATIONS), re.IGNORECASE)

# Целевой месяц - МАРТ 2026
TARGET_MONTH = 3
TARGET_YEAR = 2026

# Если дата не указана, всё равно присылаем
SEND_IF_NO_DATE = True

# Минимальная длина текста для проверки (отсекаем слишком короткие)
MIN_TEXT_LENGTH = 50
# =====================================


def clean_channel(channel):
    """Очищает ссылку на канал"""
    if not channel:
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
            await self.redis.aclose()

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
        # Паттерны для дат
        self.date_patterns = [
            # 05.03.26, 05.03.2026
            r"(\d{1,2})[./](\d{1,2})[./](\d{2,4})",
            # 05.03
            r"(\d{1,2})[./](\d{1,2})(?![./\d])",
            # 5 марта, 05 марта
            r"(\d{1,2})\s+(марта?|мар|march?|mar)\b",
            # март 5
            r"(март|march|mar)\s+(\d{1,2})\b",
        ]

        # Паттерны для цен
        self.price_patterns = [
            r"(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s?(?:руб|р\.?|₽)\b",
            r"за\s+(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s*(?:руб|р\.?|₽)",
            r"(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s*р(?!уб)",
        ]

        # Для быстрой проверки наличия дат
        self.has_date_pattern = re.compile(
            r"\d{1,2}[./]\d{1,2}|\d{1,2}\s+(мар|апр|май|июн|июл|авг|сен|окт|ноя|дек)|(янв|фев|мар|апр|май|июн|июл|авг|сен|окт|ноя|дек)\s+\d{1,2}",
            re.IGNORECASE,
        )

    def has_india_destination(self, text: str) -> bool:
        """Проверяет, есть ли в тексте упоминание Индии/Гоа (только целые слова)"""
        if not text:
            return False

        # Используем регулярное выражение с границами слов
        return bool(DEST_PATTERN.search(text))

    def extract_dates(self, text: str) -> List[Dict]:
        """Извлекает все даты из текста"""
        dates_info = []

        if not text:
            return dates_info

        text_lower = text.lower()

        # 1. Формат ДД.ММ.ГГ
        for match in re.finditer(r"(\d{1,2})[./](\d{1,2})[./](\d{2,4})", text):
            day, month, year = match.groups()
            day, month = int(day), int(month)

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
                        "full_date": datetime(year, month, day),
                        "original": match.group(0),
                    }
                )

        # 2. Формат ДД.ММ
        for match in re.finditer(r"(\d{1,2})[./](\d{1,2})(?![./\d])", text):
            day, month = match.groups()
            day, month = int(day), int(month)

            if 1 <= month <= 12 and 1 <= day <= 31:
                dates_info.append(
                    {
                        "day": day,
                        "month": month,
                        "year": TARGET_YEAR,
                        "full_date": datetime(TARGET_YEAR, month, day),
                        "original": match.group(0),
                    }
                )

        # 3. Формат "5 марта"
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

        # Убираем дубликаты
        unique_dates = []
        seen = set()
        for d in dates_info:
            key = f"{d.get('day')}-{d.get('month')}-{d.get('year')}"
            if key not in seen and d.get("day") and d.get("month"):
                seen.add(key)
                unique_dates.append(d)

        return unique_dates

    def extract_price(self, text: str) -> Optional[int]:
        """Извлекает цену из текста"""
        if not text:
            return None

        prices = []

        for pattern in self.price_patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                price_str = match.group(1)
                price_str = re.sub(r"\s+", "", price_str)
                price_str = price_str.replace(",", ".").replace(" ", "")

                try:
                    if "." in price_str:
                        price = int(float(price_str))
                    else:
                        price = int(price_str)

                    if 1000 <= price <= 500000:
                        prices.append(price)
                except ValueError:
                    continue

        return min(prices) if prices else None

    def has_any_date(self, text: str) -> bool:
        """Проверяет, есть ли в тексте вообще какие-то даты"""
        return bool(self.has_date_pattern.search(text))

    def extract_months_from_text(self, text: str) -> List[int]:
        """Извлекает все упомянутые месяцы из текста"""
        months = []
        text_lower = text.lower()

        month_names = {
            "январ": 1,
            "феврал": 2,
            "март": 3,
            "апрел": 4,
            "май": 5,
            "мая": 5,
            "июн": 6,
            "июл": 7,
            "август": 8,
            "сентябр": 9,
            "октябр": 10,
            "ноябр": 11,
            "декабр": 12,
        }

        for name, num in month_names.items():
            if name in text_lower:
                months.append(num)

        return months

    def is_relevant(self, text: str) -> Tuple[bool, Dict[str, Any]]:
        """
        Проверяет, релевантно ли сообщение
        """
        if not text or len(text) < MIN_TEXT_LENGTH:
            return False, {}

        # 1. Проверяем наличие Индии/Гоа (целые слова)
        has_india = self.has_india_destination(text)
        if not has_india:
            return False, {}

        # 2. Извлекаем данные
        all_dates = self.extract_dates(text)
        mentioned_months = self.extract_months_from_text(text)
        price = self.extract_price(text)

        # 3. Анализируем даты
        target_month_dates = []
        other_dates = []

        for date_info in all_dates:
            if date_info.get("month") == TARGET_MONTH:
                target_month_dates.append(date_info)
            else:
                other_dates.append(date_info)

        has_target_month_date = len(target_month_dates) > 0
        has_any_date_in_text = len(all_dates) > 0
        has_mention_of_march = TARGET_MONTH in mentioned_months

        # 4. Дополнительная проверка на контекст (исключаем круизы и т.д.)
        text_lower = text.lower()
        exclude_keywords = [
            "круиз",
            "круизы",
            "cruise",
            "корабль",
            "ship",
            "теплоход",
        ]
        has_exclude = any(keyword in text_lower for keyword in exclude_keywords)

        if has_exclude:
            logger.info(f"Excluded due to keyword: {text_lower[:100]}")
            return False, {}

        # Логика принятия решения:
        if has_target_month_date:
            # Есть конкретные даты в марте
            is_match = True
            reason = "exact_march_dates"
        elif has_mention_of_march and SEND_IF_NO_DATE:
            # Нет конкретных дат, но есть упоминание марта
            is_match = True
            reason = "march_mentioned"
        elif not has_any_date_in_text and SEND_IF_NO_DATE:
            # Дат вообще нет
            is_match = True
            reason = "no_dates"
        elif has_any_date_in_text and not has_target_month_date:
            # Даты есть, но не март - пропускаем
            is_match = False
            reason = "wrong_month"
        else:
            is_match = False
            reason = "unknown"

        if is_match:
            logger.info(f"✅ MATCH ({reason}): {text[:100]}...")

        return is_match, {
            "destinations": list(set(re.findall(DEST_PATTERN, text.lower()))),
            "all_dates": all_dates,
            "target_month_dates": target_month_dates,
            "mentioned_months": mentioned_months,
            "price": price,
            "has_target_month_date": has_target_month_date,
            "has_march_mention": has_mention_of_march,
            "reason": reason,
        }


async def monitor_channels():
    """Основная функция мониторинга"""
    logger.info("=" * 50)
    logger.info(f"Starting flight monitoring cycle")
    logger.info(f"Looking for flights to India/Goa in March {TARGET_YEAR}")
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
                        elif details["has_march_mention"]:
                            date_str = "март"
                        else:
                            date_str = "дата не указана"

                        dest_str = ", ".join(set(details["destinations"]))
                        price_str = (
                            f"{details['price']:,}₽".replace(",", " ")
                            if details["price"]
                            else "цена не указана"
                        )

                        # Короткий превью текста
                        preview = (
                            msg.text[:300] + "..."
                            if len(msg.text) > 300
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
        for msg in found_messages:
            # Формируем красивое сообщение
            header = f"✈️ **{msg['channel']}**\n"
            header += f"_{msg['summary']}_\n\n"

            # Добавляем превью текста
            full_text = (
                header
                + msg["preview"]
                + f"\n\n[👉 Открыть пост]({msg['link']})"
            )

            await client.send_message(
                my_user_id, full_text, parse_mode="md", link_preview=False
            )

        logger.info(f"Sent {len(found_messages)} matches")
    else:
        logger.info("No matches found in this cycle")
        await client.send_message(
            my_user_id,
            f"🔍 Мониторинг завершен: новых предложений в Индию на март {TARGET_YEAR} не найдено",
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
