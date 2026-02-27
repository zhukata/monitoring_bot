import json
import os
import asyncio
import logging
import re
import html
import requests  # добавил импорт
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional
from telethon import TelegramClient
from dotenv import load_dotenv


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SESSION_NAME = os.path.join(BASE_DIR, "session")
SESSION_FILE = f"{SESSION_NAME}.session"
STATE_FILE = os.path.join(BASE_DIR, "bot_state.json")

if os.path.exists(SESSION_FILE):
    os.chmod(SESSION_FILE, 0o600)  # правильные права доступа


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
bot_token = os.environ.get("BOT_TOKEN")  # добавил токен бота

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

# Явное указание вылета "не из Москвы" должно исключать сообщение.
# Если вылет не указан — сообщение допускаем.
MOSCOW_DEPARTURE_PATTERN = re.compile(
    r"(?<!\w)(" + "|".join(re.escape(x) for x in DEPARTURE_CITIES) + r")(?!\w)",
    re.IGNORECASE,
)

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

# Файл для хранения состояния
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


def send_telegram_message(text: str) -> bool:
    """Отправляет сообщение через бота"""
    if not bot_token:
        logger.error("BOT_TOKEN not set")
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    payload = {
        "chat_id": my_user_id,
        "text": text,
        # HTML-режим проще и надёжнее: не ломает ссылки из-за `_` в URL
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            logger.info("Message sent via bot")
            return True
        else:
            logger.error(f"Failed to send message: {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error sending message via bot: {e}")
        return False


class FileState:
    """Управление состоянием в файле"""

    def __init__(self, state_file: str):
        self.state_file = state_file
        self.state = self._load()

    def _load(self) -> Dict:
        """Загружает состояние из файла"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:  # noqa: BLE001
                logger.error(f"Failed to load state file {self.state_file}: {e}")
                return {}
        return {}

    def _save(self):
        """Сохраняет состояние в файл"""
        os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
        tmp_file = f"{self.state_file}.tmp"
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(self.state, f, indent=2, ensure_ascii=False)
        os.replace(tmp_file, self.state_file)

    def get_last_id(self, channel: str) -> int:
        """Получить последний обработанный ID"""
        return self.state.get(channel, {}).get("last_id", 0)

    def set_last_id(self, channel: str, message_id: int):
        """Сохранить последний ID"""
        if channel not in self.state:
            self.state[channel] = {}
        self.state[channel]["last_id"] = message_id
        self.state[channel]["last_check"] = datetime.now().isoformat()
        self._save()

    def is_duplicate(self, channel: str, message_id: int) -> bool:
        """Проверка на дубликат (храним последние 100 ID)"""
        if channel not in self.state:
            return False
        processed = self.state[channel].get("processed_ids", [])
        return message_id in processed

    def mark_processed(self, channel: str, message_id: int):
        """Отметить сообщение как обработанное"""
        if channel not in self.state:
            self.state[channel] = {}

        # Храним последние 100 ID чтобы не разрастался файл
        processed = self.state[channel].get("processed_ids", [])
        processed.append(message_id)
        # Оставляем только последние 100
        if len(processed) > 100:
            processed = processed[-100:]
        self.state[channel]["processed_ids"] = processed
        self._save()


class FlightSearchAnalyzer:
    """Анализатор сообщений на наличие билетов в Индию"""

    def __init__(self):
        self.date_patterns = [
            r"(\d{1,2})[./](\d{1,2})[./](\d{2,4})",
            r"(\d{1,2})[./](\d{1,2})(?![./\d])",
            r"(\d{1,2})\s+(марта?|мар|march?|mar)\b",
            r"(март|march|mar)\s+(\d{1,2})\b",
        ]

        self.price_patterns = [
            r"(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s?(?:руб|р\.?|₽)\b",
            r"за\s+(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s*(?:руб|р\.?|₽)",
            r"(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s*р(?!уб)",
            # Часто в каналах цена пишется как "60500P" (латинская P)
            r"(\d{4,6})\s*[pP]\b",
        ]

        self.has_date_pattern = re.compile(
            r"\d{1,2}[./]\d{1,2}|\d{1,2}\s+(мар|апр|май|июн|июл|авг|сен|окт|ноя|дек)|(янв|фев|мар|апр|май|июн|июл|авг|сен|окт|ноя|дек)\s+\d{1,2}",
            re.IGNORECASE,
        )

    def has_india_destination(self, text: str) -> bool:
        """Проверяет наличие Индии/Гоа"""
        if not text:
            return False
        return bool(DEST_PATTERN.search(text))

    def extract_dates(self, text: str) -> List[Dict]:
        """Извлекает даты из текста"""
        dates_info = []
        if not text:
            return dates_info

        text_lower = text.lower()

        # Формат ДД.ММ.ГГ
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
                    }
                )

        # Формат ДД.ММ
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
                    }
                )

        # Формат "5 марта"
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

    def _detect_departure(self, text: str) -> Dict[str, Any]:
        """
        Определяет, указан ли вылет, и из Москвы ли он.
        Правило:
        - если явно указан вылет НЕ из Москвы -> исключаем
        - если указан из Москвы -> допускаем
        - если вылет не указан -> допускаем
        """
        if not text:
            return {"explicit": False, "is_moscow": None, "value": None}

        stopwords = {
            "перелетотель",
            "перелётотель",
            "перелет",
            "перелёт",
            "отель",
            "тур",
            "туры",
            "сибирь",
        }

        for raw_line in text.splitlines():
            line = raw_line.strip()
            line_lower = line.lower()

            # Ищем только явные строки про вылет (чтобы не ловить "из" в обычном тексте)
            if "вылет" not in line_lower and "departure" not in line_lower:
                continue
            if " из" not in line_lower and " from" not in line_lower:
                continue

            m = re.search(
                r"(?:вылет|departure)\s*(?:из|from)\s*[:\-]?\s*(.*)$",
                line_lower,
                flags=re.IGNORECASE,
            )
            rest = m.group(1) if m else line_lower

            # приоритет: первый "похожий на город" хэштег/токен после "вылет из"
            candidates = re.findall(r"#([a-zа-яё][\w\-]{2,})", rest, flags=re.IGNORECASE)
            if not candidates:
                candidates = re.findall(r"[a-zа-яё][a-zа-яё\-]{2,}", rest, flags=re.IGNORECASE)

            value = None
            for c in candidates:
                c_norm = c.strip().lower()
                if c_norm in stopwords:
                    continue
                value = c_norm
                break

            # если после "вылет из" ничего осмысленного не нашли — считаем, что город не указан
            if not value:
                return {"explicit": True, "is_moscow": None, "value": None}

            # Москва/аэропорты Москвы
            if MOSCOW_DEPARTURE_PATTERN.search(value) or MOSCOW_DEPARTURE_PATTERN.search(rest):
                return {"explicit": True, "is_moscow": True, "value": value}

            return {"explicit": True, "is_moscow": False, "value": value}

        return {"explicit": False, "is_moscow": None, "value": None}

    def extract_months_from_text(self, text: str) -> List[int]:
        """Извлекает упомянутые месяцы"""
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
        """Проверяет релевантность сообщения"""
        if not text or len(text) < MIN_TEXT_LENGTH:
            return False, {}

        # Проверяем наличие Индии
        if not self.has_india_destination(text):
            return False, {}

        # Исключаем круизы
        exclude_keywords = [
            "круиз",
            "круизы",
            "cruise",
            "корабль",
            "ship",
            "теплоход",
        ]
        if any(keyword in text.lower() for keyword in exclude_keywords):
            return False, {}

        # Фильтр по вылету: если явно НЕ Москва — исключаем
        departure = self._detect_departure(text)
        if departure.get("explicit") and departure.get("is_moscow") is False:
            return False, {}

        # Извлекаем данные
        all_dates = self.extract_dates(text)
        mentioned_months = self.extract_months_from_text(text)
        price = self.extract_price(text)

        # Анализируем даты
        target_month_dates = [
            d for d in all_dates if d.get("month") == TARGET_MONTH
        ]
        has_target_month_date = len(target_month_dates) > 0
        has_any_date = len(all_dates) > 0
        has_march_mention = TARGET_MONTH in mentioned_months

        # Логика отбора
        if has_target_month_date:
            return True, {
                "destinations": list(
                    set(re.findall(DEST_PATTERN, text.lower()))
                ),
                "target_month_dates": target_month_dates,
                "price": price,
                "departure": departure,
                "reason": "exact_dates",
            }
        elif has_march_mention and SEND_IF_NO_DATE:
            return True, {
                "destinations": list(
                    set(re.findall(DEST_PATTERN, text.lower()))
                ),
                "target_month_dates": [],
                "price": price,
                "departure": departure,
                "reason": "march_mentioned",
            }
        elif not has_any_date and SEND_IF_NO_DATE:
            return True, {
                "destinations": list(
                    set(re.findall(DEST_PATTERN, text.lower()))
                ),
                "target_month_dates": [],
                "price": price,
                "departure": departure,
                "reason": "no_dates",
            }

        return False, {}


async def monitor_channels():
    """Основная функция мониторинга"""
    logger.info("=" * 50)
    logger.info("Starting flight monitoring cycle")
    logger.info(f"Looking for flights to India/Goa in March {TARGET_YEAR}")
    logger.info("=" * 50)

    # Инициализация файлового хранилища
    state = FileState(STATE_FILE)

    # Подключение к Telegram
    client = TelegramClient(SESSION_NAME, api_id, api_hash)
    await client.start(phone=phone_number)

    analyzer = FlightSearchAnalyzer()
    found_messages = []

    # Фильтруем каналы
    valid_channels = []
    for ch in CHANNELS:
        cleaned = clean_channel(ch)
        if cleaned:
            valid_channels.append(cleaned)

    for channel in valid_channels:
        try:
            logger.info(f"📡 Checking channel: {channel}")

            # Получаем последние сообщения
            last_id = state.get_last_id(channel)
            messages = []
            async for msg in client.iter_messages(channel, min_id=last_id, limit=200):
                messages.append(msg)

            messages.sort(key=lambda x: x.id)
            new_messages = [m for m in messages if m.id > last_id]

            if new_messages:
                logger.info(f"Found {len(new_messages)} new messages")

            for msg in new_messages:
                if state.is_duplicate(channel, msg.id):
                    continue

                if msg.text:
                    is_match, details = analyzer.is_relevant(msg.text)

                    if is_match:
                        logger.info(f"✅ Found match in {channel}: ID {msg.id}")

                        # Форматируем даты
                        if details.get("target_month_dates"):
                            date_str = ", ".join(
                                [
                                    f"{d['day']:02d}.{d['month']:02d}"
                                    for d in details["target_month_dates"]
                                ]
                            )
                        elif details.get("reason") == "march_mentioned":
                            date_str = "март"
                        else:
                            date_str = "дата не указана"

                        dest_str = ", ".join(
                            details.get("destinations", ["индия"])
                        )
                        price_str = (
                            f"{details['price']:,}₽".replace(",", " ")
                            if details.get("price")
                            else "цена не указана"
                        )

                        preview = (
                            msg.text[:300] + "..."
                            if len(msg.text) > 300
                            else msg.text
                        )

                        found_messages.append(
                            {
                                "channel": channel,
                                "preview": preview,
                                "link": f"https://t.me/{channel}/{msg.id}",
                                "summary": f"📅 {date_str} | ✈️ {dest_str} | 💰 {price_str}",
                            }
                        )

                state.mark_processed(channel, msg.id)

            # Обновляем последний ID
            if new_messages:
                max_id = max(m.id for m in new_messages)
                state.set_last_id(channel, max_id)

        except Exception as e:
            logger.error(f"Error checking {channel}: {e}")

    # Отправка результатов через бота (а не через клиента)
    if found_messages:
        for msg in found_messages:
            ch = html.escape(msg["channel"])
            summary = html.escape(msg["summary"])
            preview = html.escape(msg["preview"])
            link = msg["link"]

            text = f"✈️ <b>{ch}</b>\n"
            text += f"<i>{summary}</i>\n\n"
            text += f"{preview}\n\n"
            text += f'<a href="{link}">👉 Открыть пост</a>'

            # Отправляем через бота
            send_telegram_message(text)

        logger.info(f"Sent {len(found_messages)} matches via bot")
    else:
        logger.info("No matches found")
        # Отправляем уведомление через бота
        send_telegram_message(
            html.escape(
                f"🔍 Мониторинг: новых предложений в Индию на март {TARGET_YEAR} не найдено"
            )
        )

    await client.disconnect()
    logger.info("Monitoring cycle completed")


async def main():
    try:
        await monitor_channels()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        # Пробуем отправить ошибку через бота
        if bot_token:
            send_telegram_message(
                html.escape(f"❌ Ошибка мониторинга: {str(e)[:200]}")
            )
        raise


if __name__ == "__main__":
    asyncio.run(main())
