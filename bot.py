import telebot
import pandas as pd
import html
import os
import json
import time
from collections import Counter
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
from telebot.types import ReplyKeyboardMarkup, KeyboardButton

TOKEN = os.getenv("BOT_TOKEN")
bot = telebot.TeleBot(TOKEN)

# =======================
# СТАТИСТИКА
# =======================
TZ = ZoneInfo("Europe/Vienna")
STATS_PATH = os.getenv("STATS_PATH", "stats.json")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}

_stats = {
    "total_requests": 0,
    "unique_users": [],          # список user_id
    "requests_by_day": {},       # "2026-02-15": 12
    "dates_asked": {},           # "2026-08-18": 7
    "sources": {},               # "text": 10, "button_today": 5, ...
}
_last_save_ts = 0

def _load_stats():
    global _stats
    try:
        with open(STATS_PATH, "r", encoding="utf-8") as f:
            _stats = json.load(f)
    except FileNotFoundError:
        pass
    except Exception as e:
        print("STATS load error:", e)

def _save_stats(force: bool = False):
    global _last_save_ts
    now = time.time()
    if not force and now - _last_save_ts < 15:
        return
    _last_save_ts = now
    try:
        with open(STATS_PATH, "w", encoding="utf-8") as f:
            json.dump(_stats, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print("STATS save error:", e)

def record_request(user_id: int, date_str: str, source: str = "text"):
    today = time.strftime("%Y-%m-%d", time.localtime(time.time()))
    # лучше считать "сегодня" по Вене:
    try:
        from datetime import datetime
        today = datetime.now(TZ).date().isoformat()
    except Exception:
        pass

    _stats["total_requests"] = int(_stats.get("total_requests", 0)) + 1

    # уникальные пользователи
    if user_id not in set(_stats.get("unique_users", [])):
        _stats.setdefault("unique_users", []).append(user_id)

    # запросы по дням
    rbd = _stats.setdefault("requests_by_day", {})
    rbd[today] = int(rbd.get(today, 0)) + 1

    # какие даты спрашивают
    da = _stats.setdefault("dates_asked", {})
    if date_str:
        da[date_str] = int(da.get(date_str, 0)) + 1

    # источник (кнопка/текст)
    src = _stats.setdefault("sources", {})
    src[source] = int(src.get(source, 0)) + 1

    _save_stats()

_load_stats()


SHEETS_URL = os.getenv("SHEETS_CSV_URL")
CSV_URL = os.getenv("SHEETS_CSV_URL")


def main_keyboard():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("🔥 Сегодня"), KeyboardButton("📅 Завтра"))
    kb.row(KeyboardButton("🗓 Ввести дату"))
    return kb


def parse_date(text: str):
    t = text.strip().lower()

    if t in ("сегодня", "today"):
        return datetime.today().date()

    if t in ("завтра", "tomorrow"):
        return (datetime.today() + timedelta(days=1)).date()

    formats = ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%d-%m-%Y")
    for fmt in formats:
        try:
            return datetime.strptime(t, fmt).date()
        except ValueError:
            continue

    return None

def format_date_russian(date_obj):
    months = [
        "января", "февраля", "марта", "апреля",
        "мая", "июня", "июля", "августа",
        "сентября", "октября", "ноября", "декабря"
    ]
    return f"{date_obj.day} {months[date_obj.month - 1]} {date_obj.year}"


def format_date_short_ru(date_obj):
    months = [
        "янв", "фев", "мар", "апр",
        "май", "июн", "июл", "авг",
        "сен", "окт", "ноя", "дек"
    ]
    return f"{date_obj.day:02d} {months[date_obj.month - 1]} {date_obj.year}"

def format_date_ddmmyyyy(d):
    return d.strftime("%d.%m.%Y")


# --- simple cache for Google Sheets ---
CACHE_TTL_SECONDS = 60
_cache_df = None
_cache_loaded_at = None

def load_data_cached():
    global _cache_df, _cache_loaded_at

    now = datetime.now()
    if _cache_df is not None and _cache_loaded_at is not None:
        if (now - _cache_loaded_at).total_seconds() < CACHE_TTL_SECONDS:
            return _cache_df

    df = pd.read_csv(CSV_URL)
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce").dt.date
    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce").dt.date
    df = df.dropna(subset=["start_date", "end_date"])

    _cache_df = df
    _cache_loaded_at = now
    return df

def send_museum_chunks(chat_id, header_base, museum_blocks, max_len=3500):
    """
    header_base: строка без "Часть i/N" (мы добавим её сами)
    museum_blocks: список строк, каждая = один музей (заголовок + его выставки)
    """
    # 1) сначала соберём чанки (без отправки), чтобы узнать N
    chunks = []
    chunk = ""

    for block in museum_blocks:
        piece = block.strip() + "\n\n"
        if not piece.strip():
            continue

        # если один музейный блок слишком большой — отправим его отдельно
        if len(piece) > max_len:
            if chunk.strip():
                chunks.append(chunk.strip())
                chunk = ""
            chunks.append(piece.strip())
            continue

        if len(chunk) + len(piece) > max_len:
            chunks.append(chunk.strip())
            chunk = ""

        chunk += piece

    if chunk.strip():
        chunks.append(chunk.strip())

    total = max(1, len(chunks))

    # 2) теперь отправляем, добавляя заголовок + часть i/N
    for idx, body in enumerate(chunks, start=1):
        header = f"{header_base}\nЧасть {idx}/{total}\n\n"
        bot.send_message(
            chat_id,
            header + body,
            parse_mode="HTML",
            disable_web_page_preview=True
        )


@bot.message_handler(commands=["stats"])
def stats_cmd(message):
    if message.from_user.id not in ADMIN_IDS:
        bot.reply_to(message, "Эта команда доступна только администратору.")
        return

    unique_count = len(set(_stats.get("unique_users", [])))
    total = _stats.get("total_requests", 0)

    # последние 7 дней
    rbd = _stats.get("requests_by_day", {})
    last_days = sorted(rbd.items())[-7:]
    last_days_text = "\n".join([f"{d}: {c}" for d, c in last_days]) or "нет данных"

    # топ-10 дат
    da = _stats.get("dates_asked", {})
    top_dates = sorted(da.items(), key=lambda x: x[1], reverse=True)[:10]
    top_dates_text = "\n".join([f"{d}: {c}" for d, c in top_dates]) or "нет данных"

    # источники
    src = _stats.get("sources", {})
    src_top = sorted(src.items(), key=lambda x: x[1], reverse=True)
    src_text = "\n".join([f"{k}: {v}" for k, v in src_top]) or "нет данных"

    text = (
        "📊 Статистика\n\n"
        f"Всего запросов: {total}\n"
        f"Уникальных пользователей: {unique_count}\n\n"
        "🗓 Запросы по дням (последние 7):\n"
        f"{last_days_text}\n\n"
        "📅 Самые запрашиваемые даты (топ-10):\n"
        f"{top_dates_text}\n\n"
        "🎛 Источники:\n"
        f"{src_text}"
    )
    bot.reply_to(message, text)


@bot.message_handler(commands=["start"])
def start(message):
    bot.send_message(
        message.chat.id,
        "Привет! Я покажу выставки на выбранную дату.\n"
        "Можно нажать кнопку или написать дату текстом.\n"
        "Примеры: 2026-02-12 или 12.02.2026",
        reply_markup=main_keyboard()
    )


@bot.message_handler(func=lambda m: True)
def handle(message):
    text = message.text.strip()
    low = text.lower()

    if low in ("🔥 сегодня", "сегодня"):
        user_date = datetime.today().date()

    elif low in ("📅 завтра", "завтра"):
        user_date = (datetime.today() + timedelta(days=1)).date()

    elif low in ("🗓 ввести дату", "ввести дату"):
        bot.send_message(
            message.chat.id,
            "Напишите дату в формате 2026-02-12 или 12.02.2026",
            reply_markup=main_keyboard()
        )
        return

    else:
        user_date = parse_date(text)

    if not user_date:
        bot.send_message(
            message.chat.id,
            "Не понял дату 😅\n"
            "Примеры: 2026-02-12 или 12.02.2026\n"
            "Также можно нажать кнопку ниже 👇",
            reply_markup=main_keyboard()
        )
        return

    # Записываем статистику
    record_request(
        message.from_user.id,
        user_date.strftime("%Y-%m-%d"),
        source="text"
    )


    # Пишем статус, чтобы пользователь видел, что бот работает
    status = bot.send_message(message.chat.id, "🔍 Ищу выставки…")

    # Читаем таблицу (с кэшем)
    try:
        df = load_data_cached()
    except Exception:
        # убираем статус и показываем ошибку
        try:
            bot.delete_message(message.chat.id, status.message_id)
        except Exception:
            pass
        bot.reply_to(message, "Не удалось прочитать таблицу. Проверь доступ по ссылке.")
        return

    matches = df[(df["start_date"] <= user_date) & (df["end_date"] >= user_date)]

    # Убираем статус "Ищу..."
    try:
        bot.delete_message(message.chat.id, status.message_id)
    except Exception:
        pass

    if matches.empty:
        bot.send_message(
            message.chat.id,
            "На эту дату выставок не найдено.",
            reply_markup=main_keyboard()
        )
        return



    # ↓↓↓ ВОТ СЮДА ВСТАВЛЯЕМ НОВЫЙ КОД ↓↓↓

    date_text = format_date_ddmmyyyy(user_date)
    header_base = f"📅 Выставки на {date_text}\nНайдено: {len(matches)}"

    matches = matches.sort_values(by=["museum", "end_date", "title"])

    museum_blocks = []
    current_museum = None
    lines = []

    for _, row in matches.iterrows():
        museum = html.escape(str(row["museum"]).strip())
        title = html.escape(str(row["title"]).replace("\n", " ").strip())
        url = str(row["url"]).strip()
        end_date = row["end_date"]
        end_text = format_date_short_ru(end_date) if pd.notna(end_date) else "—"

        if museum != current_museum:
            if current_museum is not None:
                museum_blocks.append("".join(lines).strip())
                lines = []
            current_museum = museum
            lines.append(f"🏛 {museum}\n")

        lines.append(f"  • ✨ <a href=\"{url}\">{title}</a> (до {end_text})\n")

    if lines:
        museum_blocks.append("".join(lines).strip())

    send_museum_chunks(message.chat.id, header_base, museum_blocks)



bot.polling()