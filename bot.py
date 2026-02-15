import telebot
import pandas as pd
import html
import os
from datetime import datetime, timedelta
from telebot.types import ReplyKeyboardMarkup, KeyboardButton

TOKEN = os.getenv("BOT_TOKEN")
bot = telebot.TeleBot(TOKEN)

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