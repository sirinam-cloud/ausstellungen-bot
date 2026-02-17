import telebot
import pandas as pd
import html
import os
import json
import time
from telegram_bot_calendar import DetailedTelegramCalendar
from collections import Counter
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
from telebot.types import ReplyKeyboardMarkup, KeyboardButton

TOKEN = os.getenv("BOT_TOKEN")
bot = telebot.TeleBot(TOKEN)

BUTTONS = {
    "🔥 выставки на сегодня": "today",
    "📅 выставки на завтра": "tomorrow",
    "⏳ заканчиваются скоро": "ending",
    "🆕 новые выставки": "starting",
    "⭐ лучшие выставки месяца": "best_month",   # ← добавили
    "📅 выбрать дату": "pick_date",

}


# =======================
# СТАТИСТИКА
# =======================
TZ = ZoneInfo("Europe/Vienna")
STATS_PATH = os.getenv("STATS_PATH", "stats.json")
os.makedirs(os.path.dirname(STATS_PATH) or ".", exist_ok=True)
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}

# ===== ШАБЛОН СТАТИСТИКИ =====
DEFAULT_STATS = {
    "total_requests": 0,
    "unique_users": [],
    "requests_by_day": {},
    "dates_asked": {},
    "sources": {},
}

_stats = DEFAULT_STATS.copy()
_last_save_ts = 0


def _load_stats():
    global _stats
    try:
        with open(STATS_PATH, "r", encoding="utf-8") as f:
            loaded = json.load(f)

            # создаём чистый шаблон
            _stats = DEFAULT_STATS.copy()

            # обновляем его данными из файла
            if isinstance(loaded, dict):
                _stats.update(loaded)

    except FileNotFoundError:
        _stats = DEFAULT_STATS.copy()
    except Exception as e:
        print("STATS load error:", e)
        _stats = DEFAULT_STATS.copy()

def _save_stats(force: bool = False):
    global _last_save_ts
    now = time.time()
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

    _save_stats(force=True)

_load_stats()


SHEETS_URL = os.getenv("SHEETS_CSV_URL")
CSV_URL = os.getenv("SHEETS_CSV_URL")


def main_keyboard():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("🔥 Выставки на сегодня"), KeyboardButton("📅 Выставки на завтра"))
    kb.row(KeyboardButton("⏳ Заканчиваются скоро"), KeyboardButton("🆕 Новые выставки"))
    kb.row(KeyboardButton("⭐ Лучшие выставки месяца"))   # ← добавили
    kb.row(KeyboardButton("📅 Выбрать дату"))
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

    # 👉 Сохраняем статистику перед выводом
    _save_stats(force=True)

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
    text = (
        "Привет!\n\n"
        "Здесь ты можешь посмотреть список действующих и будущих выставок в музеях Вены.\n\n"
        "Нажми на кнопку ниже:\n"
        "🔥 Выставки на сегодня\n"
        "📅 Выставки на завтра\n"
        "⏳ Заканчиваются скоро — выставки, которые закончатся в ближайшие 2 недели\n"
        "🆕 Новые выставки — выставки, которые начнутся в ближайшие 2 недели\n\n"
        "Если хочешь посмотреть список выставок на конкретную дату, "
        "напиши её текстом (например, 2026-02-12 или 12.02.2026)."
    )

    bot.send_message(
        message.chat.id,
        text,
        reply_markup=main_keyboard()
    )


def send_matches(chat_id, matches, header_base):
    """
    Красивый вывод matches (DataFrame) с группировкой по музеям и разбиением на части.
    header_base — строка заголовка, например "📅 ...\nНайдено: 10"
    """
    if matches is None or matches.empty:
        bot.send_message(chat_id, "Ничего не найдено.")
        return

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

    send_museum_chunks(chat_id, header_base, museum_blocks)

@bot.message_handler(commands=["ending_soon"])
def ending_soon_cmd(message):
    today = datetime.today().date()
    until = today + timedelta(days=14)

    record_request(
        message.from_user.id,
        today.strftime("%Y-%m-%d"),
        source="ending_soon"
    )

    try:
        df = load_data_cached()
    except Exception:
        bot.reply_to(message, "Не удалось прочитать таблицу. Проверь доступ по ссылке.")
        return

    matches = df[(df["end_date"] >= today) & (df["end_date"] <= until)]

    if matches.empty:
        bot.send_message(message.chat.id, "В ближайшие 2 недели ничего не заканчивается.")
        return

    header_base = (
        f"⏳ Заканчиваются в ближайшие 2 недели\n"
        f"Период: {today.strftime('%d.%m.%Y')} – {until.strftime('%d.%m.%Y')}\n"
        f"Найдено: {len(matches)}"
    )
    send_matches(message.chat.id, matches, header_base)


@bot.message_handler(commands=["starting_soon"])
def starting_soon_cmd(message):
    today = datetime.today().date()
    until = today + timedelta(days=14)

    record_request(
        message.from_user.id,
        today.strftime("%Y-%m-%d"),
        source="starting_soon"
    )
    
    try:
        df = load_data_cached()
    except Exception:
        bot.reply_to(message, "Не удалось прочитать таблицу. Проверь доступ по ссылке.")
        return

    matches = df[(df["start_date"] >= today) & (df["start_date"] <= until)]

    if matches.empty:
        bot.send_message(message.chat.id, "В ближайшие 2 недели ничего не начинается.")
        return

    header_base = (
        f"🆕 Начинаются в ближайшие 2 недели\n"
        f"Период: {today.strftime('%d.%m.%Y')} – {until.strftime('%d.%m.%Y')}\n"
        f"Найдено: {len(matches)}"
    )
    send_matches(message.chat.id, matches, header_base)


@bot.message_handler(commands=["best_month"])
def best_month_cmd(message):
    base = datetime.today().date()
    tomorrow = base + timedelta(days=1)
    month_end = base + timedelta(days=30)

    record_request(
        message.from_user.id,
        base.strftime("%Y-%m-%d"),
        source="best_month"
    )

    try:
        df = load_data_cached()
    except Exception:
        bot.reply_to(message, "Не удалось прочитать таблицу. Проверь доступ по ссылке.")
        return

    # Проверка наличия колонки BEST (без падения регистра)
    best_column = None
    for col in df.columns:
        if col.strip().lower() == "best":
            best_column = col
            break

    if not best_column:
        bot.send_message(
            message.chat.id,
            "В таблице нет колонки 'BEST'. Добавь колонку BEST со значением 'да' для лучших выставок 🙂",
            reply_markup=main_keyboard()
        )
        return

    # Маска лучших
    best_mask = (
        df[best_column].astype(str).str.strip().str.lower()
        .isin({"да", "yes", "true", "1", "y"})
    )

    # 1️⃣ Заканчивается не позднее завтра (и ещё не закончилась раньше)
    ends_by_tomorrow = (
        (df["end_date"] >= base) &
        (df["end_date"] <= tomorrow)
    )

    # 2️⃣ Начинается в пределах ближайших 30 дней
    starts_within_month = (
        (df["start_date"] >= base) &
        (df["start_date"] <= month_end)
    )

    # 3️⃣ Покрывает весь месяц от даты запроса
    covers_whole_month = (
        (df["start_date"] <= base) &
        (df["end_date"] >= month_end)
    )

    matches = df[
        best_mask &
        (ends_by_tomorrow | starts_within_month | covers_whole_month)
    ]

    if matches.empty:
        bot.send_message(
            message.chat.id,
            "Лучших выставок по этому правилу не нашла 😅",
            reply_markup=main_keyboard()
        )
        return

    header_base = (
        f"⭐ Лучшие выставки месяца\n"
        f"Период: {base.strftime('%d.%m.%Y')} – {month_end.strftime('%d.%m.%Y')}\n"
        f"Найдено: {len(matches)}"
    )

    send_matches(message.chat.id, matches, header_base)



@bot.message_handler(commands=["about"])
def about_command(message):
    text = (
        "ℹ️ Обо мне\n\n"
        "Меня зовут Маша и я люблю ходить в музеи.\n"
        "Для себя я уже давно веду список выставок и теперь решила перенести его в бот. Надеюсь, "
        "он вам будет полезен.\n\n"
        "Если вы интересуетесь историей, "
        "подписывайтесь на мой канал <a href='https://t.me/hofburg_depot'>В запасниках Хофбурга</a>.\n"
        "Там я пишу короткие заметки о разных интересностях, "
        "которые мне удалось найти об императрице Елизавете, "
        "других Габсбургах и не только о них.\n"
        "Там же я размещаю анонсы исторических прогулок, "
        "которые периодически провожу по Вене, "
        "присоединяйтесь!"
    )

    bot.send_message(
        message.chat.id,
        text,
        parse_mode="HTML",
        reply_markup=main_keyboard()
    )


@bot.message_handler(func=lambda m: True)
def handle(message):
    text = (message.text or "").strip()
    key = text.lower()

    action = BUTTONS.get(key)

    # === 1. Кнопки ===

    if action == "today":
        user_date = datetime.today().date()

    elif action == "tomorrow":
        user_date = (datetime.today() + timedelta(days=1)).date()

    elif action == "ending":
        ending_soon_cmd(message)
        return

    elif action == "starting":
        starting_soon_cmd(message)
        return

    elif action == "best_month":
        best_month_cmd(message)
        return

    elif action == "pick_date":
        calendar, step = DetailedTelegramCalendar().build()
        bot.send_message(
            message.chat.id,
            "Выберите дату:",
            reply_markup=calendar
        )
        return


    # === 2. Ввод вручную ===

    elif key in ("сегодня", "today"):
        user_date = datetime.today().date()

    elif key in ("завтра", "tomorrow"):
        user_date = (datetime.today() + timedelta(days=1)).date()

    else:
        user_date = parse_date(text)

    # === 3. Проверка даты ===

    if not user_date:
        bot.send_message(
            message.chat.id,
            "Не понял дату 😅\n"
            "Примеры: 2026-02-12 или 12.02.2026\n"
            "Также можно нажать кнопку ниже 👇",
            reply_markup=main_keyboard()
        )
        return

    # === 4. Логируем запрос ===

    record_request(
        message.from_user.id,
        user_date.strftime("%Y-%m-%d"),
        source="button" if action else "text"
    )

    # === 5. Загружаем данные ===

    status = bot.send_message(message.chat.id, "🔍 Ищу выставки…")

    try:
        df = load_data_cached()
    except Exception:
        try:
            bot.delete_message(message.chat.id, status.message_id)
        except Exception:
            pass
        bot.reply_to(message, "Не удалось прочитать таблицу. Проверь доступ по ссылке.")
        return

    matches = df[(df["start_date"] <= user_date) & (df["end_date"] >= user_date)]

    try:
        bot.delete_message(message.chat.id, status.message_id)
    except Exception:
        pass

    # === 6. Если ничего не найдено ===

    if matches.empty:
        bot.send_message(
            message.chat.id,
            "На эту дату выставок не найдено.",
            reply_markup=main_keyboard()
        )
        return

    # === 7. Отправляем результат ===

    date_text = format_date_ddmmyyyy(user_date)
    header_base = f"📅 Выставки на {date_text}\nНайдено: {len(matches)}"
    send_matches(message.chat.id, matches, header_base)


@bot.callback_query_handler(func=DetailedTelegramCalendar.func())
def cal(callback_query):
    result, key, step = DetailedTelegramCalendar().process(callback_query.data)

    if not result and key:
        bot.edit_message_text(
            f"Выберите {step}:",
            callback_query.message.chat.id,
            callback_query.message.message_id,
            reply_markup=key
        )
    elif result:
        selected_date = result

        bot.edit_message_text(
            f"Вы выбрали {selected_date.strftime('%d.%m.%Y')}",
            callback_query.message.chat.id,
            callback_query.message.message_id
        )

        user_date = selected_date

        record_request(
            callback_query.from_user.id,
            user_date.strftime("%Y-%m-%d"),
            source="calendar"
        )

        df = load_data_cached()

        matches = df[
            (df["start_date"] <= user_date) &
            (df["end_date"] >= user_date)
        ]

        if matches.empty:
            bot.send_message(
                callback_query.message.chat.id,
                "На эту дату выставок не найдено.",
                reply_markup=main_keyboard()
            )
            return

        date_text = user_date.strftime("%d.%m.%Y")
        header_base = f"📅 Выставки на {date_text}\nНайдено: {len(matches)}"

        send_matches(callback_query.message.chat.id, matches, header_base)


bot.polling()