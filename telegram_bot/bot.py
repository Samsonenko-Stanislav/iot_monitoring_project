import asyncio
import os
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F, types
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import StatesGroup, State

# FSM состояния
class ParamSelect(StatesGroup):
    sensor = State()
    parameter = State()
    count = State()

# Главное меню
main_kb = types.ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[
    [types.KeyboardButton(text="🔎 Статус")],
    [types.KeyboardButton(text="📊 Последние показания")]
])

# Подключение к боту
BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())

# Глобальное подключение к базе
conn = None

@dp.message(Command("start"))
async def start_cmd(msg: types.Message):
    global conn
    user_id = msg.from_user.id
    username = msg.from_user.username or ""
    full_name = msg.from_user.full_name or ""

    with conn.cursor() as cur:
        cur.execute("SELECT * FROM users WHERE telegram_id = %s", (user_id,))
        user = cur.fetchone()
        if not user:
            cur.execute("""
                INSERT INTO users (telegram_id, full_name, username, role)
                VALUES (%s, %s, %s, %s)
            """, (user_id, full_name, username, 'operator'))
            await msg.answer("✅ Вы зарегистрированы как оператор.", reply_markup=main_kb)
        else:
            await msg.answer("👋 Добро пожаловать!", reply_markup=main_kb)

@dp.message(Command("users"))
async def show_users(msg: types.Message):
    global conn
    user_id = msg.from_user.id
    with conn.cursor() as cur:
        cur.execute("SELECT role FROM users WHERE telegram_id = %s", (user_id,))
        row = cur.fetchone()
        if not row or row[0] != 'admin':
            await msg.reply("⛔ Только для администраторов.")
            return
        cur.execute("SELECT full_name, username, role, registered_at FROM users")
        users = cur.fetchall()

    text = "👥 <b>Пользователи:</b>\n"
    for name, username, role, reg_at in users:
        text += f"{name} (@{username}) — <i>{role}</i>, {reg_at.strftime('%Y-%m-%d %H:%M')}\n"
    await msg.reply(text or "Нет пользователей.")

@dp.message(Command("setadmin"))
async def set_admin(msg: types.Message):
    global conn
    admin_id = msg.from_user.id
    parts = msg.text.strip().split()
    if len(parts) != 2 or not parts[1].startswith("@"):
        await msg.reply("Использование: /setadmin @username")
        return
    username = parts[1][1:]

    with conn.cursor() as cur:
        cur.execute("SELECT role FROM users WHERE telegram_id = %s", (admin_id,))
        role_row = cur.fetchone()
        if not role_row or role_row[0] != 'admin':
            await msg.reply("⛔ Только администратор может менять роли.")
            return

        cur.execute("SELECT telegram_id FROM users WHERE username = %s", (username,))
        target = cur.fetchone()
        if not target:
            await msg.reply(f"❌ Пользователь @{username} не найден.")
            return
        if target[0] == admin_id:
            await msg.reply("🚫 Нельзя изменить свою собственную роль.")
            return

        cur.execute("UPDATE users SET role = 'admin' WHERE username = %s", (username,))
        await msg.reply(f"✅ @{username} теперь администратор.")

@dp.message(Command("setoperator"))
async def set_operator(msg: types.Message):
    global conn
    admin_id = msg.from_user.id
    parts = msg.text.strip().split()
    if len(parts) != 2 or not parts[1].startswith("@"):
        await msg.reply("Использование: /setoperator @username")
        return
    username = parts[1][1:]

    with conn.cursor() as cur:
        cur.execute("SELECT role FROM users WHERE telegram_id = %s", (admin_id,))
        role_row = cur.fetchone()
        if not role_row or role_row[0] != 'admin':
            await msg.reply("⛔ Только администратор может менять роли.")
            return

        cur.execute("SELECT telegram_id FROM users WHERE username = %s", (username,))
        target = cur.fetchone()
        if not target:
            await msg.reply(f"❌ Пользователь @{username} не найден.")
            return
        if target[0] == admin_id:
            await msg.reply("🚫 Нельзя изменить свою собственную роль.")
            return

        cur.execute("UPDATE users SET role = 'operator' WHERE username = %s", (username,))
        await msg.reply(f"🔁 @{username} теперь оператор.")

@dp.message(F.text == "🔎 Статус")
async def status_sensors(msg: types.Message):
    global conn
    user_id = msg.from_user.id
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT sensor FROM sensor_data_ext WHERE telegram_id = %s", (user_id,))
        sensors = cur.fetchall()

    if not sensors:
        await msg.answer("Нет данных.")
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=s[0], callback_data=f"status:{s[0]}")] for s in sensors
    ])
    await msg.answer("Выберите датчик:", reply_markup=kb)

@dp.callback_query(F.data.startswith("status:"))
async def status_sensor_selected(callback: types.CallbackQuery):
    global conn
    sensor = callback.data.split(":")[1]
    user_id = callback.from_user.id
    with conn.cursor() as cur:
        cur.execute("""
            SELECT parameter, value, unit, timestamp
            FROM sensor_data_ext
            WHERE sensor = %s AND telegram_id = %s
            ORDER BY timestamp DESC
        """, (sensor, user_id))
        rows = cur.fetchall()

    result = {}
    for param, value, unit, ts in rows:
        if param not in result:
            result[param] = (value, unit, ts)

    if not result:
        await callback.message.answer("Нет данных.")
        return

    text = f"📟 <b>{sensor}</b>:\n"
    for param, (val, unit, ts) in result.items():
        text += f"{param} = {val} {unit}\n"
    await callback.message.answer(text)

@dp.message(F.text == "📊 Последние показания")
async def latest_sensor_select(msg: types.Message, state: FSMContext):
    global conn
    user_id = msg.from_user.id
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT sensor FROM sensor_data_ext WHERE telegram_id = %s", (user_id,))
        sensors = cur.fetchall()

    if not sensors:
        await msg.answer("Нет данных.")
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=s[0], callback_data=f"param_sensor:{s[0]}")] for s in sensors
    ])
    await msg.answer("Выберите датчик:", reply_markup=kb)

@dp.callback_query(F.data.startswith("param_sensor:"))
async def choose_parameter(callback: types.CallbackQuery, state: FSMContext):
    global conn
    sensor = callback.data.split(":")[1]
    user_id = callback.from_user.id
    await state.update_data(sensor=sensor)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT parameter FROM sensor_data_ext
            WHERE sensor = %s AND telegram_id = %s
        """, (sensor, user_id))
        params = cur.fetchall()

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=p[0], callback_data=f"param_select:{p[0]}")] for p in params
    ])
    await callback.message.answer("Выберите параметр:", reply_markup=kb)

@dp.callback_query(F.data.startswith("param_select:"))
async def ask_count(callback: types.CallbackQuery, state: FSMContext):
    param = callback.data.split(":")[1]
    await state.update_data(parameter=param)
    await state.set_state(ParamSelect.count)
    await callback.message.answer(f"Сколько последних значений показать для <b>{param}</b>?")

@dp.message(ParamSelect.count)
async def show_last_values(msg: types.Message, state: FSMContext):
    global conn
    user_id = msg.from_user.id
    try:
        n = int(msg.text)
        if n <= 0:
            raise ValueError
    except:
        await msg.answer("Введите положительное число.")
        return

    data = await state.get_data()
    sensor = data['sensor']
    parameter = data['parameter']

    with conn.cursor() as cur:
        cur.execute("""
            SELECT timestamp, value
            FROM sensor_data_ext
            WHERE telegram_id = %s AND sensor = %s AND parameter = %s
            ORDER BY timestamp DESC LIMIT %s
        """, (user_id, sensor, parameter, n))
        rows = cur.fetchall()

    if not rows:
        await msg.answer("Нет данных.")
        return

    df = pd.DataFrame(rows, columns=["timestamp", "value"]).sort_values("timestamp")
    plt.figure(figsize=(8, 4))
    plt.plot(df["timestamp"], df["value"], marker="o")
    plt.title(f"{sensor}.{parameter}")
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    path = "plot.png"
    plt.savefig(path)
    plt.close()

    photo = FSInputFile(path)
    await msg.answer_photo(photo)
    await state.clear()

# Запуск
async def main():
    global conn
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    conn.autocommit = True
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
