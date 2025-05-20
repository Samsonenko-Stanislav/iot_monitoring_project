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
    target_id = State()

# Подключение к окружению
BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())

conn = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
conn.autocommit = True
def get_main_kb(is_admin=False):
    keyboard = [[types.KeyboardButton(text="🔎 Статус")],
                [types.KeyboardButton(text="📊 Последние показания")]]
    if is_admin:
        keyboard.append([types.KeyboardButton(text="👥 Пользователи")])
        keyboard.append([types.KeyboardButton(text="🔑 Выдать админа"), types.KeyboardButton(text="🔄 Понизить")])
    return types.ReplyKeyboardMarkup(resize_keyboard=True, keyboard=keyboard)

@dp.message(Command("start"))
async def start_cmd(msg: types.Message):
    user_id = msg.from_user.id
    username = msg.from_user.username or ""
    full_name = msg.from_user.full_name or ""

    with conn.cursor() as cur:
        cur.execute("SELECT * FROM users WHERE telegram_id = %s", (user_id,))
        user = cur.fetchone()
        if not user:
            cur.execute("INSERT INTO users (telegram_id, full_name, username, role) VALUES (%s, %s, %s, %s)",
                        (user_id, full_name, username, 'operator'))
            role = 'operator'
        else:
            role = user[4]

    await msg.answer("👋 Добро пожаловать!", reply_markup=get_main_kb(is_admin=(role == 'admin')))

@dp.message(F.text == "🔎 Статус")
async def status_sensors(msg: types.Message):
    user_id = msg.from_user.id
    with conn.cursor() as cur:
        cur.execute("SELECT role FROM users WHERE telegram_id = %s", (user_id,))
        row = cur.fetchone()
        if row and row[0] == 'admin':
            cur.execute("SELECT DISTINCT telegram_id, username FROM users")
            users = cur.fetchall()
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"👤 @{u[1]}", callback_data=f"status_user:{u[0]}")] for u in users
            ])
            await msg.answer("Выберите пользователя:", reply_markup=kb)
        else:
            await show_status_for_user(msg, user_id)

@dp.callback_query(F.data.startswith("status_user:"))
async def status_user_selected(callback: types.CallbackQuery):
    target_id = int(callback.data.split(":")[1])
    await show_status_for_user(callback.message, target_id)

async def show_status_for_user(msg, telegram_id):
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT sensor FROM sensor_data_ext WHERE telegram_id = %s", (telegram_id,))
        sensors = cur.fetchall()
    if not sensors:
        await msg.answer("Нет данных.")
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=s[0], callback_data=f"status:{telegram_id}:{s[0]}")] for s in sensors
    ])
    await msg.answer("Выберите датчик:", reply_markup=kb)

@dp.callback_query(F.data.startswith("status:"))
async def status_sensor_selected(callback: types.CallbackQuery):
    _, telegram_id, sensor = callback.data.split(":")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT parameter, value, unit, timestamp
            FROM sensor_data_ext
            WHERE sensor = %s AND telegram_id = %s
            ORDER BY timestamp DESC
        """, (sensor, telegram_id))
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
    user_id = msg.from_user.id
    with conn.cursor() as cur:
        cur.execute("SELECT role FROM users WHERE telegram_id = %s", (user_id,))
        row = cur.fetchone()
        if row and row[0] == 'admin':
            cur.execute("SELECT DISTINCT telegram_id, username FROM users")
            users = cur.fetchall()
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"👤 @{u[1]}", callback_data=f"latest_user:{u[0]}")] for u in users
            ])
            await msg.answer("Выберите пользователя:", reply_markup=kb)
        else:
            await start_latest_sensor_selection(msg, state, user_id)

@dp.callback_query(F.data.startswith("latest_user:"))
async def latest_user_selected(callback: types.CallbackQuery, state: FSMContext):
    target_id = int(callback.data.split(":")[1])
    await start_latest_sensor_selection(callback.message, state, target_id)

async def start_latest_sensor_selection(msg, state: FSMContext, telegram_id):
    await state.update_data(target_id=telegram_id)
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT sensor FROM sensor_data_ext WHERE telegram_id = %s", (telegram_id,))
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
    sensor = callback.data.split(":")[1]
    await state.update_data(sensor=sensor)
    data = await state.get_data()
    telegram_id = data.get("target_id", callback.from_user.id)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT parameter FROM sensor_data_ext
            WHERE sensor = %s AND telegram_id = %s
        """, (sensor, telegram_id))
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
    telegram_id = data.get("target_id", msg.from_user.id)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT timestamp, value
            FROM sensor_data_ext
            WHERE telegram_id = %s AND sensor = %s AND parameter = %s
            ORDER BY timestamp DESC LIMIT %s
        """, (telegram_id, sensor, parameter, n))
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


