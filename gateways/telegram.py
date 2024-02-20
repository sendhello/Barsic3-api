import asyncio
import types
from aiogram import Bot, types
from core.settings import settings
from aiogram.types import Message


bot = Bot(token=settings.telegram_token)


async def send_message(channel_id: int, text: str) -> Message:
    return await bot.send_message(channel_id, text)
