from aiogram import Bot
from aiogram.types import Message

from core.settings import settings


bot = Bot(token=settings.telegram_token)


async def send_message(channel_id: int, text: str) -> Message:
    return await bot.send_message(channel_id, text)
