from aiogram import Bot
from aiogram.types import Message

from core.settings import settings


class TelegramBot(Bot):
    def __init__(self, telegram_token):
        self._bot = Bot(token=telegram_token)

    async def send_message(self, channel_id: int, text: str) -> Message:
        return await self._bot.send_message(channel_id, text)


def get_telegram_bot():
    return TelegramBot(settings.telegram_token)
