import asyncio
import os

import yadisk

CLIENT_ID = os.environ.get("YANDEX_CLIENT_ID")
CLIENT_SECRET = os.environ.get("YANDEX_CLIENT_SECRET")


async def main():
    async with yadisk.AsyncClient(CLIENT_ID, CLIENT_SECRET) as client:
        client.get_code_url()
        code = input("Enter the confirmation code: ").strip()

        await client.get_token(code)


asyncio.run(main())
