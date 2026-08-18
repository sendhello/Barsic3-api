from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta

import yadisk

report_path = os.getenv("REPORT_PATH")
TOKEN = os.getenv("YADISK_TOKEN")
start_date = datetime.strptime("2020-01-01", "%Y-%m-%d")


async def main():
    async with yadisk.AsyncClient(token=TOKEN) as client:
        ok = await client.check_token()
        if not ok:
            pass

        async for item in client.listdir(report_path):
            if getattr(item, "type", None) == "file":
                print(item.name, item.file)


asyncio.run(main())

# вычисление отсутствующих дат
with open("/Users/ivanbazhenov/Downloads/dates from 2020-01-01.txt") as f:
    res = set()
    for line in f:
        try:
            cd = datetime.strptime(line.strip("\n"), "%d.%m.%Y")
            res.add(cd)
        except ValueError:
            print(f"Error in {line=}")
            raise
    exclude_dates = []

    def get_next_date(_start_date: datetime):
        current_date = _start_date
        while True:
            yield current_date
            current_date += timedelta(days=1)
            if current_date > datetime.now():
                break

    for date in get_next_date(datetime.strptime("2020-01-01", "%Y-%m-%d")):
        if date not in res:
            exclude_dates.append(date)
    result = "\n".join([d.strftime("%Y-%m-%d") for d in exclude_dates])
    print(result)
