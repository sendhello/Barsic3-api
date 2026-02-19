from __future__ import annotations

import asyncio
import os
import tempfile
from datetime import datetime, timedelta
from typing import Any, AsyncIterator, Tuple

import yadisk
from openpyxl import load_workbook

from repositories.yandex import get_yandex_repo


def _is_dir(item: Any) -> bool:
    # В yadisk обычно item.type == "dir"
    t = getattr(item, "type", None)
    if t is not None:
        return t == "dir"
    # На всякий случай — если есть метод/флаг
    is_dir = getattr(item, "is_dir", None)
    return bool(is_dir() if callable(is_dir) else is_dir)


def _item_path(parent: str, item: Any) -> str:
    # Обычно есть item.path (часто вида "disk:/..."), и его можно передавать обратно в методы.
    p = getattr(item, "path", None)
    if p:
        return p
    name = getattr(item, "name", None) or str(item)
    return parent.rstrip("/") + "/" + name


async def walk_all(
    client: yadisk.AsyncClient,
    root: str,
) -> AsyncIterator[Tuple[str, Any]]:
    """
    Yield: (full_path, item_resource)
    """
    stack = [root]
    while stack:
        cur = stack.pop()
        async for item in client.listdir(cur):
            full = _item_path(cur, item)
            yield full, item
            if _is_dir(item):
                stack.append(full)


repo = get_yandex_repo()

report_path = os.getenv("REPORT_PATH")
TOKEN = os.getenv("YADISK_TOKEN")
start_date = datetime.strptime("2020-01-01", "%Y-%m-%d")


async def main():
    tariffs = set()
    dates = []
    async with yadisk.AsyncClient(token=TOKEN) as client:
        ok = await client.check_token()
        if not ok:
            pass

        async for path, item in walk_all(client, report_path):
            if getattr(item, "type", None) == "file" and path.endswith(".xlsx") and "Итоговый отчет" in item.name:
                if datetime.strptime(item.name.split(" ")[0], "%Y-%m-%d") < start_date:
                    continue

                with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
                    await client.download(path, tmp.name)
                    wb = load_workbook(tmp.name, data_only=True)
                    ws = wb.active
                    for row in ws.iter_rows(min_row=1):
                        val_b = row[1].value if len(row) > 1 else None
                        val_d = row[4].value if len(row) > 4 else None
                        val_j = row[9].value if len(row) > 9 else None
                        val_l = row[11].value if len(row) > 11 else None
                        if val_b and isinstance(val_j, int) and isinstance(val_l, int):
                            tariffs.add(val_b)

                        if val_d:
                            dates.append(val_d)

        tariffs_list = sorted(map(str, tariffs))
        file_name = f"tariffs from {start_date.strftime('%Y-%m-%d')} ({len(tariffs_list)}).txt"
        remote_path = f"{report_path.rstrip('/')}/{file_name}"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", encoding="utf-8") as tmp_txt:
            tmp_txt.write("\n".join(tariffs_list))
            tmp_txt.flush()
            await client.upload(tmp_txt.name, remote_path, overwrite=True)

        sorted_dates = sorted(dates)
        file_name = f"dates from {start_date.strftime('%Y-%m-%d')}.txt"
        remote_path = f"{report_path.rstrip('/')}/{file_name}"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", encoding="utf-8") as tmp_txt:
            tmp_txt.write("\n".join(sorted_dates))
            tmp_txt.flush()
            await client.upload(tmp_txt.name, remote_path, overwrite=True)

        exclude_dates = []

        def get_next_date(_start_date: datetime) -> GeneratorExit[str]:
            current_date = _start_date
            while True:
                yield datetime.strftime(current_date, "%Y-%m-%d")
                current_date += timedelta(days=1)
                if current_date > datetime.now():
                    break

        for date in get_next_date(start_date):
            if date not in dates:
                exclude_dates.append(date)

        sorted_exclude_dates = sorted(exclude_dates)
        file_name = f"exclude_dates from {start_date.strftime('%Y-%m-%d')}.txt"
        remote_path = f"{report_path.rstrip('/')}/{file_name}"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", encoding="utf-8") as tmp_txt:
            tmp_txt.write("\n".join(sorted_exclude_dates))
            tmp_txt.flush()
            await client.upload(tmp_txt.name, remote_path, overwrite=True)


asyncio.run(main())
