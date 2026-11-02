import asyncio
from collections import defaultdict
from datetime import datetime

from core.settings import settings
from db.mssql import MsSqlDatabase
from legacy import functions
from legacy.barsicreport2 import AQUA_COMPANIES_IDS, NON_REVENUE_GROUPS
from services.report_config import get_report_config_service


async def amain():
    cfg = await get_report_config_service().get_report_elements_with_groups("GoogleReport")
    groups_of = defaultdict(list)
    for g, servs in cfg.items():
        for s in servs:
            groups_of[s].append(g)

    srv = MsSqlDatabase(server=settings.mssql_server, user=settings.mssql_user, password=settings.mssql_pwd)
    srv.set_database(settings.mssql_database1)

    date_from = datetime(2024, 1, 1)
    date_to = datetime(2026, 9, 1)

    amounts = defaultdict(float)
    with srv as connect:
        for cid in AQUA_COMPANIES_IDS:
            r = functions.get_total_report(connect=connect, org=cid, org_name=str(cid),
                                           date_from=date_from, date_to=date_to)
            for name, row in r.items():
                if name == "Итого по отчету" or not isinstance(row[1], int | float):
                    continue
                amounts[name] += row[1]

    missing, ignored, dup = [], [], []
    for name, amt in amounts.items():
        if not amt:
            continue
        all_groups = groups_of.get(name, [])
        groups = [g for g in all_groups if g not in NON_REVENUE_GROUPS]
        if not groups:
            (ignored if "Не учитывать" in all_groups else missing).append((amt, name, all_groups))
        elif len(groups) > 1:
            dup.append((amt, name, groups))

    def show(title, rows):
        print(f"\n### {title}: {len(rows)} услуг, {sum(a for a, _, _ in rows):,.2f} ₽")
        for amt, name, gs in sorted(rows, key=lambda x: -abs(x[0])):
            print(f"  {amt:>13,.2f}  {name}   [{', '.join(gs) or 'нет группы'}]")

    print(f"Период {date_from:%d.%m.%Y} — {date_to:%d.%m.%Y}, организации: {AQUA_COMPANIES_IDS}")
    show("Лежат в «Не учитывать», но выручка есть", ignored)
    show("Нет ни в одной группе", missing)
    show("Попадают сразу в несколько групп (двойной счет)", dup)


asyncio.run(amain())
