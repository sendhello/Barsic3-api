from __future__ import annotations

import argparse
import asyncio
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import select

from db.postgres import async_session
from models.report import ReportElementModel, ReportGroupModel, ReportNameModel

logger = logging.getLogger(__name__)

DEFAULT_REPORT_NAME = "attendance"
DEFAULT_DELIMITER = " / "
DEFAULT_JSON_PATH = Path(__file__).resolve().parent / "attendance_report_groups.json"


@dataclass
class Stats:
    report_name_created: bool = False
    groups_created: int = 0
    groups_existing: int = 0
    elements_created: int = 0
    elements_existing: int = 0


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)
    if not isinstance(data, dict):
        raise ValueError("JSON root should be an object.")
    return data


async def _get_report_name(session, title: str) -> ReportNameModel | None:
    result = await session.execute(select(ReportNameModel).where(ReportNameModel.title == title))
    return result.scalars().first()


async def _get_group(session, report_name_id, title: str) -> ReportGroupModel | None:
    result = await session.execute(
        select(ReportGroupModel).where(
            ReportGroupModel.title == title,
            ReportGroupModel.report_name_id == report_name_id,
        )
    )
    return result.scalars().first()


async def _get_element(session, group_id, title: str) -> ReportElementModel | None:
    result = await session.execute(
        select(ReportElementModel).where(
            ReportElementModel.title == title,
            ReportElementModel.group_id == group_id,
        )
    )
    return result.scalars().first()


def _make_title(parts: list[str], delimiter: str) -> str:
    return delimiter.join(parts)


async def _ensure_group(
    session,
    report_name_id,
    path_key: tuple[str, ...],
    title: str,
    parent_id,
    cache: dict[tuple[str, ...], ReportGroupModel],
    stats: Stats,
) -> ReportGroupModel:
    cached = cache.get(path_key)
    if cached is not None:
        return cached

    group = await _get_group(session, report_name_id, title)
    if group is None:
        group = ReportGroupModel(title=title, parent_id=parent_id, report_name_id=report_name_id)
        session.add(group)
        await session.flush()
        stats.groups_created += 1
    else:
        stats.groups_existing += 1
        if group.parent_id != parent_id:
            raise ValueError(
                "Found existing group with unexpected parent. "
                f"title='{title}', current_parent_id={group.parent_id}, expected_parent_id={parent_id}."
            )

    cache[path_key] = group
    return group


async def _ensure_element(session, group_id, title: str, stats: Stats) -> ReportElementModel:
    element = await _get_element(session, group_id, title)
    if element is None:
        element = ReportElementModel(title=title, group_id=group_id)
        session.add(element)
        await session.flush()
        stats.elements_created += 1
    else:
        stats.elements_existing += 1
    return element


async def _apply_structure(
    session,
    data: dict[str, Any],
    report_name_id,
    delimiter: str,
    stats: Stats,
) -> None:
    cache: dict[tuple[str, ...], ReportGroupModel] = {}
    for level1, level2_map in data.items():
        if not isinstance(level2_map, dict):
            raise ValueError(f"Invalid structure under '{level1}': expected object.")

        level1_path = (level1,)
        level1_title = _make_title(list(level1_path), delimiter)
        level1_group = await _ensure_group(
            session,
            report_name_id,
            path_key=level1_path,
            title=level1_title,
            parent_id=None,
            cache=cache,
            stats=stats,
        )

        for level2, level3_map in level2_map.items():
            if not isinstance(level3_map, dict):
                raise ValueError(f"Invalid structure under '{level1} -> {level2}': expected object.")

            level2_path = (level1, level2)
            level2_title = _make_title(list(level2_path), delimiter)
            level2_group = await _ensure_group(
                session,
                report_name_id,
                path_key=level2_path,
                title=level2_title,
                parent_id=level1_group.id,
                cache=cache,
                stats=stats,
            )

            for level3, elements in level3_map.items():
                if not isinstance(elements, list):
                    raise ValueError(
                        f"Invalid structure under '{level1} -> {level2} -> {level3}': expected list."
                    )

                level3_path = (level1, level2, level3)
                level3_title = _make_title(list(level3_path), delimiter)
                level3_group = await _ensure_group(
                    session,
                    report_name_id,
                    path_key=level3_path,
                    title=level3_title,
                    parent_id=level2_group.id,
                    cache=cache,
                    stats=stats,
                )

                for element_title in elements:
                    if not isinstance(element_title, str):
                        raise ValueError(
                            f"Invalid element under '{level1} -> {level2} -> {level3}': expected string."
                        )
                    await _ensure_element(session, level3_group.id, element_title, stats)


async def main() -> int:
    parser = argparse.ArgumentParser(
        description="Load attendance report groups and elements into Postgres.",
    )
    parser.add_argument(
        "--json",
        dest="json_path",
        default=str(DEFAULT_JSON_PATH),
        help="Path to JSON file with report groups.",
    )
    parser.add_argument(
        "--report-name",
        dest="report_name",
        default=DEFAULT_REPORT_NAME,
        help="Report name to create/use.",
    )
    parser.add_argument(
        "--delimiter",
        dest="delimiter",
        default=DEFAULT_DELIMITER,
        help="Delimiter for group titles (full path is stored to keep titles unique).",
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        help="Parse and validate, but rollback changes.",
    )
    args = parser.parse_args()

    json_path = Path(args.json_path).expanduser().resolve()
    data = _load_json(json_path)

    stats = Stats()
    async with async_session() as session:
        report_name = await _get_report_name(session, args.report_name)
        if report_name is None:
            report_name = ReportNameModel(title=args.report_name)
            session.add(report_name)
            await session.flush()
            stats.report_name_created = True

        await _apply_structure(
            session,
            data=data,
            report_name_id=report_name.id,
            delimiter=args.delimiter,
            stats=stats,
        )

        if args.dry_run:
            await session.rollback()
        else:
            await session.commit()

    logger.info(
        "Done. report_name_created=%s groups_created=%s groups_existing=%s elements_created=%s elements_existing=%s",
        stats.report_name_created,
        stats.groups_created,
        stats.groups_existing,
        stats.elements_created,
        stats.elements_existing,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
