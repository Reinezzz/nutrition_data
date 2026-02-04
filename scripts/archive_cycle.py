#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monthly archive cycle script:
1) Fetch candidates from Airtable views (minimal GET requests, pageSize=100, fields[] limited)
2) Save CSV dumps into:
   data_dumps/dump_<MM:YYYY>/
     - DailyLog_dump_<MM:YYYY>.csv
     - Meals_dump_<MM:YYYY>.csv
3) Create ExportRun record
4) Mark exported records as Archived in DailyLog and Meals (PATCH batches of 10)
"""

import csv
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests

# ---------------- ENV ----------------
AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE_ID = os.environ["AIRTABLE_BASE_ID"]

API_BASE = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}"
HEADERS = {"Authorization": f"Bearer {AIRTABLE_TOKEN}", "Content-Type": "application/json"}

# Tables / Views
TABLE_DAILYLOG = os.environ.get("AIRTABLE_DAILYLOG_TABLE", "DailyLog")
TABLE_MEALS = os.environ.get("AIRTABLE_MEALS_TABLE", "Meals")
TABLE_RUNS = os.environ.get("AIRTABLE_RUN_TABLE", "ExportRun")

VIEW_DAILYLOG = os.environ.get("AIRTABLE_DAILYLOG_VIEW", "Export_DailyLog")
VIEW_MEALS = os.environ.get("AIRTABLE_MEALS_VIEW", "Export_Meals")

# ExportRun field names (override if your schema differs)
RUN_FIELD_RUNID = os.environ.get("RUN_FIELD_RUNID", "RunId")  # primary
RUN_FIELD_STATUS = os.environ.get("RUN_FIELD_STATUS", "Status")
RUN_FIELD_EXECUTED_AT = os.environ.get("RUN_FIELD_EXECUTED_AT", "ExecutedAt")
RUN_FIELD_CUTOFF = os.environ.get("RUN_FIELD_CUTOFF", "CutoffDate")
RUN_FIELD_DL_COUNT = os.environ.get("RUN_FIELD_DL_COUNT", "DailyLogCount")
RUN_FIELD_MEALS_COUNT = os.environ.get("RUN_FIELD_MEALS_COUNT", "MealsCount")
RUN_FIELD_DL_FILE = os.environ.get("RUN_FIELD_DL_FILE", "DailyLogFile")
RUN_FIELD_MEALS_FILE = os.environ.get("RUN_FIELD_MEALS_FILE", "MealsFile")

# Archived field names in fact tables
FIELD_ARCHIVED = os.environ.get("FIELD_ARCHIVED", "Archived")
FIELD_ARCHIVED_AT = os.environ.get("FIELD_ARCHIVED_AT", "ArchivedAt")
FIELD_ARCHIVE_BATCH = os.environ.get("FIELD_ARCHIVE_BATCH", "ArchiveBatch")
FIELD_EXPORT_RUN_LINK = os.environ.get("FIELD_EXPORT_RUN_LINK", "ExportRun")

# Airtable limits
PAGE_SIZE = 100
PATCH_BATCH = 10
MIN_SLEEP = 0.25  # ~4 req/sec

# ---------------- EXPORT FIELDS ----------------
DAILYLOG_FIELDS = [
    "Вес утром",
    "Вес вечером",
    "Средний вес за день",
    "Дата",
    "Калории приема пищи",
    "Белки приема пищи",
    "Жиры приема пищи",
    "Углеводы приема пищи",
    "Цель по калориям",
    "Цель по белкам",
    "Цель по жирам",
    "Цель по углеводам",
    "Описание",
]

MEALS_FIELDS = [
    "Тип приема пищи",
    "Дата и время",
    "Вид приема пищи",
    "Калории приема пищи",
    "Белки приема пищи",
    "Жиры приема пищи",
    "Углеводы приема пищи",
    "DayKey",
    "DayDate",
    "Описание",
]

# ---------------- PATHS & NAMING ----------------
def tag_mm_colon_yyyy(dt: datetime) -> str:
    return dt.strftime("%m:%Y")  # "02:2026"

def dump_dir_for(tag: str) -> Path:
    return Path("data_dumps") / f"dump_{tag}"

def dailylog_file_for(tag: str) -> str:
    return f"DailyLog_dump_{tag}.csv"

def meals_file_for(tag: str) -> str:
    return f"Meals_dump_{tag}.csv"

# ---------------- HELPERS ----------------
def _sleep():
    time.sleep(MIN_SLEEP)

def _normalize_cell(v: Any) -> Any:
    if isinstance(v, list):
        return "; ".join(str(x) for x in v)
    if isinstance(v, dict):
        return str(v)
    return v

def _request_with_backoff(method: str, url: str, **kwargs) -> requests.Response:
    for _ in range(8):
        _sleep()
        resp = requests.request(method, url, headers=HEADERS, timeout=60, **kwargs)
        if resp.status_code != 429:
            resp.raise_for_status()
            return resp
        retry_after = int(resp.headers.get("Retry-After", "1"))
        time.sleep(max(retry_after, 1))
    resp.raise_for_status()
    return resp

def fetch_records(table: str, view: str, fields: List[str]) -> List[Dict[str, Any]]:
    url = f"{API_BASE}/{table}"
    out: List[Dict[str, Any]] = []
    offset: Optional[str] = None

    while True:
        params: Dict[str, Any] = {"view": view, "pageSize": PAGE_SIZE}
        params["fields[]"] = fields
        if offset:
            params["offset"] = offset

        resp = _request_with_backoff("GET", url, params=params)
        data = resp.json()

        out.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break

    return out

def write_csv_utf8sig(path: Path, fields: List[str], records: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(fields)
        for rec in records:
            rf = rec.get("fields", {})
            row = [_normalize_cell(rf.get(col, "")) for col in fields]
            w.writerow(row)

def chunk(lst: List[Any], n: int) -> Iterable[List[Any]]:
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def create_export_run(
    run_tag: str,
    cutoff_date: Optional[str],
    daily_count: int,
    meals_count: int,
    daily_file_path: str,
    meals_file_path: str,
) -> str:
    url = f"{API_BASE}/ExportRun"

    fields = {
        "RunID": run_tag,                     # primary
        "Status": "Exported",                 # single select (точно как в Airtable)
        "ExecutedAt": datetime.utcnow().isoformat(),
        "DailyLogCount": daily_count,
        "MealsCount": meals_count,
        "DailyLogfile": daily_file_path,      # Text
        "MealsFile": meals_file_path,          # Text
    }

    if cutoff_date:
        fields["CutoffDate"] = cutoff_date

    payload = {"fields": fields}

    resp = _request_with_backoff("POST", url, json=payload)
    return resp.json()["id"]


def patch_records_archived(table: str, record_ids: List[str], run_record_id: str, run_tag: str) -> None:
    if not record_ids:
        return

    url = f"{API_BASE}/{table}"
    now = datetime.utcnow().isoformat() + "Z"

    for part in chunk(record_ids, PATCH_BATCH):
        payload = {
            "records": [
                {
                    "id": rid,
                    "fields": {
                        FIELD_ARCHIVED: True,
                        FIELD_ARCHIVED_AT: now,
                        FIELD_ARCHIVE_BATCH: run_tag,
                        FIELD_EXPORT_RUN_LINK: [run_record_id],
                    },
                }
                for rid in part
            ]
        }
        _request_with_backoff("PATCH", url, json=payload)

def infer_cutoff_date(_: List[Dict[str, Any]]) -> Optional[str]:
    # если хочешь — добавь CutoffDate lookup в view/fields и верни его из первой записи
    return None

# ---------------- MAIN ----------------
def main():
    now = datetime.utcnow()
    tag = tag_mm_colon_yyyy(now)  # "MM:YYYY"
    run_tag = tag

    dump_dir = dump_dir_for(tag)
    dl_path = dump_dir / dailylog_file_for(tag)
    meals_path = dump_dir / meals_file_for(tag)

    print(f"Run tag: {run_tag}")
    print(f"Dump dir: {dump_dir}")

    # 1) Fetch candidates
    print(f"Fetching DailyLog: table={TABLE_DAILYLOG}, view={VIEW_DAILYLOG}")
    dl = fetch_records(TABLE_DAILYLOG, VIEW_DAILYLOG, DAILYLOG_FIELDS)
    print(f"DailyLog records: {len(dl)}")

    print(f"Fetching Meals: table={TABLE_MEALS}, view={VIEW_MEALS}")
    ml = fetch_records(TABLE_MEALS, VIEW_MEALS, MEALS_FIELDS)
    print(f"Meals records: {len(ml)}")

    # 2) Write CSV dumps
    write_csv_utf8sig(dl_path, DAILYLOG_FIELDS, dl)
    write_csv_utf8sig(meals_path, MEALS_FIELDS, ml)
    print(f"Wrote:\n- {dl_path}\n- {meals_path}")

    # 3) Create ExportRun
    cutoff = infer_cutoff_date(dl)
    run_record_id = create_export_run(
        run_tag=run_tag,
        cutoff_date=cutoff,
        daily_count=len(dl),
        meals_count=len(ml),
        daily_file_path=str(dl_path).replace("\\", "/"),
        meals_file_path=str(meals_path).replace("\\", "/"),
    )
    print(f"Created ExportRun record id: {run_record_id}")

    # 4) Mark Archived
    dl_ids = [r["id"] for r in dl]
    ml_ids = [r["id"] for r in ml]

    print("Marking DailyLog as Archived...")
    patch_records_archived(TABLE_DAILYLOG, dl_ids, run_record_id, run_tag)

    print("Marking Meals as Archived...")
    patch_records_archived(TABLE_MEALS, ml_ids, run_record_id, run_tag)

    print("Done.")

if __name__ == "__main__":
    try:
        main()
    except KeyError as e:
        print(f"Missing env var: {e}", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
