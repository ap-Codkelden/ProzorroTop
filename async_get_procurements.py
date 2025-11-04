#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Async fetcher for Prozorro tenders.

- Uses aiohttp with 20 concurrent workers (~20 req/s total).
- Paginates by `offset`, persists last offset to resume if interrupted.
- Batches DuckDB inserts every ~1000 tenders (configurable).
- Reuses your existing helpers from `get_procurements.py`:
  - duckdb_insert(data, store_parquet=False)
  - get_tender_info(proc_data) -> dict
  - get_tender_date(proc_data) -> datetime
  - mk_offset_param(...)
  - API_URL, API_PATH, YESTERDAY
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from datetime import timedelta, datetime
from typing import Optional, Any, Dict, List
from zoneinfo import ZoneInfo

import aiohttp

# >>> Reuse your existing helpers & constants from the sync module
# Make sure get_procurements.py is importable (same folder / PYTHONPATH).
from get_procurements import (  # type: ignore
    duckdb_insert,
    get_tender_info,
    get_tender_date,
    mk_offset_param,
    API_URL,
    API_PATH,
    YESTERDAY,
)

from utils import KYIV_ZONE


CONCURRENCY: int = 20           # number of parallel workers
PER_WORKER_DELAY: float = 1.2    # seconds: ~1 req/sec per worker => ~20 req/s total
BATCH_SIZE: int = 1000           # insert into DuckDB every N tenders
REQUEST_TOTAL_TIMEOUT: float = 20.0
RETRY_ATTEMPTS: int = 4          # per-request retries (with backoff)
OFFSET_FILE = Path("last_offset.txt")
DUCKDB_NAME = "procurements3.db"

STOP_DATE = (datetime.combine(YESTERDAY - timedelta(days=1), datetime.min.time())).replace(tzinfo=KYIV_ZONE)



def setup_logging() -> None:
    root = logging.getLogger()
    if root.handlers:
        return
    root.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s", "%Y-%m-%d %H:%M:%S")

    fh = logging.FileHandler("download_async.log", mode="w", encoding="utf-8")
    fh.setFormatter(fmt)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)

    root.addHandler(fh)
    root.addHandler(ch)


def save_offset(offset: str) -> None:
    try:
        OFFSET_FILE.write_text(offset, encoding="utf-8")
    except Exception as e:
        logging.warning(f"Failed to save offset: {e}")

def load_offset() -> Optional[str]:
    try:
        if OFFSET_FILE.exists():
            return OFFSET_FILE.read_text(encoding="utf-8").strip() or None
    except Exception as e:
        logging.warning(f"Failed to load offset: {e}")
    return None


async def fetch_json(session: aiohttp.ClientSession, url: str, *, attempts: int = RETRY_ATTEMPTS) -> Optional[Dict[str, Any]]:
    backoff = 1.0
    for attempt in range(1, attempts + 1):
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    return await resp.json()
                if resp.status in (429, 500, 502, 503, 504):
                    ra = resp.headers.get("Retry-After")
                    sleep_s = float(ra) if ra and ra.isdigit() else backoff
                    logging.warning(f"{resp.status} for {url}; retry {attempt}/{attempts} after {sleep_s:.1f}s")
                    await asyncio.sleep(sleep_s)
                    backoff = min(backoff * 2, 30.0)
                    continue
                text = (await resp.text())[:300]
                logging.warning(f"Unexpected {resp.status} for {url}: {text}")
                return None
        except Exception as e:
            logging.warning(f"Attempt {attempt}/{attempts} failed for {url}: {e}; backoff {backoff:.1f}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
    return None


async def get_procurement_async(session: aiohttp.ClientSession, tid: str) -> Optional[Dict[str, Any]]:
    """Async version of single tender fetch (`/tenders/{id}`), returns payload['data'] or None."""
    url = f"{API_URL}{API_PATH}/{tid}"
    data = await fetch_json(session, url)
    if not data:
        return None
    body = data.get("data")
    if not isinstance(body, dict):
        return None
    return body


async def worker(
    name: int,
    session: aiohttp.ClientSession,
    queue: "asyncio.Queue[str]",
    results: List[Dict[str, Any]],
) -> None:
    """
    Worker:
    - Takes tender IDs from queue
    - Fetches tender details
    - Appends raw tender dict to results
    - Enforces ~1s pause per request (per worker) to target ~1 req/sec/worker
    """
    while True:
        tid = await queue.get()
        if tid is None:  # poison pill to exit
            queue.task_done()
            break

        tender = await get_procurement_async(session, tid)
        if tender is not None:
            results.append(tender)

        # per-worker pacing
        await asyncio.sleep(PER_WORKER_DELAY)

        queue.task_done()


# -------------------------
# Page loop + batching
# -------------------------
async def process_page(
    session: aiohttp.ClientSession,
    page_data: List[Dict[str, Any]],
    *,
    batch_acc: List[Dict[str, Any]],
) -> bool:
    """
    Given a list of tender metadata from a page, concurrently fetch details.
    Transform with get_tender_info() and batch into DuckDB via duckdb_insert().
    Returns True if stop condition (old data) is hit; else False.
    """
    # 1) queue all tender IDs
    tid_queue: "asyncio.Queue[str]" = asyncio.Queue()
    for t in page_data:
        tid = t.get("id")
        if isinstance(tid, str) and tid:
            await tid_queue.put(tid)


    results: List[Dict[str, Any]] = []
    tasks = [asyncio.create_task(worker(i, session, tid_queue, results)) for i in range(CONCURRENCY)]

    for _ in range(CONCURRENCY):
        await tid_queue.put(None)
    await tid_queue.join()
    await asyncio.gather(*tasks, return_exceptions=True)

    # 4) transform, check stop condition, and batch insert
    #    We check STOP_DATE against each tender and short-circuit if needed.
    for td in results:
        tdate = get_tender_date(td)
        if tdate < STOP_DATE:
            logging.info(f"Stop condition reached at {tdate.isoformat()}")
            return True  # signal caller to stop

        row = get_tender_info(td)
        # ensure date is iso string (yyyy-mm-dd)
        row["date"] = tdate.date().isoformat()
        batch_acc.append(row)

        if len(batch_acc) >= BATCH_SIZE:
            inserted = duckdb_insert(batch_acc)
            logging.info(f"Inserted batch of {inserted} (acc flush).")
            batch_acc.clear()

    return False


async def main_async() -> None:
    setup_logging()
    logging.info("Starting async tender fetcher...")

    # starting offset: resume or fresh
    offset = load_offset() or mk_offset_param(YESTERDAY)
    logging.info(f"Initial offset: {offset}")

    print(offset)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TOTAL_TIMEOUT)
    headers = {
        # keep a friendly UA; replicate your sync headers if you had them
        "User-Agent": "UZ Prozorro async-fetcher"
    }

    batch_acc: List[Dict[str, Any]] = []
    pages = 0
    tenders_seen = 0

    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        while True:
            url = f"{API_URL}{API_PATH}?offset={offset}"
            page = await fetch_json(session, url)
            if not page:
                logging.info("No page received (None). Stopping.")
                break

            data = page.get("data", [])
            if not data:
                logging.info("Empty data page. Stopping.")
                break

            pages += 1
            tenders_seen += len(data)
            logging.info(f"Page {pages}: received {len(data)} tender meta items.")

            # Process this page (concurrently fetch details)
            stop = await process_page(session, data, batch_acc=batch_acc)
            if stop:
                logging.info("Stop condition reached; exiting pagination.")
                break

            # Move to next page
            next_offset = page.get("next_page", {}).get("offset")
            if not next_offset:
                logging.info("No next_page.offset; done.")
                break

            offset = next_offset
            save_offset(offset)

    # Final flush
    if batch_acc:
        inserted = duckdb_insert(batch_acc)
        logging.info(f"Inserted final batch of {inserted} rows.")
        batch_acc.clear()

    logging.info(f"Done. Pages: {pages}, meta items seen: {tenders_seen}")


if __name__ == "__main__":
    asyncio.run(main_async())
