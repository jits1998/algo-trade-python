"""
Telegram NFO Candle Downloader & Ingester
==========================================
Downloads daily 1-min OHLCV feather files from a public Telegram channel
and ingests them into QuestDB table `nobroker_candles`.

Channel posts one feather file per day:
    <date>-index-nfo-data.feather  —  1-min OHLCV for all NFO instruments

CLI:
    python TelegramTickDownloader.py --ingest-date 2026-04-10
    python TelegramTickDownloader.py --ingest-latest

Config (config/server.json):
    "telegram": {
        "api_id": 12345678,
        "api_hash": "abc...",
        "channel": "nfo_data"
    }
"""

import asyncio
import io
import json
import logging
import zipfile
from datetime import datetime, date, timezone, timedelta
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def _load_server_config() -> dict:
    config_path = Path(__file__).parent.parent.parent / "config" / "server.json"
    with open(config_path) as f:
        return json.load(f)


def _get_questdb_conn():
    import psycopg2
    server = _load_server_config()
    q = server["questDB"]
    return psycopg2.connect(
        host=q["host"], port=q["port"],
        user=q["username"], password=q["password"],
        database=q["database"],
    )


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
def _rows_exist(conn, target_date: date) -> bool:
    day_start = target_date.strftime("%Y-%m-%dT00:00:00")
    day_end = target_date.strftime("%Y-%m-%dT23:59:59")
    with conn.cursor() as cur:
        try:
            cur.execute(
                "SELECT count() FROM nobroker_candles WHERE ts >= %s AND ts <= %s",
                (day_start, day_end),
            )
            return cur.fetchone()[0] > 0
        except Exception:
            # Table doesn't exist yet
            return False


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

def _parse_feather(buf: io.BytesIO) -> pd.DataFrame:
    """
    Parse *-index-nfo-data.feather.
    Source cols: date, open, high, low, close, volume, oi, symbol, name, expiry, strike, instrument_type
    """
    df = pd.read_feather(buf)
    df = df.rename(columns={"date": "ts", "symbol": "trading_symbol"})
    df["ts"] = pd.to_datetime(df["ts"], utc=False).dt.tz_convert(IST).dt.floor("min")
    df["expiry"] = df["expiry"].astype(str)
    return df[["ts", "trading_symbol", "open", "high", "low", "close",
               "volume", "oi", "name", "expiry", "strike", "instrument_type"]]


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

def _questdb_http_base() -> str:
    server = _load_server_config()
    q = server["questDB"]
    return f"http://{q['host']}:9000"


def _ingest(df: pd.DataFrame, target_date: date, force: bool = False) -> int:
    import urllib.request
    import urllib.parse

    # Check for existing data
    if not force:
        conn = _get_questdb_conn()
        try:
            if _rows_exist(conn, target_date):
                print(f"  nobroker_candles already has data for {target_date}, skipping.")
                return 0
        finally:
            conn.close()

    # Bulk-insert via QuestDB HTTP /imp endpoint (CSV upload) in chunks
    # Timestamp must be ISO8601 UTC format: YYYY-MM-DDTHH:MM:SS.000000Z
    export = df.copy()
    export["ts"] = export["ts"].dt.strftime("%Y-%m-%dT%H:%M:%S.000000Z")

    base = _questdb_http_base()
    url = f"{base}/imp?name=nobroker_candles&timestamp=ts&partitionBy=DAY&durable=true&overwrite=false"

    CHUNK_SIZE = 50_000
    total = len(export)
    inserted = 0

    for chunk_start in range(0, total, CHUNK_SIZE):
        chunk = export.iloc[chunk_start: chunk_start + CHUNK_SIZE]
        csv_buf = io.BytesIO()
        chunk.to_csv(csv_buf, index=False)
        csv_bytes = csv_buf.getvalue()

        boundary = "boundary123456"
        body = (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="data"; filename="data.csv"\r\n'
            f"Content-Type: text/csv\r\n\r\n"
        ).encode() + csv_bytes + f"\r\n--{boundary}--\r\n".encode()

        req = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=120) as resp:
            result = resp.read().decode()
            if "Rows imported" not in result:
                raise RuntimeError(f"QuestDB /imp error: {result[:300]}")

        inserted += len(chunk)
        print(f"  {inserted:,}/{total:,} rows uploaded...", end="\r")

    print()
    return inserted


# ---------------------------------------------------------------------------
# Telegram helpers
# ---------------------------------------------------------------------------

def _get_filename(msg) -> str | None:
    try:
        for attr in msg.media.document.attributes:
            if hasattr(attr, "file_name"):
                return attr.file_name
    except Exception:
        pass
    return None


async def _download_buf(client, msg) -> io.BytesIO:
    buf = io.BytesIO()
    await client.download_media(msg, file=buf)
    buf.seek(0)
    return buf


async def _find_feather_for_date(client, channel: str, target_date: date):
    """Return the feather message for target_date, or None."""
    from telethon.tl.types import MessageMediaDocument
    async for msg in client.iter_messages(channel, limit=200):
        if not (msg.media and isinstance(msg.media, MessageMediaDocument)):
            continue
        msg_date = msg.date.astimezone(IST).date()
        if msg_date == target_date:
            fname = _get_filename(msg) or ""
            if fname.endswith(".feather"):
                return msg
        elif msg_date < target_date:
            break
    return None


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

async def ingest_date(target_date: date, force: bool = False):
    server = _load_server_config()
    tg = server["telegram"]
    from telethon import TelegramClient
    session_path = str(Path(server["deployDir"]) / "telegram_session")
    client = TelegramClient(session_path, tg["api_id"], tg["api_hash"])

    async with client:
        msg = await _find_feather_for_date(client, tg["channel"], target_date)
        if msg is None:
            print(f"No feather file found for {target_date}")
            return

        fname = _get_filename(msg)
        print(f"Found: {fname}  ({msg.media.document.size / 1e6:.1f} MB)")
        print("Downloading...")
        buf = await _download_buf(client, msg)

        print("Parsing...")
        df = _parse_feather(buf)
        print(f"Parsed {len(df):,} rows  ({df['trading_symbol'].nunique()} symbols)")

        n = _ingest(df, target_date, force=force)
        if n:
            print(f"Inserted {n:,} rows into nobroker_candles")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args():
    import argparse
    p = argparse.ArgumentParser(description="Ingest Telegram NFO candles into QuestDB (nobroker_candles)")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--ingest-date", metavar="YYYY-MM-DD", help="Ingest data for a specific date")
    group.add_argument("--ingest-latest", action="store_true", help="Ingest the most recent available day")
    p.add_argument("--force", action="store_true", help="Re-ingest even if data already exists")
    return p.parse_args()


async def _main():
    args = _parse_args()
    if args.ingest_latest:
        target = (datetime.now(IST) - timedelta(days=1)).date()
    else:
        target = datetime.strptime(args.ingest_date, "%Y-%m-%d").date()

    print(f"Ingesting candles for {target}{' (force)' if args.force else ''}")
    await ingest_date(target, force=args.force)
    print("Done.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(_main())
