"""Dagster assets: raw ingestion -> clean -> marts -> Tableau extracts.

Raw layer   data/raw/...            files exactly as downloaded (plus injected mess, see make_messy)
Clean layer clean.prices            typed, deduplicated, one row per ticker per day
Mart layer  mart.fundamentals_ttm   one row per company per quarter, point-in-time
            mart.daily_signals      one row per ticker per day: factor scores + BUY/SELL/HOLD
"""
import io
import os
import tempfile
import zipfile
from datetime import date, timedelta

import duckdb
import exchange_calendars as xcals
import numpy as np
import pandas as pd
import requests
import yfinance as yf
from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    BackfillPolicy,
    DailyPartitionsDefinition,
    MaterializeResult,
    StaticPartitionsDefinition,
    asset,
    asset_check,
)

from .warehouse import EXPORTS, RAW, SQL, connect, query_df, render, run_sql

BENCHMARK = "SPY"
START = "2015-01-01"
WIKI_SP500 = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
SEC_FSDS = "https://www.sec.gov/files/dera/data/financial-statement-data-sets/{q}.zip"

# One partition per calendar day (weekends/holidays load zero rows; monitoring knows which days to expect).
DAILY = DailyPartitionsDefinition(start_date=START, timezone="America/New_York")


def _quarters() -> list[str]:
    """Every fully finished quarter since 2014 (a year of history before START for TTM math)."""
    out, today = [], date.today() - timedelta(days=10)
    for year in range(2014, today.year + 1):
        for q in range(1, 5):
            if date(year, 3 * q, 28) < today:
                out.append(f"{year}q{q}")
    return out


QUARTERS = StaticPartitionsDefinition(_quarters())


def _window(context: AssetExecutionContext) -> tuple[str, str]:
    """Inclusive date range for this run. A single-run backfill gets the whole range at once."""
    tw = context.partition_time_window
    return tw.start.date().isoformat(), (tw.end.date() - timedelta(days=1)).isoformat()


def _is_backfill(context: AssetExecutionContext) -> bool:
    return "dagster/backfill" in context.run.tags


# ---------------------------------------------------------------- reference data
@asset(group_name="reference")
def sp500_universe() -> MaterializeResult:
    """Current S&P 500 members with sector and SEC CIK (note: survivorship bias, see README)."""
    html = requests.get(WIKI_SP500, headers={"User-Agent": "stock-signal-pipeline"}, timeout=30).text
    df = pd.read_html(io.StringIO(html))[0]
    df = pd.DataFrame({
        "ticker": df["Symbol"].str.replace(".", "-", regex=False),  # BRK.B -> BRK-B (Yahoo format)
        "company": df["Security"],
        "sector": df["GICS Sector"],
        "cik": df["CIK"].astype(int),
    })
    with connect() as con:
        con.execute(render("setup.sql"))
        con.execute("CREATE OR REPLACE TABLE ref.sp500 AS SELECT * FROM df")
    return MaterializeResult(metadata={"companies": len(df)})


@asset(group_name="reference")
def trading_calendar() -> MaterializeResult:
    """NYSE sessions, so monitoring can tell a missing partition from a market holiday."""
    sessions = xcals.get_calendar("XNYS").sessions_in_range(START, date.today() + timedelta(days=365))
    df = pd.DataFrame({"session_date": sessions.date})
    with connect() as con:
        con.execute(render("setup.sql"))
        con.execute("CREATE OR REPLACE TABLE ref.trading_calendar AS SELECT * FROM df")
    return MaterializeResult(metadata={"sessions": len(df)})


# ---------------------------------------------------------------- raw layer
def make_messy(df: pd.DataFrame, seed: int) -> pd.DataFrame:
    """Simulate a sloppy upstream feed so the clean layer has real work to do."""
    rng = np.random.default_rng(seed)
    df = df.copy()
    n = len(df)
    df["trade_date"] = df["date"].dt.strftime("%Y-%m-%d")
    us = rng.random(n) < 0.2
    df.loc[us, "trade_date"] = df.loc[us, "date"].dt.strftime("%m/%d/%Y")          # mixed date formats
    df["close"] = df["close"].map("{:.4f}".format)
    dollars = rng.random(n) < 0.1
    df.loc[dollars, "close"] = "$" + df.loc[dollars, "close"].astype(float).map("{:,.2f}".format)  # "$1,234.50"
    lower = rng.random(n) < 0.1
    df.loc[lower, "ticker"] = " " + df.loc[lower, "ticker"].str.lower() + " "       # " aapl "
    dupes = df.sample(frac=0.02, random_state=seed)                                  # 2% duplicate rows
    return pd.concat([df, dupes], ignore_index=True)


@asset(partitions_def=DAILY, backfill_policy=BackfillPolicy.single_run(), deps=[sp500_universe], group_name="raw")
def raw_prices(context: AssetExecutionContext) -> MaterializeResult:
    """Daily adjusted close + volume from Yahoo Finance, written as one Parquet folder per day."""
    start, end = _window(context)
    tickers = query_df("util/tickers.sql")["ticker"].tolist() + [BENCHMARK]
    wide = yf.download(tickers, start=start, end=(date.fromisoformat(end) + timedelta(days=1)).isoformat(),
                       auto_adjust=True, progress=False, threads=True)
    if wide.empty:
        return MaterializeResult(metadata={"rows": 0})
    df = (wide[["Close", "Volume"]].stack(level=1, future_stack=True).reset_index()
          .set_axis(["date", "ticker", "close", "volume"], axis=1).dropna(subset=["close"]))
    df = make_messy(df, seed=int(start.replace("-", "")))
    for day, part in df.groupby(df["date"].dt.date):
        folder = RAW / "prices" / f"date={day}"
        folder.mkdir(parents=True, exist_ok=True)
        part.drop(columns="date").to_parquet(folder / "part.parquet", index=False)
    return MaterializeResult(metadata={"rows": len(df)})


@asset(partitions_def=QUARTERS, group_name="raw")
def raw_sec_filings(context: AssetExecutionContext) -> MaterializeResult:
    """SEC Financial Statement Data Sets: every 10-K/10-Q number filed that quarter (sub + num files)."""
    q = context.partition_key
    headers = {"User-Agent": os.environ["SEC_USER_AGENT"]}  # SEC requires "Name email@example.com"
    resp = requests.get(SEC_FSDS.format(q=q), headers=headers, timeout=300)
    resp.raise_for_status()
    out = RAW / "sec" / f"quarter={q}"
    out.mkdir(parents=True, exist_ok=True)
    # In-memory DuckDB, not the warehouse: several quarters can download at once without file locks.
    with tempfile.TemporaryDirectory() as tmp, duckdb.connect() as con:
        zipfile.ZipFile(io.BytesIO(resp.content)).extractall(tmp, members=["sub.txt", "num.txt"])
        for name in ("sub", "num"):
            # SEC quotes fields that contain tabs; strict_mode=false tolerates stray quotes in footnotes.
            src = (f"read_csv('{tmp}/{name}.txt', delim='\t', header=true, quote='\"', escape='\"', "
                   f"strict_mode=false, all_varchar=true)")
            cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()}
            extra = ", NULL::VARCHAR AS segments" if name == "num" and "segments" not in cols else ""
            con.execute(f"COPY (SELECT *{extra} FROM {src}) TO '{out}/{name}.parquet' (FORMAT parquet)")
    return MaterializeResult(metadata={"quarter": q})


# ---------------------------------------------------------------- clean + marts
@asset(partitions_def=DAILY, backfill_policy=BackfillPolicy.single_run(), deps=[raw_prices], group_name="clean")
def clean_prices(context: AssetExecutionContext) -> MaterializeResult:
    start, end = _window(context)
    if not any((RAW / "prices" / f"date={d.date()}").exists() for d in pd.date_range(start, end)):
        return MaterializeResult(metadata={"rows": 0})  # weekend / holiday
    run_sql("clean/prices.sql", start_date=start, end_date=end, is_backfill=str(_is_backfill(context)).lower())
    return MaterializeResult()


@asset(deps=[raw_sec_filings, sp500_universe], group_name="mart")
def fundamentals_ttm() -> MaterializeResult:
    run_sql("marts/fundamentals_ttm.sql")
    return MaterializeResult()


@asset(partitions_def=DAILY, backfill_policy=BackfillPolicy.single_run(),
       deps=[clean_prices, fundamentals_ttm], group_name="mart")
def daily_signals(context: AssetExecutionContext) -> MaterializeResult:
    start, end = _window(context)
    run_sql("marts/daily_signals.sql", start_date=start, end_date=end)
    return MaterializeResult()


@asset(deps=[daily_signals, trading_calendar], group_name="tableau")
def tableau_extracts() -> MaterializeResult:
    """Tableau Public can't connect to DuckDB, so each analysis query is exported as a CSV extract."""
    EXPORTS.mkdir(parents=True, exist_ok=True)
    files = sorted((SQL / "analysis").glob("*.sql")) + sorted((SQL / "monitoring").glob("*.sql"))
    for f in files:
        query_df(f"{f.parent.name}/{f.name}").to_csv(EXPORTS / f"{f.stem}.csv", index=False)
    return MaterializeResult(metadata={"extracts": len(files)})


# ---------------------------------------------------------------- data quality
@asset_check(asset=clean_prices, blocking=True)
def clean_prices_unique() -> AssetCheckResult:
    dupes = query_df("util/duplicate_prices.sql")["dupes"][0]
    return AssetCheckResult(passed=bool(dupes == 0), metadata={"duplicate_rows": int(dupes)})
