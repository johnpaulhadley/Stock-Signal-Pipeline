"""Paths and a tiny helper for running .sql files against the DuckDB warehouse."""
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
RAW = DATA / "raw"
EXPORTS = DATA / "exports"
SQL = ROOT / "sql"
WAREHOUSE = DATA / "warehouse.duckdb"


def connect() -> duckdb.DuckDBPyConnection:
    DATA.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(WAREHOUSE))


def render(name: str, **params) -> str:
    """Read sql/<name> and fill {{placeholders}}. Values come from the pipeline, never from users."""
    sql = (SQL / name).read_text()
    for key, value in {"raw": RAW.as_posix(), **params}.items():
        sql = sql.replace("{{" + key + "}}", str(value))
    return sql


def run_sql(name: str, **params) -> None:
    with connect() as con:
        con.execute(render("setup.sql"))
        con.execute(render(name, **params))


def query_df(name: str, **params):
    with connect() as con:
        return con.execute(render(name, **params)).df()
