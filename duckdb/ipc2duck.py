import polars as pl
import duckdb

df = pl.read_ipc("20241126sector.ipc")

with duckdb.connect("bar1d.db", read_only=False) as conn:
    conn.execute("CREATE TABLE etf AS SELECT * FROM df")
