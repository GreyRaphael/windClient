import polars as pl
from WindPy import w
import datetime as dt

w.start()
w.isconnected()


def wsd_download_daily(codes: list[str], target_dt: dt.date):
    # codes like [510050.OF,]
    dfs = []
    for code in codes:
        record = w.wsd(code, "unit_floortrading,nav", target_dt, target_dt, "unit=1")
        df = pl.from_records([record.Codes] + [record.Times] + record.Data, schema={"code": pl.Utf8, "dt": pl.Date, "unit": pl.Float64, "netvalue": pl.Float64}).with_columns(
            pl.col("code").str.slice(0, 6).cast(pl.UInt32),
            (pl.col("netvalue") * 1e4).round().cast(pl.UInt32),
        )
        dfs.append(df)
    return pl.concat(dfs)


def wsd_download_unit(codes: list[str], target_year: int):
    # codes like [510050.OF,]
    dfs = []
    for code in codes:
        record = w.wsd(code, "unit_floortrading", f"{target_year}-01-01", f"{target_year}-12-31", "unit=1")
        df = pl.from_records([record.Codes] + [record.Times] + record.Data, schema={"code": pl.Utf8, "dt": pl.Date, "unit": pl.Float64}).with_columns(
            pl.col("code").str.slice(0, 6).cast(pl.UInt32),
        )
        dfs.append(df)
    return pl.concat(dfs)


if __name__ == "__main__":
    codes = ["510050.OF", "159901.OF"]
    target_dt = dt.date.today()
    wsd_download_daily(codes, target_dt)
