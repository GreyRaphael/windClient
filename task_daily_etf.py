import datetime as dt
import json
import polars as pl
from WindPy import w

w.start()
w.isconnected()


def wsd_download_daily(codes: list[str], start_dt: dt.date, end_dt: dt.date):
    # codes like [510050.OF,]
    dfs = []
    codes_failed = []
    for code in codes:
        record = w.wsd(code, "unit_floortrading,nav", start_dt, end_dt, "unit=1")
        if record.ErrorCode == 0:
            length = len(record.Times)
            df = pl.from_records(
                [record.Codes * length] + [record.Times] + record.Data,
                schema={"code": pl.Utf8, "dt": pl.Date, "unit": pl.Float64, "netvalue": pl.Float64},
            ).with_columns(
                pl.col("code").str.slice(0, 6).cast(pl.UInt32),
                (pl.col("netvalue") * 1e4).round().cast(pl.UInt32),
            )
            dfs.append(df)
        else:
            print(f"{code} failed, from {start_dt} to {end_dt}")
            codes_failed.append(code)

    if codes_failed:
        with open(f"failed_{start_dt}_{end_dt}.json", "w") as file:
            json.dump(codes_failed, file)

    return pl.concat(dfs)


def wsd_download_unit(codes: list[str], start_dt: dt.date, end_dt: dt.date):
    # codes like [510050.OF,]
    dfs = []
    codes_failed = []
    for code in codes:
        record = w.wsd(code, "unit_floortrading", start_dt, end_dt, "unit=1")
        if record.ErrorCode == 0:
            length = len(record.Times)
            df = pl.from_records(
                [record.Codes * length] + [record.Times] + record.Data,
                schema={"code": pl.Utf8, "dt": pl.Date, "unit": pl.Float64},
            ).with_columns(
                pl.col("code").str.slice(0, 6).cast(pl.UInt32),
            )
            dfs.append(df)
        else:
            print(f"{code} failed, from {start_dt} to {end_dt}")
            codes_failed.append(code)

    if codes_failed:
        with open(f"failed_{start_dt}_{end_dt}.json", "w") as file:
            json.dump(codes_failed, file)

    return pl.concat(dfs)


if __name__ == "__main__":
    codes = ["510050.OF", "159901.OF"]
    target_dt = dt.date.today()
    wsd_download_daily(codes, target_dt)
