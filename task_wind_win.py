import datetime as dt
import os
import polars as pl
from WindPy import w


def wind_ready() -> bool:
    for _ in range(10):
        w.start()
        if w.isconnected():
            return True
    return False


def get_etf_list(date_str: str) -> list[str]:
    # get all etf list(包含未上市和退市的)
    response = w.wset("sectorconstituent", f"date={date_str};sectorid=1000051239000000;field=wind_code")
    if response.ErrorCode == 0:
        return response.Data[0]
    else:
        print(f"WindPy error code: {response.ErrorCode}")
        return []


def process_data(data) -> pl.DataFrame:
    return (
        pl.from_records(
            [data.Codes * 4] + [data.Times] + data.Data,
            schema={
                "code": pl.Utf8,
                "dt": pl.Date,
                "preclose": pl.Float64,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
                "amount": pl.Float64,
                "turnover": pl.Float64,
                "discount": pl.Float64,
                "adjfactor": pl.Float64,
                "trades_count": pl.Float64,
            },
        )
        .fill_nan(None)
        .select(
            pl.col("code").str.slice(0, 6).cast(pl.UInt32),
            "dt",
            (pl.col("preclose") * 1e4).round(0).cast(pl.UInt32),
            (pl.col("open") * 1e4).round(0).cast(pl.UInt32),
            (pl.col("high") * 1e4).round(0).cast(pl.UInt32),
            (pl.col("low") * 1e4).round(0).cast(pl.UInt32),
            (pl.col("close") * 1e4).round(0).cast(pl.UInt32),
            pl.col("volume").cast(pl.UInt64),
            (pl.col("amount") * 1e4).round(0).cast(pl.UInt64),
            "turnover",
            ((pl.col("close") - pl.col("discount")) * 1e4).round(0).cast(pl.UInt32).alias("netvalue"),
            "adjfactor",
            pl.col("trades_count").cast(pl.UInt32),
        )
    )


def download(code: str, start_date: str, end_date: str):
    cols = ["pre_close", "open", "high", "low", "close", "volume", "amt", "turn", "discount", "adjfactor", "dealnum"]
    return w.wsd(code, fields=cols, beginTime=start_date, endTime=end_date)


def collect_trade_days(current_dt: dt.date | dt.datetime) -> list[str]:
    """collect the trading days in the week of current date or datetime"""
    monday = current_dt - dt.timedelta(days=current_dt.weekday())
    this_workdays = [monday + dt.timedelta(days=i) for i in range(5)]
    return [i.strftime("%Y-%m-%d") for i in this_workdays]


def job_worker(today: dt.date | dt.datetime):
    out_dir = "wind-etf-bar1d"
    os.makedirs(out_dir, exist_ok=True)
    today_str = today.strftime("%Y-%m-%d")
    week_days = collect_trade_days(today)
    if wind_ready():
        etf_list = get_etf_list(today_str)
        print(f"get etf length = {len(etf_list)} at {today_str}")
        df_list = []
        for code in etf_list:
            print("begin", code)
            wind_data = download(code, week_days[0], week_days[-1])
            df = process_data(wind_data)
            df_list.append(df)
            print("finish", code)
        pl.concat(df_list).write_ipc(f"{out_dir}/{today_str}.ipc", compression="zstd")

    else:
        print("WindPy not ready")
        # chatbot.send_msg("WindPy not ready")


if __name__ == "__main__":
    # job_worker(today=dt.date.today())
    job_worker(today=dt.date(2024, 6, 15))
