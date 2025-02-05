import os
import datetime as dt
import logging
import polars as pl
from WindPy import w
from utils import chatbot


def get_logger(name: str, level=logging.DEBUG, fmt="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s"):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    os.makedirs("log", exist_ok=True)
    file_handler = logging.FileHandler(f'log/{dt.datetime.now().strftime("%Y%m%d-%H%M")}_{name}.log')
    formatter = logging.Formatter(fmt)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    return logger


def wind_ready() -> bool:
    # check wind connection 10 times
    for _ in range(10):
        w.start()
        if w.isconnected():
            return True
    return False


def get_lof_list(date_str: str) -> list[str]:
    # get all lof list(包含未上市和退市的)
    # 基金/内地公募基金/基金市场类/基金市场类(净值)/上市基金/LOF基金
    response = w.wset("sectorconstituent", f"date={date_str};sectorid=a201010a00000000;field=wind_code")
    if response.ErrorCode == 0:
        return response.Data[0]
    else:
        chatbot.send_msg(f"WindPy Errorcode: {response.ErrorCode} at {date_str}")
        return []


def process_data(data) -> pl.DataFrame:
    """
    fields of data:
        ErrorCode: int
        Codes: list[str]
        Fields: list[str]
        Times: list[dt.datetime]
        Data: list[list[float]]
    """
    length = len(data.Times)
    return (
        pl.from_records(
            [data.Codes * length] + [data.Times] + data.Data,
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
        .filter(pl.col("open").is_not_null())
        .select(
            pl.col("code").str.slice(0, 6).cast(pl.UInt32),
            "dt",
            (pl.col("preclose") * 1e4).round(0).cast(pl.UInt32),
            (pl.col("open") * 1e4).round(0).cast(pl.UInt32),
            (pl.col("high") * 1e4).round(0).cast(pl.UInt32),
            (pl.col("low") * 1e4).round(0).cast(pl.UInt32),
            (pl.col("close") * 1e4).round(0).cast(pl.UInt32),
            pl.col("volume").fill_null(0).cast(pl.UInt64),
            (pl.col("amount") * 1e4).round(0).fill_null(0).cast(pl.UInt64),
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
    wind_logger = get_logger("wind")
    out_dir = "wind-lof-bar1d"
    os.makedirs(out_dir, exist_ok=True)
    today_str = today.strftime("%Y-%m-%d")
    week_days = collect_trade_days(today)

    wind_logger.info(f"begin wind task {week_days[0]}~{week_days[-1]}")

    if wind_ready():
        lof_list = get_lof_list(today_str)
        wind_logger.info(f"lof length={len(lof_list)} at {today_str}")
        df_list = []
        for code in lof_list:
            wind_logger.debug(f"begin download {code}")
            wind_data = download(code, week_days[0], week_days[-1])
            df = process_data(wind_data)
            df_list.append(df)
            wind_logger.debug(f"finish download {code}")

        if len(df_list) > 0:
            pl.concat(df_list).write_ipc(f"{out_dir}/{today_str}.ipc", compression="zstd")

        wind_logger.info(f"finish wind task {week_days[0]}~{week_days[-1]}")
        chatbot.send_msg(f"finish wind task {week_days[0]}~{week_days[-1]}")
    else:
        # print("WindPy not ready")
        chatbot.send_msg("WindPy not ready")


def lastday_worker(today: dt.date | dt.datetime):
    wind_logger = get_logger("wind")
    out_dir = "wind-lof-bar1d"
    os.makedirs(out_dir, exist_ok=True)
    lastday = today - dt.timedelta(days=1)
    lastday_str = lastday.strftime("%Y-%m-%d")

    wind_logger.info(f"begin lof task {lastday_str}")

    if wind_ready():
        lof_list = get_lof_list(lastday_str)
        wind_logger.info(f"lof length={len(lof_list)} at {lastday_str}")
        df_list = []
        for code in lof_list:
            wind_logger.debug(f"begin download {code}")
            wind_data = download(code, lastday_str, lastday_str)
            df = process_data(wind_data)
            df_list.append(df)
            wind_logger.debug(f"finish download {code}")

        if len(df_list) > 0:
            pl.concat(df_list).write_ipc(f"{out_dir}/lof{lastday_str}.ipc", compression="zstd")

        wind_logger.info(f"finish wind task {lastday_str}")
        chatbot.send_msg(f"finish wind task {lastday_str}")
    else:
        # print("WindPy not ready")
        chatbot.send_msg("WindPy not ready")


if __name__ == "__main__":
    # job_worker(today=dt.date.today())
    lastday_worker(today=dt.date.today())
    # job_worker(today=dt.date(2024, 6, 15))
