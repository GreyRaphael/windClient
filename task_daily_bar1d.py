import os
import datetime as dt
from WindPy import w
import polars as pl


def download_from_wind(codes: list[str], target_dt: dt.date):
    target_str = target_dt.strftime("tradeDate=%Y%m%d;priceAdj=U;cycle=D;unit=1")
    data = w.wss(",".join(codes), "pre_close,open,high,low,close,volume,amt,dealnum,adjfactor,nav,unit_floortrading", target_str)
    if data.ErrorCode != 0:
        print("error info:", data)
        exit()

    return (
        pl.from_records(
            [data.Codes] + data.Data,
            schema={
                "code": pl.Utf8,
                "preclose": pl.Float64,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
                "amount": pl.Float64,
                "trades_count": pl.Float64,
                "adjfactor": pl.Float64,
                "netvalue": pl.Float64,
                "float_unit": pl.Float64,
            },
        )
        .with_columns(
            pl.col("volume").fill_null(0).cast(pl.UInt64),
            (pl.col("amount") * 1e4).round(0).fill_null(0).cast(pl.UInt64),
        )
        .select(
            pl.col("code").str.slice(0, 6).cast(pl.UInt32),
            pl.lit(target_dt).alias("dt"),
            (pl.col("preclose") * 1e4).round(0).cast(pl.UInt32),
            (pl.col("open") * 1e4).round(0).cast(pl.UInt32),
            (pl.col("high") * 1e4).round(0).cast(pl.UInt32),
            (pl.col("low") * 1e4).round(0).cast(pl.UInt32),
            (pl.col("close") * 1e4).round(0).cast(pl.UInt32),
            "volume",
            "amount",
            pl.when(pl.col("volume") == 0).then(0).otherwise("trades_count").cast(pl.UInt32).alias("trades_count"),
            "adjfactor",
            (pl.col("netvalue") * 1e4).round(0).cast(pl.UInt32),
            "float_unit",
        )
        .sort(["code", "dt"])
    )


def worker(target_dt: dt.date):
    w.start()
    if w.isconnected():
        # ETF未成立+已到期: 1000051239000000
        codes_response = w.wset("sectorconstituent", f"date={target_dt};sectorid=1000051239000000;field=wind_code")
        # response.Data likst [['159001.OF', '159003.OF', '561200.OF',]]
        codes_line = ",".join(codes_response.Data[0])
        info_data = w.wss(codes_line, "fund_investtype,ipo_date,fund_maturitydate_2")
        df = (
            pl.from_records(
                [info_data.Codes] + info_data.Data,
                schema=["code", "type", "listdate", "maturitydate"],
            )
            .with_columns(pl.col("maturitydate").str.to_date())
            .filter(pl.col("type") != "货币市场型基金")
            .filter(pl.col("listdate").dt.year() > 1900)
            .filter(pl.col("listdate").dt.date() <= target_dt)
            .filter(pl.col("maturitydate").is_null() | (pl.col("maturitydate") >= target_dt))
        )

        code_strs = df["code"].to_list()  # use less codes
        df_result = download_from_wind(code_strs, target_dt)
        output_dir = f"etf-bar1d/{target_dt.year}"
        os.makedirs(output_dir, exist_ok=True)
        df_result.write_ipc(f"{output_dir}/etf1d_{target_dt}.ipc")
        print(f"finish etf1d task {target_dt}")
    else:
        print("WindPy not ready")


if __name__ == "__main__":
    # target_dt = dt.date.today() - dt.timedelta(days=1)
    target_dt = dt.date(2025, 2, 21)
    worker(target_dt)
