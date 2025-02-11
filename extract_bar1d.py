import polars as pl
import argparse
import glob
import datetime as dt


# 使用wind行情序列仅仅导出"换手率"，其他字段均由aiquant导出
# 使用wind导出的时候，不要选择复权净值
def extract_quotes(indir: str, aiquant_file: str):
    # read from wind csv files: code, dt, close, turnover, netvalue
    df_wind = (
        pl.concat(
            [
                pl.read_csv(
                    filename,
                    encoding="gbk",
                    columns=[
                        "代码",
                        "日期",
                        "收盘价(元)",
                        "换手率(%)",
                        "单位净值(元)",
                    ],
                    schema_overrides={
                        "代码": pl.Utf8,
                        "日期": pl.Date,
                        "收盘价(元)": pl.Float64,
                        "换手率(%)": pl.Float64,
                        "单位净值(元)": pl.Float64,
                    },
                    new_columns=[
                        "code",
                        "dt",
                        "close",
                        "turnover",
                        "netvalue",
                    ],
                )
                for filename in glob.glob(f"{indir}/*")
            ]
        )
        .with_columns(
            pl.col("code").str.slice(0, 6).cast(pl.UInt32),
            (pl.col("close") * 1e4).round(0).cast(pl.UInt32),
            (pl.col("netvalue") * 1e4).round(0).cast(pl.UInt32),
        )
        .sort(by=["code", "dt"])
    )
    # read from aiquant: code, dt, preclose, open, high, low, close, volume, amount, trades_count, adjfactor
    df_aiquant = pl.read_ipc(aiquant_file)
    # join data & turnover -> preunit
    df = (
        df_aiquant.join(df_wind, on=["code", "dt", "close"])
        .with_columns(
            (pl.col("volume") / pl.col("turnover") * 1e2).alias("preunit"),
        )
        .with_columns(
            pl.when(pl.col("preunit").is_infinite()).then(None).otherwise("preunit").alias("preunit"),
        )
        .with_columns(
            pl.col("preunit").fill_null(strategy="forward").over("code").round(0).cast(pl.UInt64),
        )
        .select(pl.exclude("turnover"))
    )
    last_dt = df.item(-1, 1)
    df.write_ipc(f"etf-bar1d-until-{last_dt}.ipc", compression="zstd")


# 使用wind数据浏览器导出daily_etf_single
# join wind的 aiq-etf-bar1d-2025-02-07.ipc
def extract_single(wind_single: str, aiquant_single: str):
    df_ai = pl.read_ipc(aiquant_single)
    target_dt = df_ai.item(-1, 1)
    df_wind = (
        pl.read_csv(
            wind_single,
            separator="\t",
            infer_schema_length=1000,
            schema_overrides={"换手率[交易日期] 最新收盘日[单位] %": pl.Utf8},
            new_columns=["code", "name", "close", "turnover", "netvalue"],
        )
        .filter(pl.col("close").is_not_null())
        .with_columns(
            pl.col("turnover").str.replace_all(",", "").cast(pl.Float64),
            (pl.col("close") * 1e4).round(0).cast(pl.UInt32),
            (pl.col("netvalue") * 1e4).round(0).cast(pl.UInt32),
            pl.col("code").str.slice(0, 6).cast(pl.UInt32),
            pl.lit(target_dt).alias("dt"),
        )
        .select("code", "dt", "close", "turnover", "netvalue")
    )

    df_ai.join(df_wind, on=["code", "dt", "close"]).write_ipc(f"etf1d_{target_dt}.ipc")


def process(args):
    if args.type == "single":
        extract_single(args.wi, args.ai)
    elif args.type == "all":
        extract_quotes(args.wi, args.ai)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="提取万得行情序列csv的数据")
    parser.add_argument("-wi", type=str, required=True, help="万得导出csv的目录, like G:/wind_etf_bar1d")
    parser.add_argument("-ai", type=str, required=True, help="aiquant导出的文件, like aiq-etf-bar1d-until-2025-02-07.ipc")
    parser.add_argument("-type", type=str, default="single", choices=["single", "all"], help="combine single | all quote")
    args = parser.parse_args()
    process(args)
