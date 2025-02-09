import polars as pl
import argparse
import glob

# 使用wind行情序列仅仅导出"换手率"，其他字段均由aiquant导出
# 使用wind导出的时候，不要选择复权净值


def extract_quote(indir: str, aiquant_file: str):
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="提取万得行情序列csv的数据")
    parser.add_argument("-wi", type=str, required=True, help="万得导出csv的目录, like G:/wind_etf_bar1d")
    parser.add_argument("-ai", type=str, required=True, help="aiquant导出的文件, like aiq-etf-bar1d-until-2025-02-07.ipc")
    args = parser.parse_args()
    extract_quote(args.wi, args.ai)
