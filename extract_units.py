import glob
import argparse
import polars as pl


def extract_units(indir: str):
    dfs = []
    for filename in glob.glob(f"{indir}/*"):
        code = filename[-11:-5]
        df = (
            pl.read_excel(filename, read_options={"header_row": 1})
            .drop_nulls()
            .select(
                pl.col("日期").cast(pl.Date).alias("dt"),
                pl.nth(1).cast(pl.UInt64).alias("unit"),
                pl.lit(int(code)).cast(pl.UInt32).alias("code"),
            )
        )
        dfs.append(df)
    df_units = pl.concat(dfs).sort(["code", "dt"])
    df_units.write_ipc("wind_units.ipc", compression="zstd")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="提取wind数据浏览器导出的xlsx基金份额")
    parser.add_argument("-wi", type=str, required=True, help="万得导出xlsx的目录, like G:/wind_units")
    args = parser.parse_args()
    extract_units(args.wi)
