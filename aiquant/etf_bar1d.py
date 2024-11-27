import polars as pl
import json


# read 1000051239000000_2024-11-26.json
def read_etf_list(filename: str):
    with open(filename, "r") as file:
        return [f"{code}.SHA" if code > 500000 else f"{code}.SZA" for code in json.load(file)]


def down_etf_bar1d(target_date: str):
    etf_list = read_etf_list(f"1000051239000000_{target_date}.json")
    arrow = DataSource("CHINACLOSEDFUNDEODPRICE").read(instruments=etf_list, end_date=target_date, bdb=True)
    df = (
        pl.from_arrow(arrow)
        .select(
            pl.col("s_info_windcode").str.slice(0, 6).cast(pl.UInt32).alias("code"),
            pl.col("date").dt.date().alias("dt"),
            pl.col("trades_count").cast(pl.UInt32),
            (pl.col("s_dq_close") * 1e4).round(0).cast(pl.UInt32).alias("close"),
            pl.col("s_dq_adjfactor").alias("adjfactor"),
            "opdate",
        )
        .with_columns(pl.col("opdate").max().over(["code", "dt"]).alias("max_opdate"))
        .filter(pl.col("opdate") == pl.col("max_opdate"))
        .select(pl.exclude(["opdate", "max_opdate"]))
        .sort(["code", "dt"])
        .write_ipc(f"aiquant-{target_date}.ipc", compression="zstd")
    )


if __name__ == "__main__":
    down_etf_bar1d("2024-11-26")
