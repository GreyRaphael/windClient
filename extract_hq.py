import polars as pl
import argparse
import os


def extract_hq(indir: str, outdir: str):
    os.makedirs(outdir, exist_ok=True)
    df = (
        pl.scan_csv(
            f"{indir}/*",
            skip_rows=1,
            has_header=False,
            schema={
                "code": pl.Utf8,
                "name": pl.Utf8,
                "dt": pl.Date,
                "preclose": pl.Float64,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.UInt64,
                "amount": pl.Float64,
                "turnover": pl.Float64,
                "netvalue": pl.Float64,
                "useless": pl.UInt8,
            },
        )
        .select(
            pl.col("code").str.slice(0, 6).cast(pl.UInt32),
            "dt",
            (pl.col("preclose") * 1e4).cast(pl.UInt32),
            (pl.col("open") * 1e4).cast(pl.UInt32),
            (pl.col("high") * 1e4).cast(pl.UInt32),
            (pl.col("low") * 1e4).cast(pl.UInt32),
            (pl.col("close") * 1e4).cast(pl.UInt32),
            pl.col("volume").fill_null(0).cast(pl.UInt64),
            (pl.col("amount") * 1e4).fill_null(0).cast(pl.UInt64),
            "turnover",
            (pl.col("netvalue") * 1e4).cast(pl.UInt32),
        )
        .collect()
    )

    # group_by and write to file
    for (gp_name,), gp_df in df.group_by(pl.col("dt").dt.year()):
        out_file = f"{outdir}/{gp_name}.ipc"
        gp_df.sort(["code", "dt"]).write_ipc(out_file, compression="zstd")
        print(f"write to {out_file}, shape={gp_df.shape}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="提取万得行情序列csv的数据")
    parser.add_argument("-indir", type=str, required=True, help="万得导出的目录, like D:/wind_etf_bar1d")
    parser.add_argument("-outdir", type=str, required=True, help="ipc output dir")
    args = parser.parse_args()
    extract_hq(args.indir, args.outdir)
