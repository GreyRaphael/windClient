import polars as pl
import argparse


def extract(csv_file: str):
    df = pl.read_csv(csv_file, skip_rows=1, has_header=False, new_columns=["code", "name", "mer", "cer"])
    df.with_columns(pl.col("code").str.slice(0, 6).cast(pl.UInt32)).write_ipc("feilv.ipc")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="extract feilv")
    parser.add_argument("-csv", type=str, required=True, help="csv file like EDE20250103.csv")

    args = parser.parse_args()
    extract(args.csv)
    print(f"finish processing {args.csv}")
