import polars as pl
import argparse

# wind数据浏览器，选择管理费率和托管费率，导出csv文件


def extract(csv_file: str, security_type: str):
    df = pl.read_csv(csv_file, skip_rows=1, has_header=False, new_columns=["code", "name", "mer", "cer"])
    output_file = f"feilv-{security_type}.ipc"
    df.with_columns(pl.col("code").str.slice(0, 6).cast(pl.UInt32)).write_ipc(output_file)
    print(f"dump to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="extract feilv")
    parser.add_argument("-csv", type=str, required=True, help="csv file like EDE20250103.csv")
    parser.add_argument("-st", type=str, required=True, choices=["lof", "etf"], help="security type")

    args = parser.parse_args()
    extract(args.csv, args.st)
