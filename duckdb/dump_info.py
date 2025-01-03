import argparse
import pickle as pkl
import polars as pl
import duckdb


def read_sector_mapping(security_type: str) -> dict:
    with open(f"sector-{security_type}.pkl", "rb") as file:
        mapping = pkl.load(file)
    return mapping


def dump_info(security_type: str):
    df = pl.read_ipc(f"feilv-{security_type}.ipc")  # code, name, mer, cer
    normal_mapping = read_sector_mapping(security_type)  # {code:sector}
    japan_mapping = {
        159866: 918,
        513000: 918,
        513520: 918,
        513800: 918,
        513880: 918,
    }
    mapping = normal_mapping | japan_mapping
    df_new = df.with_columns(pl.col("code").cast(pl.Int64).replace_strict(mapping, default=None).alias("sector"))
    with duckdb.connect(f"{security_type}.db", read_only=False) as con:
        con.execute("CREATE TABLE info AS SELECT * FROM df_new")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="dump info table to duckdb")
    parser.add_argument("-st", type=str, required=True, choices=["lof", "etf"], help="security type")
    args = parser.parse_args()
    dump_info(args.st)
