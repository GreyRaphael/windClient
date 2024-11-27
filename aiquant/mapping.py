import pickle
import polars as pl

with open("mapping.pkl", "rb") as file:
    mapping = pickle.load(file)


df = pl.read_ipc("20241126all.ipc")
dfx = df.with_columns(pl.col("code").cast(pl.Int64).replace_strict(mapping, default=None).alias("sector"))
dfx.write_ipc("20241126sector.ipc", compression="zstd")
