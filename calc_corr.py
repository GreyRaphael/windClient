import polars as pl
from itertools import combinations


df_index_quotes = pl.read_ipc("tracking.ipc")
df_data = df_index_quotes.pivot(on="index", index="dt", values="close").sort("dt")

dfs = []
for pair_first, pair_second in combinations(df_data.columns[1:], 2):
    df = (
        df_data.select(pair_first, pair_second)
        .drop_nulls()
        .select(
            pl.lit(pair_first).alias("first"),
            pl.lit(pair_second).alias("second"),
            pl.corr(pair_first, pair_second, method="pearson").alias("pearson"),
            pl.corr(pair_first, pair_second, method="spearman").alias("spearman"),
        )
    )
    dfs.append(df)

df_corr = pl.concat(dfs)
