import polars as pl
from itertools import combinations
import networkx as nx

df_index_quotes = pl.read_ipc("tracking.ipc")
df_data = df_index_quotes.pivot(on="index", index="dt", values="close").sort("dt")

dfs = []
for pair_first, pair_second in combinations(df_data.columns[1:], 2):
    df = (
        df_data.select(pair_first, pair_second)
        .drop_nulls()
        .with_columns(
            pl.col(pair_first).pct_change(n=1),
            pl.col(pair_second).pct_change(n=1),
        )
        .select(
            pl.lit(pair_first).alias("first"),
            pl.lit(pair_second).alias("second"),
            pl.corr(pair_first, pair_second, method="pearson").alias("pearson"),
            pl.corr(pair_first, pair_second, method="spearman").alias("spearman"),
        )
    )
    dfs.append(df)


ratio = 0.91
df_corr = pl.concat(dfs).filter((pl.col("pearson") > ratio) & (pl.col("spearman") > ratio))

G = nx.Graph(df_corr.select("first", "second").rows())
related_list = list(nx.connected_components(G))

corr_list = list(set(df_corr["first"].unique().to_list()) | set(df_corr["second"].unique().to_list()))
non_related_list = df_index_quotes.select(pl.col("index").unique()).filter(~pl.col("index").is_in(corr_list))["index"].sort().to_list()
