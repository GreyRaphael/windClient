import argparse
import polars as pl


# 从wind"数据浏览器"导出etf_info模板的数据,粘贴即可，然后进行处理
def extract_info(infile: str):
    df = (
        pl.read_csv(infile, separator="\t")
        .filter(pl.col("跟踪指数代码").is_not_null())
        .filter(pl.col("上市日期").is_not_null())
        .select(
            pl.col("证券代码").str.slice(0, 6).cast(pl.UInt32).alias("code"),
            pl.col("证券简称").alias("name"),
            pl.col("跟踪指数代码").alias("tracking"),
            pl.col("投资类型(二级分类)").alias("type"),
            (pl.col("管理费率[单位] %") + pl.col("托管费率[单位] %")).alias("fees"),
            pl.col("上市日期").cast(pl.Date).alias("listdate"),
            pl.col("基金到期日").cast(pl.Date).alias("maturitydate"),
        )
        .with_columns(
            pl.when(pl.col("type") == "商品型基金")
            .then(pl.lit("commodity"))
            .when(pl.col("type") == "国际(QDII)股票型基金")
            .then(pl.lit("qdii"))
            .when(pl.col("type") == "增强指数型基金")
            .then(pl.lit("enhanced_index"))
            .when(pl.col("type") == "被动指数型基金")
            .then(pl.lit("index"))
            .when(pl.col("type") == "被动指数型债券基金")
            .then(pl.lit("bond"))
            .alias("type")
        )
        .sort(["tracking", "fees", "listdate"])
    )
    df.write_ipc("etf_info.ipc")
    df.write_csv("etf_info.csv")
    print("dump etf_info.ipc & etf_info.csv")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="提取wind数据浏览器中的etf_info")
    parser.add_argument("-wi", type=str, required=True, help="粘贴数据之后的csv, like wind_info.csv")
    args = parser.parse_args()
    extract_info(args.wi)
