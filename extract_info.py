import datetime as dt
import polars as pl


# 1.从wind"数据浏览器"选择etf_info
# 2.然后选择"基金/内地公募基金/基金市场类/基金市场类(净值)/上市基金/ETF基金(含未成立、已到期)"应用到模板
# 3.全选并复制到剪切板
def extract_info(target_dt: dt.date):
    df = (
        pl.read_clipboard(
            separator="\t",
            try_parse_dates=True,
            columns=[
                "证券代码",
                "证券简称",
                "跟踪指数代码",
                "投资类型(二级分类)",
                "管理费率[单位] %",
                "托管费率[单位] %",
                "上市日期",
                "基金到期日",
            ],
            new_columns=[
                "code",
                "name",
                "tracking",
                "type",
                "mer",
                "cer",
                "listdate",
                "maturitydate",
            ],
        )
        .filter(pl.col("listdate").is_not_null())
        .filter(pl.col("listdate") <= target_dt)
        .filter(pl.col("type") != "货币市场型基金")
        .select(
            pl.col("code").str.slice(0, 6).cast(pl.UInt32),
            "name",
            pl.col("tracking").str.to_uppercase(),
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
            .alias("type"),
            (pl.col("mer") + pl.col("cer")).round(2).alias("fees"),
            "listdate",
            "maturitydate",
        )
        .sort(["tracking", "fees", "listdate"])
    )
    df.write_ipc("etf_info.ipc")
    df.write_csv("etf_info.csv")
    print("dump etf_info.ipc & etf_info.csv")


if __name__ == "__main__":
    target_dt = dt.date.today()
    extract_info(target_dt)
