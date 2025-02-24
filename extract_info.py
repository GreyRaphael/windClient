import datetime as dt
import polars as pl


# 1.从wind"数据浏览器"选择etf_info
# 2.然后选择"基金/内地公募基金/基金市场类/基金市场类(净值)/上市基金/ETF基金(含未成立、已到期)"应用到模板
# 2.然后选择"基金/内地公募基金/基金市场类/中国上市ETF/跨境ETF"应用到模板
# 3.全选并复制到剪切板
def get_from_clipboard(target_dt: dt.date = dt.date.today()):
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
    df.write_csv("etf_info.csv")
    print("dump etf_info.csv")


def get_from_api(target_dt: dt.date = dt.date.today()):
    import json
    from WindPy import w

    w.start()
    if w.isconnected():
        # 未成立+已到期: 1000051239000000
        codes_response = w.wset("sectorconstituent", f"date={target_dt};sectorid=1000051239000000;field=wind_code")
        # response.Data likst [['159001.OF', '159003.OF', '561200.OF',]]
        codes = codes_response.Data[0]
        info_data = w.wss(",".join(codes), "fund_info_name,fund_trackindexcode,fund_investtype,fund_managementfeeratio,fund_custodianfeeratio,ipo_date,fund_maturitydate_2")
        df = (
            pl.from_records(
                [info_data.Codes] + info_data.Data,
                schema=["code", "name", "tracking", "type", "mer", "cer", "listdate", "maturitydate"],
            )
            .filter(pl.col("type") != "货币市场型基金")
            .filter(pl.col("listdate").dt.year() > 1900)
            .filter(pl.col("listdate").dt.date() <= target_dt)
        )
        # dump list to json
        with open("etf_list.json", "w") as f:
            json.dump(df["code"].unique().sort().to_list(), f, indent=2)
        print("write etf_list.json success!")
        # dump info to csv
        dfx = df.select(
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
            pl.col("listdate").dt.date(),
            pl.col("maturitydate").str.to_date(),
        ).sort(["tracking", "fees", "listdate"])
        dfx.write_csv("etf_info.csv")
        print("dump info to etf_info.csv")

    else:
        print("WindPy not ready")


if __name__ == "__main__":
    get_from_api()
