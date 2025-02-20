import datetime as dt
import json
import polars as pl
from WindPy import w

w.start()
w.isconnected()


def wss_download_close(target_dt: dt.date):
    # 非货币 ETF
    codes_resp = w.wset("sectorconstituent", f"date={target_dt};sectorid=1000027480000000;field=wind_code")
    etf_list = codes_resp.Data[0]
    info_resp = w.wss(",".join(etf_list), "ipo_date,fund_trackindexcode")
    # 1. filter null ipo_date and <target_dt
    # 2. get index_list
    index_list = []
    fields_str = target_dt.strftime("tradeDate=%Y%m%d;priceAdj=U;cycle=D")
    quote_resp = w.wss(",".join(index_list), "close", fields_str)
    # save to ipc


if __name__ == "__main__":
    codes = ["510050.OF", "159901.OF"]
    target_dt = dt.date.today()
    wss_download_close(codes, target_dt)
