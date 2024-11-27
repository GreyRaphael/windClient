# WindCilent

- [WindCilent](#windcilent)

How to get etf bar1d
- 获取wind的etf日行情
  1. wind客户端/基金/行情序列/基金/内地公募基金/基金市场类/基金市场类(净值)/上市基金/ETF基金(含未成立、已到期)，板块号(**1000051239000000**)
  2. 选择**不复权**指标: open,high,low,close,preclose,volume,amount,turnover,netvalue
  3. **上市日**到目标日期，导出为大量csv
  4. 运行 `python extract_bar1d.py -indir -outdir`, 生成`wind-bar1d-2024-11-26.ipc`
- 获取aiquant日行情
  1. 使用windapi导出目标日期etf列表 `python extract_list.py -dt -sid`
  2. 在aiquant jupyter notebook中使用 `aiquant/etf_bar1d.py`的函数生成 `aiquant-bar1d-2024-11-26.ipc`
  3. 因为ipc比较小，可以直接download
- join合并字段: `df_wind.join(df_ai, on=['code', 'dt', 'close'], how='inner').write_ipc('20241126-etf-bar1d.ipc', compression='zstd')`