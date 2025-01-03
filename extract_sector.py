from WindPy import w
import argparse
import pickle

SECTOR_DICT = {
    "etf": [
        1000009712000000,  # 规模指数ETF
        1000009713000000,  # 行业指数ETF
        1000009714000000,  # 策略指数ETF
        1000009715000000,  # 风格指数ETF
        1000009716000000,  # 主题指数ETF
        1000009166000000,  # 债券ETF
        1000010087000000,  # 商品ETF
        1000032560000000,  # 货币ETF
        1000056319000000,  # 跨境规模指数ETF
        1000056320000000,  # 跨境行业指数ETF
        1000056321000000,  # 跨境策略指数ETF
        1000056322000000,  # 跨境主题指数ETF
    ],
    "lof": [
        1000043333000000,  # 股票
        1000043334000000,  # 混合
        1000043335000000,  # 债券
        1000043338000000,  # FOF
        1000043336000000,  # 另类
        1000043337000000,  # QDII
    ],
}


def wind_ready() -> bool:
    # check wind connection 10 times
    for _ in range(10):
        w.start()
        if w.isconnected():
            return True
    return False


def get_sector_list(date_str: str, sectorid: int) -> list[str]:
    response = w.wset("sectorconstituent", f"date={date_str};sectorid={sectorid};field=wind_code")
    if response.ErrorCode == 0:
        return response.Data[0]
    else:
        print(f"WindPy Errorcode: {response.ErrorCode} at {date_str}")
        return []


def generate_map(date_str: str, security_type: str) -> dict:
    if wind_ready():
        mapping = {}
        for sector in SECTOR_DICT[security_type]:
            for code in get_sector_list(date_str, sector):
                code_int = int(code[:6])
                mapping[code_int] = sector
        # as key is integer, json file not support that
        output_file = f"sector-{security_type}.pkl"
        with open(output_file, "wb") as file:
            pickle.dump(mapping, file)
        print(f"dump to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="提取万得板块映射")
    parser.add_argument("-dt", type=str, required=True, help="目标日期 2024-11-26")
    parser.add_argument("-st", type=str, required=True, choices=["lof", "etf"], help="security type")
    args = parser.parse_args()
    generate_map(args.dt, args.st)
