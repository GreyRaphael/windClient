from WindPy import w
import json
import argparse


def wind_ready() -> bool:
    # check wind connection 10 times
    for _ in range(10):
        w.start()
        if w.isconnected():
            return True
    return False


def get_code_list(date_str: str, sectorid: int) -> list[str]:
    response = w.wset("sectorconstituent", f"date={date_str};sectorid={sectorid};field=wind_code")
    if response.ErrorCode == 0:
        # ['510050.SH', '159985.SZ',...]
        return response.Data[0]
    else:
        print(f"WindPy Errorcode: {response.ErrorCode} at {date_str}")
        return []


def get_list(date_str: str, sectorid: int):
    if wind_ready():
        code_list = get_code_list(date_str, sectorid)
        code_int_list = [int(code[:6]) for code in code_list]
        with open(f"{sectorid}_{date_str}.json", "w") as file:
            json.dump(code_int_list, file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="提取万得板块列表")
    parser.add_argument("-dt", type=str, required=True, help="目标日期")
    parser.add_argument("-sid", type=str, required=True, help="板块id like 1000051239000000")
    args = parser.parse_args()
    get_list(args.dt, args.sid)
