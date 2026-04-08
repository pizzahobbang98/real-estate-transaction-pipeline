import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import requests
import pandas as pd
import time
from sigungu_codes import SIGUNGU_CODES
from tqdm import tqdm

SERVICE_KEY = "95ebf59ae83e2d42fb7ce7793694c25418da5c100a62261d290640999660094c"

TRADE_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
RENT_URL  = "https://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent"

import xml.etree.ElementTree as ET

def fetch_one(url, lawd_cd, deal_ymd):
    results = []
    page = 1
    while True:
        params = f"?serviceKey={SERVICE_KEY}&LAWD_CD={lawd_cd}&DEAL_YMD={deal_ymd}&numOfRows=1000&pageNo={page}"
        try:
            res = requests.get(url + params, timeout=10)
            root = ET.fromstring(res.text)
            
            items = root.findall(".//item")
            if not items:
                break
            
            for item in items:
                d = {child.tag: child.text for child in item}
                results.append(d)
            
            total_count = root.findtext(".//totalCount")
            if not total_count or len(results) >= int(total_count):
                break
            page += 1
        except Exception as e:
            tqdm.write(f"  오류: {lawd_cd} {deal_ymd} - {e}")
            break
    return results

def fetch_period(start_year, start_month, end_year, end_month):
    all_records = []

    # 연월 리스트 생성
    periods = []
    y, m = start_year, start_month
    while (y < end_year) or (y == end_year and m <= end_month):
        periods.append(f"{y}{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1

    sigungu_list = list(SIGUNGU_CODES.items())
    total_tasks = len(periods) * len(sigungu_list)

    print(f"수집 기간: {periods[0]} ~ {periods[-1]}")
    print(f"총 작업: {len(periods)}개월 × {len(sigungu_list)}개 시군구 = {total_tasks:,}건 API 호출")
    print("=" * 60)

    # 전체 진행바
    with tqdm(total=total_tasks, desc="전체 진행", unit="건",
              bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]") as pbar:

        for deal_ymd in periods:
            period_trade = 0
            period_rent = 0

            # 연월별 진행바
            for region_name, lawd_cd in sigungu_list:

                # 매매 수집
                trade_items = fetch_one(TRADE_URL, lawd_cd, deal_ymd)
                for item in trade_items:
                    cancel = item.get("cdealType", "") or item.get("cancelDealType", "")
                    if cancel and cancel.strip() not in ["", "NaN"]:
                        continue
                    all_records.append({
                        "region_name": region_name,
                        "data_type": "매매",
                        "lawd_cd": lawd_cd,
                        "deal_ymd": deal_ymd,
                        "apt_name": item.get("aptNm", ""),
                        "umd_nm": item.get("umdNm", ""),
                        "deal_amount": str(item.get("dealAmount", "")).replace(",", "").strip(),
                        "deposit": None,
                        "monthly_rent": None,
                        "area_sqm": item.get("excluUseAr", ""),
                        "floor": item.get("floor", ""),
                        "build_year": item.get("buildYear", ""),
                        "deal_year": item.get("dealYear", ""),
                        "deal_month": item.get("dealMonth", ""),
                        "deal_day": item.get("dealDay", ""),
                        "dealing_gbn": item.get("dealingGbn", ""),
                        "cancel_deal": cancel,
                    })
                period_trade += len(trade_items)

                # 전월세 수집
                rent_items = fetch_one(RENT_URL, lawd_cd, deal_ymd)
                for item in rent_items:
                    all_records.append({
                        "region_name": region_name,
                        "data_type": "전월세",
                        "lawd_cd": lawd_cd,
                        "deal_ymd": deal_ymd,
                        "apt_name": item.get("aptNm", ""),
                        "umd_nm": item.get("umdNm", ""),
                        "deal_amount": None,
                        "deposit": str(item.get("deposit", "")).replace(",", "").strip(),
                        "monthly_rent": str(item.get("monthlyRent", "")).replace(",", "").strip(),
                        "area_sqm": item.get("excluUseAr", ""),
                        "floor": item.get("floor", ""),
                        "build_year": item.get("buildYear", ""),
                        "deal_month": item.get("dealMonth", ""),
                        "deal_year": item.get("dealYear", ""),
                        "deal_day": item.get("dealDay", ""),
                        "dealing_gbn": None,
                        "cancel_deal": None,
                    })
                period_rent += len(rent_items)

                pbar.set_postfix({
                    "연월": deal_ymd,
                    "지역": region_name,
                    "누적": f"{len(all_records):,}건"
                })
                pbar.update(1)
                time.sleep(0.05)

            # 연월 완료 시 중간 저장
            df = pd.DataFrame(all_records)
            df.to_csv(f"output_history_{deal_ymd}.csv", index=False, encoding="utf-8-sig")
            tqdm.write(f"✅ [{deal_ymd}] 완료 - 매매:{period_trade:,} 전월세:{period_rent:,} / 누적:{len(all_records):,}건")

    print(f"\n🎉 전체 수집 완료: {len(all_records):,}건")
    return all_records

if __name__ == "__main__":
    fetch_period(2023, 1, 2026, 3)