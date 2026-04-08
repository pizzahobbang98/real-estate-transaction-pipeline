import requests
import xml.etree.ElementTree as ET
import pandas as pd
import time
import logging
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
from sigungu_codes import SIGUNGU_CODES
# 로그 설정 (Airflow에서도 같은 방식으로 출력됨)
logger = logging.getLogger(__name__)

# ────────────────────────────────────────
# 수집할 시군구 코드 목록
# 필요한 지역 추가/제거 가능
# ────────────────────────────────────────

# 매매 API
TRADE_URL = (
    "https://apis.data.go.kr/1613000"
    "/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
)

# 전/월세 API
RENT_URL = (
    "https://apis.data.go.kr/1613000"
    "/RTMSDataSvcAptRent/getRTMSDataSvcAptRent"
)


def fetch_single(
    service_key: str,
    api_url: str,
    lawd_cd: str,
    deal_ymd: str,
    num_of_rows: int = 1000,
) -> list[dict]:

    url = f"{api_url}?serviceKey={service_key}"
    all_records = []
    page_no = 1

    while True:
        params = {
            "LAWD_CD":   lawd_cd,
            "DEAL_YMD":  deal_ymd,
            "pageNo":    page_no,
            "numOfRows": num_of_rows,
        }

        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"요청 실패: {lawd_cd} {deal_ymd} p{page_no} → {e}")
            raise

        root = ET.fromstring(resp.content)

        result_code = root.findtext(".//resultCode", "")
        if result_code != "000":
            result_msg = root.findtext(".//resultMsg", "")
            raise ValueError(f"API 에러 [{result_code}]: {result_msg}")

        # 전체 건수 확인 (첫 페이지에서만)
        if page_no == 1:
            total_count = int(root.findtext(".//totalCount", "0"))
            logger.info(f"수집 시작: {lawd_cd} {deal_ymd} → 전체 {total_count}건")

        items = root.findall(".//item")

        # 이번 페이지에 데이터 없으면 종료
        if not items:
            break

        for item in items:
            all_records.append({
                "lawd_cd":      lawd_cd,
                "deal_ymd":     deal_ymd,
                "apt_name":     (item.findtext("aptNm") or "").strip(),
                "umd_nm":       (item.findtext("umdNm") or "").strip(),
                "deal_amount":  (item.findtext("dealAmount") or "").replace(",", "").strip(),
                "deposit":      (item.findtext("deposit") or "").replace(",", "").strip(),
                "monthly_rent": (item.findtext("monthlyRent") or "").replace(",", "").strip(),
                "area_sqm":     item.findtext("excluUseAr"),
                "floor":        item.findtext("floor"),
                "build_year":   item.findtext("buildYear"),
                "deal_year":    item.findtext("dealYear"),
                "deal_month":   item.findtext("dealMonth"),
                "deal_day":     item.findtext("dealDay"),
                "dealing_gbn":  item.findtext("dealingGbn"),
                "cancel_deal":  (item.findtext("cdealType") or "").strip(),
            })

        # 마지막 페이지 도달 시 종료
        if len(all_records) >= total_count:
            break

        page_no += 1
        time.sleep(0.2)

    logger.info(f"수집 완료: {lawd_cd} {deal_ymd} → {len(all_records)}건")
    return all_records


# 반복수집
def fetch_all(
    service_key: str,
    deal_ymd: str,
    trade: bool = True,       # 매매 수집 여부
    rent: bool = True,        # 전월세 수집 여부
    sleep_sec: float = 0.3,   # API 과호출 방지 딜레이
) -> pd.DataFrame:
    """
    전체 지역 × 단일 월 수집 → DataFrame 반환
    Airflow의 fetch_trade 태스크에서 이 함수를 호출함
    """
    all_records = []

    for region_name, lawd_cd in SIGUNGU_CODES.items():

        if trade:
            try:
                # 매매 수집
                rows = fetch_single(service_key, TRADE_URL, lawd_cd, deal_ymd)
                for r in rows:
                    r["data_type"] = "매매"
                    r["region_name"] = region_name
                all_records.extend(rows)
            except Exception as e:
                logger.warning(f"[매매 SKIP] {region_name}: {e}")

        if rent:
            try:
                # 전월세 수집
                rows = fetch_single(service_key, RENT_URL, lawd_cd, deal_ymd)
                for r in rows:
                    r["data_type"] = "전월세"
                    r["region_name"] = region_name
                all_records.extend(rows)
            except Exception as e:
                logger.warning(f"[전월세 SKIP] {region_name}: {e}")

        time.sleep(sleep_sec)

    df = pd.DataFrame(all_records)

    if df.empty:
        logger.warning("수집된 데이터 없음")
        return df

    # 취소된 거래 제거
    df = df[df["cancel_deal"] == ""]

    # 타입 변환
    # 문자열 -> 숫자 변환
    # errors="coerce" 변환 안되는 값은 NaN으로 처리하는 옵션
    df["deal_amount"]  = pd.to_numeric(df["deal_amount"],  errors="coerce")
    df["deposit"]      = pd.to_numeric(df["deposit"],      errors="coerce")
    df["monthly_rent"] = pd.to_numeric(df["monthly_rent"], errors="coerce")
    df["area_sqm"]     = pd.to_numeric(df["area_sqm"],     errors="coerce")
    df["floor"]        = pd.to_numeric(df["floor"],        errors="coerce")
    df["build_year"]   = pd.to_numeric(df["build_year"],   errors="coerce")

    logger.info(f"전체 수집 완료: {len(df)}건")
    return df


# ────────────────────────────────────────
# 로컬 테스트용 (직접 실행할 때만 동작)
# ────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    SERVICE_KEY = "95ebf59ae83e2d42fb7ce7793694c25418da5c100a62261d290640999660094c"

    df = fetch_all(SERVICE_KEY, deal_ymd="202501")

    print(f"\n총 {len(df)}건 수집")
    print(df[["region_name", "data_type", "apt_name", "deal_amount", "deposit", "area_sqm"]].head(20).to_string())

    # CSV로 저장
    df.to_csv("output_202501.csv", index=False, encoding="utf-8-sig")
    print("\noutput_202501.csv 저장 완료!")