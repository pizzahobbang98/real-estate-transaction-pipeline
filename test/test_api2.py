import requests
import xml.etree.ElementTree as ET
import pandas as pd

SERVICE_KEY = "95ebf59ae83e2d42fb7ce7793694c25418da5c100a62261d290640999660094c"

# 1. URL을 전월세 API로 변경
BASE_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent"

url = f"{BASE_URL}?serviceKey={SERVICE_KEY}"

params = {
    "LAWD_CD":   "11680",
    "DEAL_YMD":  "202501",
    "pageNo":    1,
    "numOfRows": 10,
}

resp = requests.get(url, params=params, timeout=30)
root = ET.fromstring(resp.content)

print("resultCode:", root.findtext(".//resultCode"))
print("totalCount:", root.findtext(".//totalCount"))

records = []
for item in root.findall(".//item"):
    records.append({
        "아파트명":     item.findtext("aptNm"),
        "법정동":       item.findtext("umdNm"),
        # 2. 전월세 전용 필드로 변경
        "보증금_만원":  item.findtext("deposit", "").replace(",", "").strip(),
        "월세_만원":    item.findtext("monthlyRent", "").replace(",", "").strip(),
        "전용면적":     item.findtext("excluUseAr"),
        "층":           item.findtext("floor"),
        "건축년도":     item.findtext("buildYear"),
        "계약년월":     f"{item.findtext('dealYear')}-{item.findtext('dealMonth').zfill(2)}",
        "전월세구분":   item.findtext("rentType"),  # 전세/월세
    })

df = pd.DataFrame(records)
print(df.to_string())