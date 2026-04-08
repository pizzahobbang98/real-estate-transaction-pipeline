import requests
import xml.etree.ElementTree as ET
import pandas as pd

SERVICE_KEY = "95ebf59ae83e2d42fb7ce7793694c25418da5c100a62261d290640999660094c"
BASE_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"

url = f"{BASE_URL}?serviceKey={SERVICE_KEY}"

params = {
    "LAWD_CD":   "11680",
    "DEAL_YMD":  "202501",
    "pageNo":    1,
    "numOfRows": 10,
}

resp = requests.get(url, params=params, timeout=30)
root = ET.fromstring(resp.content)

records = []
for item in root.findall(".//item"):
    records.append({
        "아파트명":   item.findtext("aptNm"),
        "법정동":     item.findtext("umdNm"),
        "거래금액_만원": item.findtext("dealAmount", "").replace(",", "").strip(),
        "전용면적":   item.findtext("excluUseAr"),
        "층":         item.findtext("floor"),
        "건축년도":   item.findtext("buildYear"),
        "계약년월":   f"{item.findtext('dealYear')}-{item.findtext('dealMonth').zfill(2)}",
        "거래유형":   item.findtext("dealingGbn"),
        "해제여부":   item.findtext("cdealType", "").strip(),
    })

df = pd.DataFrame(records)
print(df.to_string())