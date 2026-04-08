import requests
import xml.etree.ElementTree as ET

r = requests.get('https://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent?serviceKey=95ebf59ae83e2d42fb7ce7793694c25418da5c100a62261d290640999660094c&LAWD_CD=11110&DEAL_YMD=202301&numOfRows=1&pageNo=1')
print("상태코드:", r.status_code)
root = ET.fromstring(r.text)
item = root.find('.//item')
if item is not None:
    for child in item:
        print(child.tag, ':', child.text)
else:
    print("item 없음")
    print(r.text[:500])