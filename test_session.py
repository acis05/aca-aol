import requests

ACCURATE_BASE_URL = "https://zeus.accurate.id"

resp = requests.post(ACCURATE_BASE_URL + "/api/open-db.do")
print(resp.status_code)
print(resp.text)
