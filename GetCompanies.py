from requests.auth import HTTPBasicAuth
import requests, json
import pandas as pd


auth = HTTPBasicAuth('saccsf', '2b9c2def910f8be6c2e8b3bdd1c3d2fe')

url = "https://api.neoncrm.com/v2/accounts?userType=COMPANY&pageSize=5000"

payload = {}
headers = {
  'NEON-API-VERSION': '2.8',
  'Content-Type': 'application/json'
}

api_response = requests.request("GET", url, headers=headers, data=payload, auth=auth)

data = pd.json_normalize(api_response.json()["accounts"])

print(data)

#breakpoint()
