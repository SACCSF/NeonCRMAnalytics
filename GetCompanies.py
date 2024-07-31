from requests.auth import HTTPBasicAuth
import requests, json
import pandas as pd
import os
from dotenv import load_dotenv

# loading variables from .env file
load_dotenv()

auth = HTTPBasicAuth(os.getenv("org_id"), os.getenv("api_key"))

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
