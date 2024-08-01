from requests.auth import HTTPBasicAuth
import requests, json, os
import pandas as pd
from dotenv import load_dotenv
from datetime import date
from datetime import datetime


# loading variables from .env file
load_dotenv()
auth = HTTPBasicAuth(os.getenv("org_id"), os.getenv("api_key"))

API_BASE_URL = "https://api.neoncrm.com/v2"

def get_accounts_companies():
  url = API_BASE_URL + "/accounts?userType=COMPANY&pageSize=5000"

  payload = {}
  headers = {
    'NEON-API-VERSION': '2.8',
    'Content-Type': 'application/json'
  }

  api_response = requests.request("GET", url, headers=headers, data=payload, auth=auth)
  return pd.json_normalize(api_response.json()["accounts"])


def get_accounts_individuals():
  url = API_BASE_URL + "/accounts?userType=INDIVIDUAL&pageSize=5000"

  payload = {}
  headers = {
    'NEON-API-VERSION': '2.8',
    'Content-Type': 'application/json'
  }

  api_response = requests.request("GET", url, headers=headers, data=payload, auth=auth)
  return pd.json_normalize(api_response.json()["accounts"])

def get_accounts_all():
  individuals = get_accounts_individuals()
  companies = get_accounts_companies()
  return pd.merge(companies, individuals, on=["accountId", "firstName", "lastName", "email", "userType", "companyName"], how='outer', validate='one_to_one')

def get_accounts_type(accountId):
  url = API_BASE_URL + "/accounts/" + str(accountId) + "/memberships"

  payload = {}
  headers = {
    'NEON-API-VERSION': '2.8',
    'Content-Type': 'application/json'
  }

  api_response = requests.request("GET", url, headers=headers, data=payload, auth=auth)
  today = date.today()
  memberships = pd.json_normalize(api_response.json()["memberships"])

  for i, membership in memberships.iterrows():
    date_object = datetime.strptime(membership.termEndDate, '%Y-%m-%d').date()
    if(today < date_object):
      # alternative membershipTerm.name
      return membership["membershipLevel.name"]
  return "No Membership active"

def get_all_membership_types():
  accounts = get_accounts_all()

  for i, account in accounts.iterrows():
    print(get_accounts_type(account.accountId))

def print_all_accounts_to_csv():
  accounts = get_accounts_all()
  accounts.to_csv("out.csv", index=False, header=True)

#print(get_accounts_type(7376))
get_all_membership_types()