import logging.config
from datetime import date
from datetime import datetime

import logging
import os
import pandas as pd
import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

# loading logger
logging.config.fileConfig("NeonCRMAnalytics.log")

# loading variables from .env file
load_dotenv()
auth = HTTPBasicAuth(os.getenv("org_id"), os.getenv("api_key"))

API_BASE_URL = "https://api.neoncrm.com/v2"
API_LIMIT = 5000
API_VERSION = "2.8"

def get_accounts_companies():
    url = API_BASE_URL + "/accounts?userType=COMPANY&pageSize=" + str(API_LIMIT)

    payload = {}
    headers = {
        'NEON-API-VERSION': str(API_VERSION),
        'Content-Type': 'application/json'
    }

    api_response = requests.request("GET", url, headers=headers, data=payload, auth=auth)
    return pd.json_normalize(api_response.json()["accounts"])

def get_accounts_individuals():
    url = API_BASE_URL + "/accounts?userType=INDIVIDUAL&pageSize=" + str(API_LIMIT)

    payload = {}
    headers = {
        'NEON-API-VERSION': str(API_VERSION),
        'Content-Type': 'application/json'
    }

    api_response = requests.request("GET", url, headers=headers, data=payload, auth=auth)
    return pd.json_normalize(api_response.json()["accounts"])

def get_accounts_all():
    individuals = get_accounts_individuals()
    companies = get_accounts_companies()
    return pd.merge(companies, individuals,
                    on=["accountId", "firstName", "lastName", "email", "userType", "companyName"], how='outer',
                    validate='one_to_one')

def get_accounts_creation_date(accountId, accountType):
    logging.debug("Getting accounts creation date for " + str(accountId))
    url = API_BASE_URL + "/accounts/" + str(accountId)

    payload = {}
    headers = {
        'NEON-API-VERSION': str(API_VERSION),
        'Content-Type': 'application/json'
    }

    api_response = requests.request("GET", url, headers=headers, data=payload, auth=auth)
    creation_date = pd.json_normalize(api_response.json())
    if (accountType == "INDIVIDUAL"):
        date = creation_date.get('individualAccount.timestamps.createdDateTime')
    else:
        logging.debug("No individual account")
    if (accountType == "COMPANY"):
        date = creation_date.get('companyAccount.timestamps.createdDateTime')
    else:
        logging.debug("No company account")
    return date[0].split('T')

def get_accounts_type(accountId):
    logging.debug("Getting account type for " + accountId)
    url = API_BASE_URL + "/accounts/" + str(accountId) + "/memberships"

    payload = {}
    headers = {
        'NEON-API-VERSION': str(API_VERSION),
        'Content-Type': 'application/json'
    }

    api_response = requests.request("GET", url, headers=headers, data=payload, auth=auth)
    today = date.today()
    memberships = pd.json_normalize(api_response.json()["memberships"])

    for i, membership in memberships.iterrows():
        date_object = datetime.strptime(membership.termEndDate, '%Y-%m-%d').date()
        if (today < date_object):
            # alternative membershipTerm.name
            return membership["membershipLevel.name"]
    return "No Membership active"

def get_all_membership_types(accounts):
    logging.info("Get all membership types")
    membership_types = []
    for i, account in accounts.iterrows():
        logging.debug("Get Membership Type for " + str(account.accountId))
        membership_types.append(get_accounts_type(account.accountId))
    return membership_types

def print_all_accounts_to_csv():
    logging.info("Getting all accounts to csv")
    accounts = get_accounts_all()
    membership_types = get_all_membership_types(accounts)
    accounts["Membership Type"] = membership_types
    creation_date = []
    for x, account in accounts.iterrows():
        creation_date.append(get_accounts_creation_date(account['accountId'], account['userType'])[0])
    accounts['accountCreationDate'] = creation_date
    accounts.to_csv("out.csv", index=False, header=True)

def main():
    logging.basicConfig(filename='NeonCRMAnalytics.log', level=logging.INFO)
    logging.info('Main program started')
    print_all_accounts_to_csv()
    logging.info('Main Program finished')

if __name__ == '__main__':
    main()