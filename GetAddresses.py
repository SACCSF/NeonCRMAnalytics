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

def get_accounts_additional_information(accountId, accountType):
    logging.debug("Getting accounts additional information for " + str(accountId))
    url = API_BASE_URL + "/accounts/" + str(accountId)

    payload = {}
    headers = {
        'NEON-API-VERSION': str(API_VERSION),
        'Content-Type': 'application/json'
    }

    api_response = requests.request("GET", url, headers=headers, data=payload, auth=auth)
    additional_information = pd.json_normalize(api_response.json())

    if (accountType == "INDIVIDUAL"):
        date = additional_information.get('individualAccount.timestamps.createdDateTime')
        if(pd.isna(additional_information.get('individualAccount.primaryContact.gender')[0])):
            gender = ''
        else:
            gender = additional_information.get('individualAccount.primaryContact.gender')[0]
        if(pd.isna(additional_information.get('individualAccount.primaryContact.title')[0])):
            title = ''
        else:
            title = additional_information.get('individualAccount.primaryContact.title')[0]
        if(pd.isna(additional_information.get('individualAccount.primaryContact.employer'))):
            employer = ''
        else:
            employer = additional_information.get('individualAccount.primaryContact.employer')
        if(pd.isna(additional_information.get('individualAccount.primaryContact.origin'))):
            origin = ''
        else:
            origin = additional_information.get('individualAccount.primaryContact.origin')
        if(pd.isna(additional_information.get('individualAccount.primaryContact.originCategory'))):
            originCategory = ''
        else:
            originCategory = additional_information.get('individualAccount.primaryContact.originCategory')
        companyName = ''
        '''
        if(pd.isna(additional_information.get('individualAccount.company.name')[0])):
            companyName = ''
        else:
            companyName = additional_information.get('individualAccount.company.name')[0]
        '''
    else:
        logging.debug("No individual account")
    if (accountType == "COMPANY"):
        date = additional_information.get('companyAccount.timestamps.createdDateTime')
        gender = ''
        title = ''
        employer = ''
        origin = ''
        originCategory = ''
        companyName = ''
    else:
        logging.debug("No company account")
    creationDate = (date[0].split('T'))
    # array: accoutntId, creationDate, gender, title, employer, origin, originCategory, companyName
    logging.debug([accountId, creationDate[0], gender, title, employer, origin, originCategory, companyName])
    return [accountId, creationDate[0], gender, title, employer, origin, originCategory, companyName]


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
    df = pd.DataFrame(columns=['accountId', 'creationDate', 'gender', 'title', 'employer',
                               'origin', 'originCategory', 'companyName' ])

    for x, account in accounts.iterrows():
        additional_information = get_accounts_additional_information(account['accountId'], account['userType'])

        df.loc[-1] = [additional_information[0], additional_information[1], additional_information[2],
                      additional_information[3],additional_information[4], additional_information[5],
                      additional_information[6], additional_information[7]]
    # array: accoutntId, creationDate, gender, title, employer, origin, originCategory, companyName

    dataexport = pd.merge(accounts, df, on=["accountId"], how='outer', validate='one_to_one')
    dataexport.to_csv("out.csv", index=False, header=True)


def main():
    logging.basicConfig(filename='NeonCRMAnalytics.log', level=logging.INFO)
    logging.info('Main program started')
    print_all_accounts_to_csv()
    logging.info('Main Program finished')

if __name__ == '__main__':
    main()

