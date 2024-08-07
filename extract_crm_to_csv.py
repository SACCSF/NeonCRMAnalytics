import logging.config
from datetime import date
from datetime import datetime

import logging
import os
import pandas as pd
import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
import concurrent.futures
import time

# loading logger
logging.config.fileConfig("NeonCRMAnalytics.log")

# loading variables from .env file
load_dotenv()
auth = HTTPBasicAuth(os.getenv("API_ORG_ID"), os.getenv("API_API_KEY"))

# Configurable global variables
API_BASE_URL = "https://api.neoncrm.com/v2"
API_LIMIT = 5000
API_VERSION = "2.8"
MAX_WORKERS = 2


def get_request(url: str) -> dict:
    payload = {}
    headers = {"NEON-API-VERSION": str(API_VERSION), "Content-Type": "application/json"}

    api_response = requests.request(
        "GET", url, headers=headers, data=payload, auth=auth
    )
    return api_response.json()


def get_accounts_companies() -> pd.DataFrame:
    url = API_BASE_URL + "/accounts?userType=COMPANY&pageSize=" + str(API_LIMIT)

    response = get_request(url)
    logging.debug(response)
    return (pd.json_normalize(response["accounts"]))


def get_accounts_individuals() -> pd.DataFrame:
    url = API_BASE_URL + "/accounts?userType=INDIVIDUAL&pageSize=" + str(API_LIMIT)

    response = get_request(url)
    logging.debug(response)
    return (pd.json_normalize(response["accounts"]))


def get_accounts_additional_information(
    account_id, account_type, actual_type
) -> pd.DataFrame:
    logging.debug("Getting accounts additional information for " + str(account_id))
    url = API_BASE_URL + "/accounts/" + str(account_id)

    response = get_request(url)

    if actual_type == "INDIVIDUAL":
        if account_type == "COMPANY":
            return pd.DataFrame({"accountId": [account_id]})
        additional_information = pd.json_normalize(response["individualAccount"])
    elif actual_type == "COMPANY":
        if account_type == "INDIVIDUAL":
            return pd.DataFrame({"accountId": [account_id]})
        additional_information = pd.json_normalize(response["companyAccount"])
    else:
        raise ValueError("Invalid account type")

    return additional_information


def get_accounts_type(account: pd.Series) -> tuple:
    account_id = account["accountId"]
    logging.debug("Getting account type for " + account_id)
    url = API_BASE_URL + "/accounts/" + str(account_id) + "/memberships"

    response = get_request(url)
    today = date.today()
    memberships = pd.json_normalize(response["memberships"])

    for i, membership in memberships.iterrows():
        date_object = datetime.strptime(membership.termEndDate, "%Y-%m-%d").date()
        if today < date_object:
            return (account_id, membership["membershipLevel.name"], membership["fee"])
    return (account_id, "No Membership active", "0.0")


def get_all_membership_types(accounts: pd.DataFrame) -> dict:
    logging.info("Get all membership types")
    membership_types = {}
    fees = {}
    # Get membership types concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(get_accounts_type, account): account
            for _, account in accounts.iterrows()
        }
        for future in concurrent.futures.as_completed(futures):
            id, account_type, fee = future.result()
            membership_types[id] = account_type
            fees[id] = fee

    return (membership_types, fees)


def get_all_event_ids() -> list:
    url = API_BASE_URL + "/events?pageSize=5000"

    response = get_request(url)
    event_list = response["events"]

    return [event["id"] for event in event_list]


def get_attendees(eventId: int) -> list:
    url = API_BASE_URL + "/events/" + str(eventId) + "/attendees"

    response = get_request(url)
    attendees = response["attendees"]
    if attendees is None:
        return []
    # Return only unique ids
    return list(set([attendee["registrantAccountId"] for attendee in attendees]))


def add_events_to_account(df) -> pd.DataFrame:
    event_ids = get_all_event_ids()
    df.loc[:, "event_ids"] = [[] for _ in range(len(df))]

    for event_id in event_ids:
        attendees = get_attendees(event_id)
        for attendee in attendees:
            df.loc[df["accountId"] == attendee, "event_ids"] = df.loc[
                df["accountId"] == attendee, "event_ids"
            ].apply(lambda x: x + [event_id])

    return df


def add_creation_date_to_account(df, actual_type):
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                get_accounts_additional_information,
                account["accountId"],
                account["userType"],
                actual_type,
            ): account
            for _, account in df.iterrows()
        }
        results = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]

    all_information = pd.concat(results, ignore_index=True)

    # Discard all columns that only have NaN or None values
    all_information = all_information.dropna(axis=1, how="all")

    # Merge all information with the original dataframe by accountId
    df = pd.merge(
        df,
        all_information,
        on=["accountId"],
        how="outer",
        validate="one_to_one",
    )

    return df


def add_membership_type_to_account(df) -> pd.DataFrame:
    membership_types, fees = get_all_membership_types(df)
    df["Membership Type"] = df["accountId"].map(membership_types)
    df["Fee"] = df["accountId"].map(fees)
    return df


def filter_non_active_accounts(df) -> pd.DataFrame:
    return df[df["Membership Type"] != "No Membership active"]


def filter_individuals(individuals: pd.DataFrame) -> pd.DataFrame:

    to_drop = [
        "noSolicitation",
        "accountCustomFields",
        "sendSystemEmail",
        "accountCurrentMembershipStatus",
        "primaryContact.contactId",
        "primaryContact.firstName",
        "primaryContact.middleName",
        "primaryContact.lastName",
        "primaryContact.salutation",
        "primaryContact.preferredName",
        "primaryContact.deceased",
        "primaryContact.department",
        "primaryContact.title",
        "generosityIndicator.indicator",
        "generosityIndicator.affinity",
        "generosityIndicator.recency",
        "generosityIndicator.frequency",
        "generosityIndicator.monetaryValue",
        "company.name",
        "login.username",
        "primaryContact.gender.code",
        "primaryContact.gender.name",
        "individualTypes",
    ]
    return (individuals.drop(columns=to_drop))


def filter_companies(companies: pd.DataFrame) -> pd.DataFrame:
    to_drop = [
        "firstName",
        "lastName",
        "noSolicitation",
        "accountCustomFields",
        "sendSystemEmail",
        "accountCurrentMembershipStatus",
        "name",
        "primaryContact.contactId",
        "primaryContact.firstName",
        "primaryContact.middleName",
        "primaryContact.lastName",
        "primaryContact.prefix",
        "primaryContact.suffix",
        "primaryContact.salutation",
        "primaryContact.preferredName",
        "primaryContact.email1",
        "primaryContact.deceased",
        "primaryContact.department",
        "primaryContact.title",
        "primaryContact.primaryContact",
        "primaryContact.currentEmployer",
        "primaryContact.startDate",
        "primaryContact.addresses",
        "generosityIndicator.indicator",
        "generosityIndicator.affinity",
        "generosityIndicator.recency",
        "generosityIndicator.frequency",
        "generosityIndicator.monetaryValue",
        "login.username",
        "primaryContact.gender.code",
        "primaryContact.gender.name",
        "companyTypes",
    ]

    return (companies.drop(columns=to_drop))


def add_fields_to_account(account: pd.DataFrame, actual) -> pd.DataFrame:
    account = add_membership_type_to_account(account)
    account = filter_non_active_accounts(account)
    account = add_events_to_account(account)
    account = add_creation_date_to_account(account, actual)

    if actual == "INDIVIDUAL":
        account = filter_individuals(account)
    elif actual == "COMPANY":
        account = filter_companies(account)
    else:
        raise ValueError("Invalid account type")

    return account


def print_all_accounts_to_csv() -> None:
    logging.info("Getting all accounts to csv")

    individuals = get_accounts_individuals()
    companies = get_accounts_companies()

    individuals = add_fields_to_account(individuals, "INDIVIDUAL")
    companies = add_fields_to_account(companies, "COMPANY")

    individuals.to_csv("individuals.csv", index=False, header=True)
    companies.to_csv("companies.csv", index=False, header=True)


def main():
    logging.basicConfig(filename="NeonCRMAnalytics.log", level=logging.INFO)
    t1 = time.time()
    logging.info(f"Main program started")
    print_all_accounts_to_csv()
    logging.info(f"Main Program finished in {time.time() - t1} seconds")


if __name__ == "__main__":
    main()
