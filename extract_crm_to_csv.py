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

# loading logger
logging.config.fileConfig("NeonCRMAnalytics.log")

# loading variables from .env file
load_dotenv()
auth = HTTPBasicAuth(os.getenv("org_id"), os.getenv("api_key"))

# Configurable global variables
API_BASE_URL = "https://api.neoncrm.com/v2"
API_LIMIT = 5000
API_VERSION = "2.8"
MAX_WORKERS = 4


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
    return pd.json_normalize(response["accounts"])


def get_accounts_individuals() -> pd.DataFrame:
    url = API_BASE_URL + "/accounts?userType=INDIVIDUAL&pageSize=" + str(API_LIMIT)

    response = get_request(url)
    return pd.json_normalize(response["accounts"])


def get_accounts_all() -> pd.DataFrame:
    individuals = get_accounts_individuals()
    companies = get_accounts_companies()
    return pd.merge(
        companies,
        individuals,
        on=["accountId", "firstName", "lastName", "email", "userType", "companyName"],
        how="outer",
        validate="one_to_one",
    )


def get_accounts_creation_date(accountId: int, accountType: str) -> list:
    logging.debug("Getting accounts creation date for " + str(accountId))
    url = API_BASE_URL + "/accounts/" + str(accountId)

    response = get_request(url)
    creation_date = pd.json_normalize(response)
    if accountType == "INDIVIDUAL":
        date = creation_date.get("individualAccount.timestamps.createdDateTime")
    else:
        logging.debug("No individual account")
    if accountType == "COMPANY":
        date = creation_date.get("companyAccount.timestamps.createdDateTime")
    else:
        logging.debug("No company account")
    return date[0].split("T")


def get_accounts_type(account: pd.Series) -> tuple:
    accountId = account.accountId
    logging.debug("Getting account type for " + accountId)
    url = API_BASE_URL + "/accounts/" + str(accountId) + "/memberships"

    response = get_request(url)
    today = date.today()
    memberships = pd.json_normalize(response["memberships"])

    for i, membership in memberships.iterrows():
        date_object = datetime.strptime(membership.termEndDate, "%Y-%m-%d").date()
        if today < date_object:
            # alternative membershipTerm.name
            return (accountId, membership["membershipLevel.name"])
    return (accountId, "No Membership active")


def get_all_membership_types(accounts: pd.DataFrame) -> dict:
    logging.info("Get all membership types")
    membership_types = {}
    # Get membership types concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(get_accounts_type, account): account
            for _, account in accounts.iterrows()
        }
        for future in concurrent.futures.as_completed(futures):
            id, account_type = future.result()
            membership_types[id] = account_type

    return membership_types


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


def add_events_to_person(df) -> pd.DataFrame:
    event_ids = get_all_event_ids()
    df["event_ids"] = df["accountId"].apply(lambda x: [])

    for event_id in event_ids:
        attendees = get_attendees(event_id)
        for attendee in attendees:
            df.loc[df["accountId"] == attendee, "event_ids"] = df.loc[
                df["accountId"] == attendee, "event_ids"
            ].apply(lambda x: x + [event_id])

    return df


def add_creation_date_to_person(df) -> pd.DataFrame:
    creation_date = []
    for x, account in df.iterrows():
        creation_date.append(
            get_accounts_creation_date(account["accountId"], account["userType"])[0]
        )
    df["accountCreationDate"] = creation_date
    return df


def add_membership_type_to_person(df) -> pd.DataFrame:
    membership_types = get_all_membership_types(df)
    df["Membership Type"] = df["accountId"].map(membership_types)
    return df


def print_all_accounts_to_csv() -> None:
    logging.info("Getting all accounts to csv")
    accounts = get_accounts_all()

    accounts = add_membership_type_to_person(accounts)
    accounts = add_creation_date_to_person(accounts)
    accounts = add_events_to_person(accounts)

    path_to_csv = "out.csv"
    counter = 0
    while os.path.exists(path_to_csv):
        path_to_csv = f"out{counter}.csv"
        counter += 1

    accounts.to_csv("out.csv", index=False, header=True)


def main():
    logging.basicConfig(filename="NeonCRMAnalytics.log", level=logging.INFO)
    logging.info("Main program started")
    print_all_accounts_to_csv()
    logging.info("Main Program finished")


if __name__ == "__main__":
    main()
