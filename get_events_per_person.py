from requests.auth import HTTPBasicAuth
import requests, os
import pandas as pd
from dotenv import load_dotenv


# loading variables from .env file
load_dotenv()
auth = HTTPBasicAuth(os.getenv("org_id"), os.getenv("api_key"))

API_BASE_URL = "https://api.neoncrm.com/v2"
API_LIMIT = 5000
API_VERSION = "2.8"


def get_accounts_companies():
    url = API_BASE_URL + "/accounts?userType=COMPANY&pageSize=" + str(API_LIMIT)

    payload = {}
    headers = {"NEON-API-VERSION": str(API_VERSION), "Content-Type": "application/json"}

    api_response = requests.request(
        "GET", url, headers=headers, data=payload, auth=auth
    )
    return pd.json_normalize(api_response.json()["accounts"])


def get_accounts_individuals():
    url = API_BASE_URL + "/accounts?userType=INDIVIDUAL&pageSize=" + str(API_LIMIT)

    payload = {}
    headers = {"NEON-API-VERSION": str(API_VERSION), "Content-Type": "application/json"}

    api_response = requests.request(
        "GET", url, headers=headers, data=payload, auth=auth
    )
    return pd.json_normalize(api_response.json()["accounts"])


def get_accounts_all():
    individuals = get_accounts_individuals()
    companies = get_accounts_companies()
    return pd.merge(
        companies,
        individuals,
        on=["accountId", "firstName", "lastName", "email", "userType", "companyName"],
        how="outer",
        validate="one_to_one",
    )


def get_all_event_ids():
    url = API_BASE_URL + "/events?pageSize=5000"

    payload = {}
    headers = {"NEON-API-VERSION": str(API_VERSION), "Content-Type": "application/json"}

    api_response = requests.request(
        "GET", url, headers=headers, data=payload, auth=auth
    )

    event_list = api_response.json()["events"]

    return [event["id"] for event in event_list]


def get_attendees(eventId):
    url = API_BASE_URL + "/events/" + str(eventId) + "/attendees"

    payload = {}
    headers = {"NEON-API-VERSION": str(API_VERSION), "Content-Type": "application/json"}

    api_response = requests.request(
        "GET", url, headers=headers, data=payload, auth=auth
    )
    attendees = api_response.json()["attendees"]
    if attendees is None:
        return []
    # Return only unique ids
    return list(set([attendee["registrantAccountId"] for attendee in attendees]))


def add_events_to_person():
    df = get_accounts_all()
    event_ids = get_all_event_ids()

    # create empty list to store event ids for each person
    df["event_ids"] = df["accountId"].apply(lambda x: [])

    for event_id in event_ids:
        attendees = get_attendees(event_id)
        for attendee in attendees:
            df.loc[df["accountId"] == attendee, "event_ids"] = df.loc[
                df["accountId"] == attendee, "event_ids"
            ].apply(lambda x: x + [event_id])

    return df


if __name__ == "__main__":
    df = add_events_to_person()
    df.to_csv("events_per_person.csv", index=False)
