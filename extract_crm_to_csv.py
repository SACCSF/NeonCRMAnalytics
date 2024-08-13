import logging.config
from datetime import date
from datetime import datetime

import logging
import os
import pandas as pd
import numpy as np
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

logging.debug(os.getenv("API_ORG_ID"))

# Configurable global variables
API_BASE_URL = "https://api.neoncrm.com/v2"
API_LIMIT = 5000
API_VERSION = "2.8"
MAX_WORKERS = 4
API_TIMEOUT = 0.5


def get_request(url: str, return_key: str) -> dict:
    """
    Sends a GET request to the specified URL with the required headers and authentication,
    and returns the JSON response.

    Parameters:
        url (str): The URL to which the GET request will be sent.

    Returns:
        dict: The JSON response from the server.

    Behavior:
        - Sends a GET request to the given URL with the following:
            - Headers:
                - "NEON-API-VERSION": Version of the API, set by the global variable `API_VERSION`.
                - "Content-Type": "application/json" to specify that the content is in JSON format.
            - Payload: An empty dictionary, as no payload is needed for a GET request.
            - Authentication: Set by the global variable `auth`.
        - Measures the time taken for the API request and compares it with a predefined timeout (`API_TIMEOUT`).
        - If the request completes faster than `API_TIMEOUT`, the function waits for the remaining duration to ensure a consistent pacing of API requests.
        - The function returns the JSON response received from the server.
    """
    while True:
        headers = {
            "NEON-API-VERSION": str(API_VERSION),
            "Content-Type": "application/json",
        }
        t1 = time.time()
        try:
            api_response = requests.request("GET", url, headers=headers, auth=auth)
            api_response.raise_for_status()

            if not api_response.content:
                logging.error(f"Empty API response, retrying...")
                time.sleep(1)
                continue

            t2 = time.time()
            duration = t2 - t1
            if duration < API_TIMEOUT:
                time.sleep(API_TIMEOUT - duration)

            try:
                res = api_response.json()

            except ValueError:
                logging.error(f"Response is not in JSON format, retrying...")
                time.sleep(1)
                continue
            if type(res) == dict:
                return res[return_key]
            else:
                logging.error(f"Error in API request: {res}, retrying...")
                time.sleep(1)
                continue

        except requests.exceptions.HTTPError as err:
            logging.error(
                f"HTTP error occurred: {err} retrying...",
            )
            time.sleep(1)
            continue


def get_accounts_companies() -> pd.DataFrame:
    """
    Fetches a list of company accounts from the API and returns it as a normalized pandas DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing the normalized data of company accounts.

    Behavior:
        - Constructs the URL to fetch company accounts from the API using the base URL (`API_BASE_URL`) and the `API_LIMIT` to specify the maximum number of records to retrieve.
        - Calls the `get_request` function to send a GET request to the constructed URL.
        - Logs the API response at the debug level for troubleshooting purposes.
        - Normalizes the JSON response's "accounts" data and converts it into a pandas DataFrame for easier data manipulation and analysis.
    """
    url = API_BASE_URL + "/accounts?userType=COMPANY&pageSize=" + str(API_LIMIT)

    response = get_request(url, "accounts")
    logging.debug("All companies received!")
    return pd.json_normalize(response)


def get_accounts_individuals() -> pd.DataFrame:
    """
    Fetches a list of individual user accounts from the API and returns it as a normalized pandas DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing the normalized data of individual user accounts.

    Behavior:
        - Constructs the URL to fetch individual user accounts from the API using the base URL (`API_BASE_URL`) and the `API_LIMIT` to specify the maximum number of records to retrieve.
        - Calls the `get_request` function to send a GET request to the constructed URL.
        - Logs the API response at the debug level for troubleshooting purposes.
        - Normalizes the JSON response's "accounts" data and converts it into a pandas DataFrame for easier data manipulation and analysis.
    """
    url = API_BASE_URL + "/accounts?userType=INDIVIDUAL&pageSize=" + str(API_LIMIT)

    response = get_request(url, "accounts")
    logging.debug("All individuals received!")
    return pd.json_normalize(response)


def get_accounts_additional_information(
    account_id, account_type, actual_type
) -> pd.DataFrame:
    """
    Fetches additional information for a specific account based on its ID and type,
    and returns it as a normalized pandas DataFrame.

    Parameters:
        account_id (str or int): The unique identifier for the account.
        account_type (str): The expected type of the account ("COMPANY" or "INDIVIDUAL").
        actual_type (str): The actual type of the account as returned by the API ("COMPANY" or "INDIVIDUAL").

    Returns:
        pd.DataFrame: A DataFrame containing the additional account information.
                     If the `account_type` does not match the `actual_type`,
                     a DataFrame with only the `accountId` is returned.
                     If the `actual_type` is neither "COMPANY" nor "INDIVIDUAL", a ValueError is raised.

    Behavior:
        - Logs a debug message indicating the retrieval of additional information for the specified account ID.
        - Constructs the URL to fetch account information based on the given `account_id`.
        - Calls the `get_request` function to send a GET request to the constructed URL and retrieves the response.
        - Based on the `actual_type`:
            - If `actual_type` is "INDIVIDUAL" and `account_type` matches, it normalizes and returns the "individualAccount" data.
            - If `actual_type` is "COMPANY" and `account_type` matches, it normalizes and returns the "companyAccount" data.
            - If there is a mismatch between `account_type` and `actual_type`, it returns a DataFrame with only the `accountId`.
        - Raises a `ValueError` if `actual_type` is not "COMPANY" or "INDIVIDUAL".
    """
    logging.debug("Getting accounts additional information for " + str(account_id))
    url = API_BASE_URL + "/accounts/" + str(account_id)

    if actual_type == "INDIVIDUAL":
        if account_type == "COMPANY":
            return pd.DataFrame({"accountId": [account_id]})
        response = get_request(url, "individualAccount")
        additional_information = pd.json_normalize(response)
    elif actual_type == "COMPANY":
        if account_type == "INDIVIDUAL":
            return pd.DataFrame({"accountId": [account_id]})
        response = get_request(url, "companyAccount")
        additional_information = pd.json_normalize(response)
    else:
        raise ValueError("Invalid account type")

    return additional_information


def get_accounts_type(account: pd.Series) -> tuple:
    """
    Determines the membership type and associated fee for a given account, based on its active memberships.

    Parameters:
        account (pd.Series): A pandas Series containing account information,
                             including at least the "accountId" field.

    Returns:
        tuple: A tuple containing:
               - account_id (str): The ID of the account.
               - membership_level (str): The name of the active membership level.
                                        If no active membership is found, returns "No Membership active".
               - fee (str): The fee associated with the active membership.
                            If no active membership is found, returns "0.0".
               - termEndDate (str): The end date of the active membership.
                                      If no active membership is found, returns np.nan.
               - transactionDate (str): The transaction date of the active membership.
                                         If no active membership is found, returns np.nan.
               - totalMemberships (int): The total number of memberships.
                                            If no active membership is found, returns 0.

    Behavior:
        - Extracts the `account_id` from the given pandas Series.
        - Logs a debug message indicating that it is retrieving the account type for the specified `account_id`.
        - Constructs the API URL to fetch the membership information for the account.
        - Calls the `get_request` function to send a GET request to the constructed URL and retrieves the response.
        - Normalizes the JSON response's "memberships" data into a pandas DataFrame.
        - Iterates through the memberships:
            - Converts the membership's `termEndDate` into a date object and checks if it is in the future.
            - If an active membership is found (i.e., the current date is before `termEndDate`), returns the `account_id`, membership level name, and associated fee.
        - If no active membership is found, returns the `account_id`, "No Membership active", and a fee of "0.0" along with np.nan values for `termEndDate` and `transactionDate`.
    """
    account_id = account["accountId"]
    logging.debug("Getting account type for " + account_id)
    url = API_BASE_URL + "/accounts/" + str(account_id) + "/memberships"

    response = get_request(url, "memberships")
    today = date.today()
    memberships = pd.json_normalize(response)

    if len(memberships) == 0:
        return (account_id, "No Membership active", "0.0", np.nan, np.nan, 0)

    for i, membership in memberships.iterrows():
        date_object = datetime.strptime(membership.termEndDate, "%Y-%m-%d").date()
        if today < date_object:
            return (
                account_id,
                membership["membershipLevel.name"],
                membership["fee"],
                membership["termEndDate"],
                membership["transactionDate"],
                len(memberships),
            )
    return (account_id, "No Membership active", "0.0", np.nan, np.nan, len(memberships))


def get_all_membership_types(accounts: pd.DataFrame) -> dict:
    """
    Retrieves the membership types and associated fees for all accounts concurrently and returns them in two dictionaries.

    Parameters:
        accounts (pd.DataFrame): A pandas DataFrame containing account information.
                                 Each row should represent an account with at least an "accountId" field.

    Returns:
        dict: A tuple containing two dictionaries:
              - membership_types (dict): A dictionary where keys are account IDs and values are the corresponding membership levels.
              - fees (dict): A dictionary where keys are account IDs and values are the associated fees for the memberships.

    Behavior:
        - Logs an informational message indicating the start of the process to get all membership types.
        - Initializes two dictionaries to store membership types and fees.
        - Uses a `ThreadPoolExecutor` to fetch membership information concurrently for all accounts.
        - Submits tasks to the executor to call `get_accounts_type` for each account in the DataFrame.
        - Collects results as they are completed:
            - Updates the `membership_types` dictionary with the account ID and its membership level.
            - Updates the `fees` dictionary with the account ID and its membership fee.
        - Returns a tuple containing the `membership_types` and `fees` dictionaries.

    Notes:
        - The `get_accounts_type` function is used to fetch the membership type and fee for each account.
        - The number of concurrent threads is controlled by the `MAX_WORKERS` variable.
    """
    logging.info("Get all membership types")
    membership_types, fees, term_end_dates, transaction_dates, number_of_memberships = (
        {},
        {},
        {},
        {},
        {},
    )
    # Get membership types concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(get_accounts_type, account): account
            for _, account in accounts.iterrows()
        }
        for future in concurrent.futures.as_completed(futures):
            (
                id,
                account_type,
                fee,
                term_end_date,
                transaction_date,
                number_of_membership,
            ) = future.result()
            membership_types[id] = account_type
            fees[id] = fee
            term_end_dates[id] = term_end_date
            transaction_dates[id] = transaction_date
            number_of_memberships[id] = number_of_membership

    return (
        membership_types,
        fees,
        term_end_dates,
        transaction_dates,
        number_of_memberships,
    )


def get_all_event_ids() -> list:
    """
    Retrieves all event IDs from the API and returns them as a list.

    Returns:
        list: A list of event IDs retrieved from the API.

    Behavior:
        - Constructs the API URL to fetch event data with a maximum page size of 5000.
        - Calls the `get_request` function to send a GET request to the constructed URL and retrieves the response.
        - Extracts the list of events from the response.
        - Iterates over the list of events and collects the IDs of all events into a list.
        - Returns the list of event IDs.

    Notes:
        - Assumes that the API response contains an "events" field, which is a list of event objects.
        - Each event object in the list is expected to have an "id" field.
    """
    url = API_BASE_URL + "/events?pageSize=5000"

    response = get_request(url, "events")
    return [event["id"] for event in response]


def get_attendees(eventId: int) -> list:
    """
    Retrieves the list of unique attendee IDs for a given event from the API.

    Parameters:
        eventId (int): The unique identifier of the event for which attendees are being fetched.

    Returns:
        list: A list of unique attendee IDs associated with the specified event.
              Returns an empty list if no attendees are found.

    Behavior:
        - Constructs the API URL to fetch attendee data for the specified event.
        - Calls the `get_request` function to send a GET request to the constructed URL and retrieves the response.
        - Extracts the list of attendees from the response.
        - If no attendees are found (i.e., the "attendees" field is `None`), returns an empty list.
        - Otherwise, iterates over the list of attendees, collects the `registrantAccountId` for each attendee, and ensures that only unique IDs are included in the final list.
        - Returns the list of unique attendee IDs.

    Notes:
        - Assumes that the API response contains an "attendees" field, which is a list of attendee objects.
        - Each attendee object in the list is expected to have a "registrantAccountId" field.
    """
    url = API_BASE_URL + "/events/" + str(eventId) + "/attendees"

    response = get_request(url, "attendees")
    if response is None:
        return []
    # Return only unique ids
    return list(set([attendee["registrantAccountId"] for attendee in response]))


def add_events_to_account(df) -> pd.DataFrame:
    """
    Adds a list of event IDs to each account in a DataFrame, representing the events each account has attended.

    Parameters:
        df (pd.DataFrame): A pandas DataFrame containing account information.
                           Each row should represent an account with at least an "accountId" field.

    Returns:
        pd.DataFrame: The input DataFrame with an additional column "event_ids" added.
                      This column contains lists of event IDs that each account has attended.

    Behavior:
        - Retrieves all event IDs using the `get_all_event_ids` function.
        - Initializes an empty list in a new "event_ids" column for each account in the DataFrame.
        - Iterates over each event ID:
            - Retrieves the list of attendee IDs for the current event using the `get_attendees` function.
            - For each attendee, finds the corresponding account in the DataFrame and appends the current event ID to its "event_ids" list.
        - Returns the updated DataFrame with the "event_ids" column populated with lists of event IDs.

    Notes:
        - Assumes that the DataFrame `df` contains a column named "accountId" which uniquely identifies each account.
        - The "event_ids" column is added to the DataFrame, where each cell contains a list of event IDs representing the events attended by the account.
        - The `get_all_event_ids` and `get_attendees` functions are used to fetch event and attendee data from the API.
    """
    event_ids = get_all_event_ids()
    df.loc[:, "event_ids"] = [[] for _ in range(len(df))]

    for event_id in event_ids:
        logging.debug("Getting attendees for event " + str(event_id))
        attendees = get_attendees(event_id)
        for attendee in attendees:
            df.loc[df["accountId"] == attendee, "event_ids"] = df.loc[
                df["accountId"] == attendee, "event_ids"
            ].apply(lambda x: x + [event_id])

    return df


def add_creation_date_to_account(df, actual_type):
    """
    Adds creation date and other additional information to accounts in a DataFrame by retrieving details from an API.

    Parameters:
       df (pd.DataFrame): A pandas DataFrame containing account information.
                          Each row should represent an account with at least an "accountId" and "userType" field.
       actual_type (str): The actual type of the accounts to be fetched (e.g., "INDIVIDUAL" or "COMPANY").

    Returns:
       pd.DataFrame: The input DataFrame merged with additional account information, including creation dates.
                     Columns that only contain NaN or None values are discarded.

    Behavior:
       - Uses a `ThreadPoolExecutor` to fetch additional account information concurrently for each account in the DataFrame.
       - For each account, submits a task to the executor to call the `get_accounts_additional_information` function, passing in the account's ID, user type, and the specified actual type.
       - Collects the results as they are completed and concatenates them into a single DataFrame.
       - Drops any columns in the concatenated DataFrame that contain only NaN or None values.
       - Merges the original DataFrame with the concatenated DataFrame on the "accountId" column, using an outer join to include all accounts and their additional information.
       - Returns the merged DataFrame.

    Notes:
       - The `get_accounts_additional_information` function is used to fetch the additional details for each account.
       - The number of concurrent threads is controlled by the `MAX_WORKERS` variable.
       - The merged DataFrame contains additional columns based on the data retrieved from the API, which may include creation dates and other relevant information.

    Example:
       df = add_creation_date_to_account(df, actual_type="COMPANY")
    """
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
    """
    Adds membership type and fee information to each account in a DataFrame.

    Parameters:
        df (pd.DataFrame): A pandas DataFrame containing account information.
                           Each row should represent an account with at least an "accountId" field.

    Returns:
        pd.DataFrame: The input DataFrame with two additional columns:
                      - "Membership Type": The type of membership associated with each account.
                      - "Fee": The fee associated with the membership type.

    Behavior:
        - Calls the `get_all_membership_types` function to retrieve membership types and fees for all accounts in the DataFrame.
        - Maps the retrieved membership types to the "accountId" column in the DataFrame, creating a new "Membership Type" column.
        - Maps the retrieved fees to the "accountId" column in the DataFrame, creating a new "Fee" column.
        - Returns the updated DataFrame with the added membership information.

    Notes:
        - Assumes that the DataFrame `df` contains a column named "accountId" which uniquely identifies each account.
        - The `get_all_membership_types` function is used to fetch the membership type and fee for each account.

    Example:
        df = add_membership_type_to_account(df)
    """
    membership_types, fees, term_end_dates, transactiom_dates, number_of_memberships = (
        get_all_membership_types(df)
    )
    df["Membership Type"] = df["accountId"].map(membership_types)
    df["Fee"] = df["accountId"].map(fees)
    df["Term End Date"] = df["accountId"].map(term_end_dates)
    df["Transaction Date"] = df["accountId"].map(transactiom_dates)
    df["Number of Memberships"] = df["accountId"].map(number_of_memberships)
    return df


def filter_non_active_accounts(df) -> pd.DataFrame:
    """
    Filters out accounts that do not have an active membership from a DataFrame.

    Parameters:
        df (pd.DataFrame): A pandas DataFrame containing account information, including a "Membership Type" column.

    Returns:
        pd.DataFrame: A filtered DataFrame containing only accounts with active memberships.

    Behavior:
        - Filters the input DataFrame by excluding rows where the "Membership Type" is equal to "No Membership active".
        - Returns a DataFrame with only the accounts that have an active membership.

    Notes:
        - Assumes that the DataFrame `df` contains a column named "Membership Type".
        - Accounts with "No Membership active" are considered non-active and are removed from the returned DataFrame.

    Example:
        active_accounts_df = filter_non_active_accounts(df)
    """
    return df[df["Membership Type"] != "No Membership active"]


def filter_individuals(individuals: pd.DataFrame) -> pd.DataFrame:
    """
    Filters out specific columns from a DataFrame of individual accounts, removing unnecessary information.

    Parameters:
        individuals (pd.DataFrame): A pandas DataFrame containing information about individual accounts.

    Returns:
        pd.DataFrame: The input DataFrame with specific columns removed.

    Behavior:
        - Defines a list of columns (`to_drop`) that are deemed unnecessary for the current context.
        - Drops the specified columns from the input DataFrame.
        - Returns the filtered DataFrame, which no longer includes the specified columns.

    Notes:
        - Assumes that the DataFrame `individuals` contains the columns listed in `to_drop`.
        - This function is used to simplify the DataFrame by removing detailed or irrelevant fields, focusing on the essential information.

    Example:
        filtered_individuals_df = filter_individuals(individuals_df)
    """
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
    # Filter columns that are no in companies.columns
    availlable_columns = individuals.columns
    to_drop = [column for column in to_drop if column in availlable_columns]
    individuals.drop(columns=to_drop, inplace=True)
    return individuals.dropna(axis=1, how="all")


def filter_companies(companies: pd.DataFrame) -> pd.DataFrame:
    """
    Filters out specific columns from a DataFrame of company accounts, removing unnecessary information and discarding columns that only contain NaN values.

    Parameters:
        companies (pd.DataFrame): A pandas DataFrame containing information about company accounts.

    Returns:
        pd.DataFrame: The input DataFrame with specific columns removed and columns with only NaN values dropped.

    Behavior:
        - Defines a list of columns (`to_drop`) that are deemed unnecessary for the current context.
        - Filters the `to_drop` list to include only columns that are present in the input DataFrame.
        - Drops the filtered columns from the input DataFrame.
        - Drops any columns in the resulting DataFrame that contain only NaN values.
        - Returns the cleaned DataFrame.

    Notes:
        - Assumes that the DataFrame `companies` contains various columns, including those listed in `to_drop`.
        - The function ensures that only existing columns are dropped, avoiding errors if some columns are not present in the DataFrame.

    Example:
        filtered_companies_df = filter_companies(companies_df)
    """
    to_drop = [
        "firstName",
        "lastName",
        "noSolicitation",
        "accountCustomFields",
        "sendSystemEmail",
        "accountCurrentMembershipStatus",
        "name",
        "primaryContact.contactId",
        "primaryContact.accountId",
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
    # Filter columns that are no in companies.columns
    availlable_columns = companies.columns
    to_drop = [column for column in to_drop if column in availlable_columns]
    companies.drop(columns=to_drop, inplace=True)

    return companies.dropna(axis=1, how="all")


def add_export_date(df) -> pd.DataFrame:
    """
    Adds a column to the DataFrame with the current date and time, labeled as "Export Date".

    Parameters:
        df (pd.DataFrame): A pandas DataFrame to which the export date will be added.

    Returns:
        pd.DataFrame: The input DataFrame with an additional column "Export Date" containing the current date and time.

    Behavior:
        - Adds a new column "Export Date" to the DataFrame.
        - The value of this column is set to the current date and time, formatted as "YYYY-MM-DD HH:MM:SS".
        - Returns the updated DataFrame.

    Notes:
        - The `datetime.now().strftime("%Y-%m-%d %H:%M:%S")` function is used to generate the current date and time.
        - This function is useful for timestamping the data when exporting or saving, providing a reference for when the data was processed or exported.

    Example:
        df = add_export_date(df)
    """
    df["Export Date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df


def add_fields_to_account(account: pd.DataFrame, actual) -> pd.DataFrame:
    """
    Enhances an account DataFrame by adding various fields and filtering based on account type, then returns the modified DataFrame.

    Parameters:
        account (pd.DataFrame): A pandas DataFrame containing account information.
        actual (str): The actual type of the accounts, either "INDIVIDUAL" or "COMPANY".

    Returns:
        pd.DataFrame: The input DataFrame with additional fields and filtering applied based on the account type.

    Behavior:
        - Adds membership type and fee information to the account DataFrame using `add_membership_type_to_account`.
        - Filters out accounts with no active membership using `filter_non_active_accounts`.
        - Adds event participation details to the account DataFrame using `add_events_to_account`.
        - Adds creation date and additional information to the account DataFrame using `add_creation_date_to_account`.
        - Filters the DataFrame based on the account type (`actual`):
            - If `actual` is "INDIVIDUAL", irrelevant columns are removed using `filter_individuals`.
            - If `actual` is "COMPANY", irrelevant columns are removed using `filter_companies`.
        - Adds an "Export Date" column with the current date and time using `add_export_date`.
        - Returns the fully processed and filtered DataFrame.

    Notes:
        - The `actual` parameter must be either "INDIVIDUAL" or "COMPANY". If an invalid type is provided, a `ValueError` is raised.
        - This function combines multiple processing steps to enrich and clean the account data, making it ready for export or analysis.

    Example:
        processed_accounts = add_fields_to_account(accounts_df, actual="COMPANY")
    """
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

    account = add_export_date(account)

    return account


def print_all_accounts_to_csv() -> None:
    """
    Retrieves all individual and company accounts, processes them to add additional fields, and saves them to CSV files.

    Parameters:
        None

    Returns:
        None

    Behavior:
        - Logs the start of the process for retrieving and saving all accounts to CSV.
        - Retrieves individual account data using `get_accounts_individuals`.
        - Retrieves company account data using `get_accounts_companies`.
        - Processes the individual accounts to add additional fields and filters using `add_fields_to_account` with the type "INDIVIDUAL".
        - Processes the company accounts to add additional fields and filters using `add_fields_to_account` with the type "COMPANY".
        - Saves the processed individual accounts to a CSV file named "individuals.csv".
        - Saves the processed company accounts to a CSV file named "companies.csv".

    Notes:
        - The CSV files are saved with headers included, and the indices are excluded from the files.
        - This function does not return anything; it only performs I/O operations to save the data to files.

    Example:
        print_all_accounts_to_csv()
    """
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
