import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from typing import List, Tuple


def get_quality_columns(mode: str):
    """
    Returns a list of quality columns based on the mode provided.

    Parameters:
    mode (str): The mode for which the quality columns are needed. It can be either "individual" or "company".

    Returns:
    list: A list of quality columns.
    """
    res = ["email"]
    if mode.lower() == "individual":
        res += [
            "firstName",
            "lastName",
            "companyName",
        ]
    elif mode.lower() == "company":
        res += [
            "companyName",
            "primaryContactAccountId",
        ]
    return res


def fee_vs_member_type(df, enable_raw_values=False):
    """
    Counts the number of each member type for each fee.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the data.
    enable_raw_values (bool): Whether to return raw values. Default is False.

    Returns:
    pd.DataFrame: A DataFrame with the count of each member type for each fee.
    """
    fee = df["Fee"]
    member_type = df["Membership Type"]

    fees = fee.unique()
    types = member_type.unique()

    # Count the number of each member type for each fee
    values_df = pd.DataFrame(
        np.zeros((len(types), len(fees))),
        index=types,
        columns=fees,
        dtype=int,
    )

    # Populate the DataFrame
    for i, f in enumerate(fees):
        for j, t in enumerate(types):
            values_df.loc[t, f] = np.sum((fee == f) & (member_type == t))

    # Rename the columns with a $ sign
    raw_values = list(values_df.columns.copy())
    values_df.columns = [f"{col}$" for col in values_df.columns]
    if enable_raw_values:
        return values_df, raw_values

    return values_df


def fee_vs_member_type_missmatch(df: pd.DataFrame) -> pd.DataFrame:
    """
    Identifies mismatches in fee vs member type.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the data.

    Returns:
    pd.DataFrame: A DataFrame with the mismatched member types.
    """
    res = []
    for name, row in df.iterrows():
        uniques = row.unique()
        # Remove 0 from the list
        uniques = uniques[uniques != 0]
        if len(uniques) > 1:
            res.append(name)
    return res


def total_income_by_member_type_ploty(df: pd.DataFrame) -> px.bar:
    """
    Plots total income by member type.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the data.

    Returns:
    px.bar: A Plotly bar chart.
    """
    values, raw_values = fee_vs_member_type(df, True)
    # put index into a column
    values.reset_index(inplace=True)

    # all columns except the first one
    columns = list(values.columns[1:])
    values.rename(columns={"index": "Membership Type"}, inplace=True)
    # Multiply the raw values by the fee
    for i, row in values.iterrows():
        for j, col in enumerate(columns):
            values.at[i, col] = row[col] * raw_values[j]

    fig = px.bar(
        values, x="Membership Type", y=columns, title="Total Income by Member Type"
    )

    fig_html = fig.to_html(full_html=False, include_plotlyjs=False)
    return fig_html


def membership_type_vs_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    Counts the number of each member type for each event.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the data.

    Returns:
    pd.DataFrame: A DataFrame with the count of each member type for each event.
    """
    events = df["event_ids"]
    num_events = events.apply(lambda x: len(eval(x)))
    member_type = df["Membership Type"]

    past_members = get_past_members(df)
    # change the past members to "Past Member"
    member_type[past_members.index] = "Past Member"

    event = num_events.unique()
    types = member_type.unique()

    # Count the number of each member type for each fee
    values_df = pd.DataFrame(
        np.zeros((len(types), len(event))),
        index=types,
        columns=event,
        dtype=int,
    )

    # Populate the DataFrame
    for i, e in enumerate(event):
        for j, t in enumerate(types):
            values_df.loc[t, e] = np.sum((num_events == e) & (member_type == t))

    # Get all columns except "0", "1", "2", "3"
    columns = list(values_df.columns[values_df.columns > 3])
    # combine the columns
    values_df["4+"] = values_df[columns].sum(axis=1)
    values_df.drop(columns=columns, inplace=True)
    # Reorder the columns
    values_df = values_df[[0, 1, 2, 3, "4+"]]
    # Add grand totals
    values_df["Grand Total"] = values_df.sum(axis=1)
    values_df.loc["Grand Total"] = values_df.sum(axis=0)
    return values_df


def number_of_membership_vs_membership_type(df: pd.DataFrame):
    """
    Counts the number of each membership type.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the data.

    Returns:
    pd.DataFrame: A DataFrame with the count of each membership type.
    """
    membership_type = df["Membership Type"]
    number_of_memberships = df["Number of Memberships"]

    types = membership_type.unique()
    memberships = number_of_memberships.unique()

    # Count the number of each member type for each fee
    values_df = pd.DataFrame(
        np.zeros((len(types), len(memberships))),
        index=types,
        columns=memberships,
        dtype=int,
    )

    # Populate the DataFrame
    for i, m in enumerate(memberships):
        for j, t in enumerate(types):
            values_df.loc[t, m] = np.sum(
                (number_of_memberships == m) & (membership_type == t)
            )

    # sort orders
    values_df = values_df[sorted(values_df.columns)].T

    # Create column with the sum of all the rows
    values_df["Grand Total"] = values_df.sum(axis=1)
    return values_df


def get_missing_ids(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Returns the IDs of missing values in a column.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the data.
    col (str): The column to check for missing values.

    Returns:
    pd.DataFrame: A DataFrame with the IDs of missing values.
    """
    return df[df[col].isna()]["accountId"]


def get_empty_ids(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Returns the IDs of empty values in a column.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the data.
    col (str): The column to check for empty values.

    Returns:
    pd.DataFrame: A DataFrame with the IDs of empty values.
    """
    return df[df[col].apply(lambda x: len(eval(x)) == 0)]["accountId"].values


def get_special_characters_id(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Returns the IDs of values with special characters in a column.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the data.
    col (str): The column to check for special characters.

    Returns:
    pd.DataFrame: A DataFrame with the IDs of values with special characters.
    """
    special_chars = [
        "!",
        "@",
        "#",
        "$",
        "%",
        "^",
        "&",
        "*",
        "(",
        ")",
        "_",
        "+",
        "=",
        ":",
        ";",
        ",",
        ".",
        "<",
        ">",
        "/",
        "?",
        "|",
        "\\",
        "]",
        "[",
        "{",
        "}",
        "~",
    ]
    return df[
        df[col].apply(
            lambda x: isinstance(x, str) and any(char in x for char in special_chars)
        )
    ][["accountId", "firstName", "lastName"]]


def get_plotly_list_nan_values(df: pd.DataFrame, columns: list, mode: str) -> list:
    """
    Returns a list of Plotly charts for NaN values in specified columns.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the data.
    columns (list): The list of columns to check for NaN values.
    mode (str): The mode for which the charts are needed.

    Returns:
    list: A list of Plotly charts for NaN values.
    """
    charts = {}
    url_dict = fetch_report_urls(columns, mode)
    filter_columns = ["accountId"] + columns
    plotly_df = df.rename(
        columns={
            "timestamps.createdBy": "timestamps",
            "origin.originDetail": "origin",
        }
    )[filter_columns]
    for column in columns:
        nan_ids = get_missing_ids(plotly_df, column)
        plotly_fig = go.Figure(
            data=[
                go.Pie(
                    labels=["Valid Data", "Missing Data"],
                    values=[len(plotly_df) - len(nan_ids), len(nan_ids)],
                    hole=0.3,
                )
            ]
        )

        plotly_fig.update_traces(
            marker=dict(
                colors=["#50C878", "#FF0000"], line=dict(color="#000000", width=2)
            ),
            showlegend=False,
        )
        chart_html = plotly_fig.to_html(full_html=False, include_plotlyjs=False)
        charts[column] = chart_html
    return [(charts[column], url_dict[column], str(column)) for column in columns]


def get_name_inconsistencies(individuals: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a DataFrame with name inconsistencies.

    Parameters:
    individuals (pd.DataFrame): The DataFrame containing the data.

    Returns:
    pd.DataFrame: A DataFrame with name inconsistencies.
    """
    first_name = get_special_characters_id(individuals, "firstName")
    last_name = get_special_characters_id(individuals, "lastName")

    # Concat the with an additional column stating the column of the special character
    first_name["where"] = "firstName"
    last_name["where"] = "lastName"
    res = pd.concat([first_name, last_name])
    res.reset_index(drop=True, inplace=True)
    # Add url to the DataFrame
    base_url = "https://saccsf.app.neoncrm.com/admin/accounts/*/about"
    res["url"] = res["accountId"].astype(str).apply(lambda x: base_url.replace("*", x))
    return res


def get_wrong_user_type_ids(df: pd.DataFrame, expected_value: str) -> pd.DataFrame:
    """
    Returns the IDs of rows with wrong user types.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the data.
    expected_value (str): The expected user type.

    Returns:
    pd.DataFrame: A DataFrame with the IDs of rows with wrong user types.
    """
    return df[df["userType"] != expected_value]["accountId"].to_list()


def get_account_creation_date_plot(df: pd.DataFrame) -> go.Figure:
    """
    Plots account creation dates by quarter.

    Parameters:
    individuals (pd.DataFrame): The DataFrame containing individual accounts.
    companies (pd.DataFrame): The DataFrame containing company accounts.

    Returns:
    go.Figure: A Plotly figure with account creation dates by quarter.
    """
    accounts = df.copy()
    accounts["timestamps.createdDateTime"] = pd.to_datetime(
        accounts["timestamps.createdDateTime"]
    )

    # Remove the rows with NaN values
    accounts = accounts[~pd.isna(accounts["timestamps.createdDateTime"])]

    # Group the dates by quarters and convert to integers
    accounts["Quarter"] = accounts["timestamps.createdDateTime"].dt.quarter.astype(int)
    accounts["Year"] = accounts["timestamps.createdDateTime"].dt.year.astype(int)

    # Create a histogram based on the quarters
    fig = go.Figure()
    for quarter in sorted(accounts["Quarter"].unique()):
        quarter_df = accounts[accounts["Quarter"] == quarter]

        fig.add_trace(
            go.Histogram(
                x=quarter_df["Year"],
                name=f"Q{quarter}",
                histfunc="count",
                xbins=dict(size=1),
            )
        )

    fig_html = fig.to_html(full_html=False, include_plotlyjs=False)

    return fig_html


def get_31_dec_term_end_table_plot(
    df: pd.DataFrame, mode: str
) -> Tuple[pd.DataFrame, go.Figure]:

    accounts = df.copy()

    accounts["Term End Date"] = pd.to_datetime(df["Term End Date"])

    term_31_dec = accounts[
        (accounts["Term End Date"].dt.month == 12)
        & (accounts["Term End Date"].dt.day == 31)
    ]
    if mode == "individuals":
        term_31_dec_filtered = term_31_dec[["accountId", "firstName", "lastName"]]
    else:
        term_31_dec_filtered = term_31_dec[["accountId", "companyName"]]
    # Create pie chart of percentage of members with term end date 31 Dec
    fig = go.Figure(
        data=[
            go.Pie(
                labels=[
                    "Members with 31 Dec Term End Date",
                    "Members with Other Term End Date",
                ],
                values=[
                    len(term_31_dec_filtered),
                    len(accounts) - len(term_31_dec_filtered),
                ],
                hole=0.3,
            )
        ]
    )

    fig.update_traces(
        marker=dict(colors=["#FF0000", "#50C878"], line=dict(color="#000000", width=2)),
        showlegend=False,
    )

    fig_html = fig.to_html(full_html=False, include_plotlyjs=False)

    # Load URL from txt file
    with open("references.txt", "r") as f:
        lines = f.read().splitlines()

    if mode == "individuals":
        url = lines[7].split(";")[1]
        title = "Individuals with a Membership End Date of 31 Dec"
    else:
        url = lines[8].split(";")[1]
        title = "Companies with a Membership End Date of 31 Dec"

    return (fig_html, url, title)


def get_members(df) -> pd.DataFrame:
    """
    Filters out non-active accounts.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the data.

    Returns:
    pd.DataFrame: A DataFrame with only active accounts.
    """
    return df[df["Membership Type"] != "No Membership active"]


def get_non_members(df) -> pd.DataFrame:
    """
    Filters out active accounts.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the data.

    Returns:
    pd.DataFrame: A DataFrame with only non-active accounts.
    """
    return df[df["Membership Type"] == "No Membership active"]


def get_past_members(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a DataFrame with past member accounts.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the data.

    Returns:
    pd.DataFrame: A DataFrame with past member accounts.
    """
    non_members = get_non_members(df)
    past_members = non_members[non_members["Number of Memberships"] > 0]
    return past_members


def fetch_report_urls(columns, mode):
    """
    Fetches report URLs for specified columns and mode.

    Parameters:
    columns (list): The list of columns.
    mode (str): The mode for which the URLs are needed.

    Returns:
    dict: A dictionary with columns as keys and URLs as values.
    """
    with open("references.txt", "r") as f:
        lines = f.read().splitlines()

    url_dict = {}

    for column in columns:
        for line in lines:
            name, url = line.split(";")
            if not mode in name:
                continue
            if not column in name:
                continue
            url_dict[column] = url
    return url_dict


if __name__ == "__main__":
    individuals = pd.read_csv("individuals.csv")
    companies = pd.read_csv("companies.csv")
