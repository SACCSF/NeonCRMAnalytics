import pandas as pd
import numpy as np
import plotly.express as px


def fee_vs_member_type(df, enable_raw_values=False):
    fee = df["Fee"]
    member_type = df["Membership Type"]

    fees = fee.unique()
    types = member_type.unique()

    # Count the number of each member type for each fee
    values_df = pd.DataFrame(
        np.zeros((len(types), len(fees))), index=types, columns=fees
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


def total_income_by_member_type_ploty(df: pd.DataFrame) -> px.bar:
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
    return fig


def membership_type_vs_events(df: pd.DataFrame):

    events = df["event_ids"]
    num_events = events.apply(lambda x: len(eval(x)))
    member_type = df["Membership Type"]

    event = num_events.unique()
    types = member_type.unique()

    # Count the number of each member type for each fee
    values_df = pd.DataFrame(
        np.zeros((len(types), len(event))), index=types, columns=event
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
    return values_df


def origin_vs_timestmap_created_date(df: pd.DataFrame):
    origin = df["origin.originCategory"]
    created_date = df["timestamps.createdDateTime"]

    origins = origin.unique()
    created_dates = created_date.unique()

    # Remove the NaN columns and rows
    origins = origins[~pd.isna(origins)]
    created_dates = created_dates[~pd.isna(created_dates)]

    # Count the number of each member type for each fee
    values_df = pd.DataFrame(
        np.zeros((len(origins), len(created_dates))),
        index=origins,
        columns=created_dates,
    )

    # Populate the DataFrame
    for i, o in enumerate(origins):
        for j, c in enumerate(created_dates):
            values_df.loc[o, c] = np.sum((origin == o) & (created_date == c))

    # Create column with the sum of all the rows
    values_df = values_df.T
    values_df["Grand Total"] = values_df.sum(axis=1)
    return values_df


def number_of_membershuis_vs_membership_type(df: pd.DataFrame):
    membership_type = df["Membership Type"]
    number_of_memberships = df["Number of Memberships"]

    types = membership_type.unique()
    memberships = number_of_memberships.unique()

    # Count the number of each member type for each fee
    values_df = pd.DataFrame(
        np.zeros((len(types), len(memberships))),
        index=types,
        columns=memberships,
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
    return df[df[col].isna()]["accountId"]


def get_empty_ids(df: pd.DataFrame, col: str) -> pd.DataFrame:
    return df[df[col].apply(lambda x: len(eval(x)) == 0)]["accountId"].values


def get_special_characters_id(df: pd.DataFrame, col: str) -> pd.DataFrame:
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
        "-",
        ":",
        ";",
        "'",
        '"',
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
        "`",
    ]
    return df[df[col].apply(lambda x: any(char in x for char in special_chars))][
        "accountId"
    ].values


if __name__ == "__main__":
    individuals = pd.read_csv("individuals.csv")
    companies = pd.read_csv("companies.csv")

    print(get_special_characters_id(individuals, "lastName"))
