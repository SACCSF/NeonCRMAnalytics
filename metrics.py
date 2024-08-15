import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go


def fee_vs_member_type(df, enable_raw_values=False):
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
    res = []
    for name, row in df.iterrows():
        uniques = row.unique()
        # Remove 0 from the list
        uniques = uniques[uniques != 0]
        if len(uniques) > 1:
            res.append(name)
    return res


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

    fig_html = fig.to_html(full_html=False, include_plotlyjs=False)
    return fig_html


def membership_type_vs_events(df: pd.DataFrame):

    events = df["event_ids"]
    num_events = events.apply(lambda x: len(eval(x)))
    member_type = df["Membership Type"]

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
    return values_df


def number_of_membership_vs_membership_type(df: pd.DataFrame):
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
    return df[
        df[col].apply(
            lambda x: isinstance(x, str) and any(char in x for char in special_chars)
        )
    ][["accountId", "firstName", "lastName"]]


def get_plotly_list_nan_values(df: pd.DataFrame, columns: list) -> list:
    charts, nan_ids_dict = {}, {}
    filter_columns = ["accountId"] + columns
    plotly_df = df.rename(
        columns={
            "timestamps.createdBy": "timestamps",
            "origin.originDetail": "origin",
        }
    )[filter_columns]
    for column in columns:
        nan_ids = get_missing_ids(plotly_df, column)
        nan_ids_dict[column] = nan_ids
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
    return [(charts[column], nan_ids_dict[column], str(column)) for column in columns]


def get_name_inconsistencies(individuals: pd.DataFrame) -> pd.DataFrame:
    first_name = get_special_characters_id(individuals, "firstName")
    last_name = get_special_characters_id(individuals, "lastName")

    # Concat the with an additional column stating the column of the special character
    first_name["where"] = "firstName"
    last_name["where"] = "lastName"
    res = pd.concat([first_name, last_name])
    res.reset_index(drop=True, inplace=True)
    return res


def get_wrong_user_type_ids(df: pd.DataFrame, expected_value: str) -> pd.DataFrame:
    return df[df["userType"] != expected_value]["accountId"].to_list()


def get_account_creation_date_plot(
    individuals: pd.DataFrame, companies: pd.DataFrame
) -> go.Figure:
    accounts = pd.concat([individuals, companies]).reset_index(drop=True)
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

    return fig


if __name__ == "__main__":
    individuals = pd.read_csv("individuals.csv")
    companies = pd.read_csv("companies.csv")

    fees = fee_vs_member_type(individuals)
    missmatch = fee_vs_member_type_missmatch(fees)

    print(missmatch)

    fees = fee_vs_member_type(companies)
    missmatch = fee_vs_member_type_missmatch(fees)

    print(missmatch)
