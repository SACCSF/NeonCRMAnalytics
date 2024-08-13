import datetime
import logging
import logging.config
from dotenv import load_dotenv
import pandas as pd
import plotly.graph_objects as go

from jinja2 import Environment, FileSystemLoader

# loading logger
logging.config.fileConfig("NeonCRMAnalytics.log")
# loading variables from .env file
load_dotenv()

pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 1000)


def get_event_plot_categories(df: pd.DataFrame):

    splits = [6, 4, 2, 1, 0]
    # Allocate the splits to the number of attended events

    for i, row in df.iterrows():
        for j, split in enumerate(splits):
            if len(eval(row["event_ids"])) >= split:
                df.at[i, f"event_plot_category"] = j
                break
    return df


def get_nan_ids(df: pd.DataFrame, column: str) -> pd.DataFrame:
    return df[df[column].isnull()]["accountId"]


def get_quality_columns(mode: str):
    res = [
        "firstName",
        "lastName",
        "companyName",
        "email",
        "timestamps",
        "origin",
    ]
    return res


def generate_quality_report_individuals(df: pd.DataFrame):
    quality_columns = get_quality_columns("individual")
    charts = {}
    nan_ids_dict = {}
    for column in quality_columns:
        nan_ids = get_nan_ids(df, column)
        nan_ids_dict[column] = nan_ids
        fig = go.Figure(
            data=[
                go.Pie(
                    labels=["Valid Data", "Missing Data"],
                    values=[len(df) - len(nan_ids), len(nan_ids)],
                    hole=0.3,
                )
            ]
        )
        # change color to red and green
        fig.update_traces(
            marker=dict(
                colors=["#00FF00", "#FF0000"], line=dict(color="#000000", width=2)
            ),
            showlegend=False,
        )

        # Disable legend

        chart_html = fig.to_html(full_html=False, include_plotlyjs=False)
        charts[column] = chart_html

    template_data = [
        (charts[column], nan_ids_dict[column], str(column))
        for column in quality_columns
    ]

    # Set up Jinja2 environment
    env = Environment(loader=FileSystemLoader("."))
    template = env.get_template("quality_report_individual.html")

    date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    # Render the template with the data
    # individuals_nan, individuals_fee_membership, individuals_income_membership, individuals_membership_events_attended, individuals_origin_time_created, individuals_number_memberships_type
    # organizations_nan, organizations_fee_membership, organizations_income_membership, organizations_membership_events_attended, organizations_origin_time_created, orginizations_number_memberships_type
    rendered_html = template.render(individuals_nan=template_data, export_date=date)

    # Save the rendered HTML to a file
    with open("nan_report_combined_jinja.html", "w") as f:
        f.write(rendered_html)


def check_individuals(csv_file):
    df = pd.read_csv(csv_file)
    total_individuals = len(df)
    # get percentage of individuals for each column
    percentage = df.count() / total_individuals

    # Exclude certain columns from percentage calculation
    percentage = percentage.drop(
        [
            "accountId",
            "primaryContact.email3",
            "Export Date",
            "timestamps.createdDateTime",
            "timestamps.lastModifiedBy",
            "timestamps.lastModifiedDateTime",
            "origin.originCategory",
        ]
    )

    # reneme columns
    df.rename(
        columns={
            "timestamps.createdBy": "timestamps",
            "origin.originDetail": "origin",
        },
        inplace=True,
    )

    addresses = df["primaryContact.addresses"].apply(
        lambda x: eval(x) if isinstance(x, str) else []
    )
    len_addresses = addresses.apply(lambda x: len(x))

    # get percentage of addresses that are above 0
    percentage["primaryContact.addresses"] = len_addresses[
        len_addresses > 0
    ].count() / len(len_addresses)

    generate_quality_report_individuals(df)
    return
    breakpoint()

    # Check for wrong user types
    wrong_user_types = df[df["userType"] != "INDIVIDUAL"]["accountId"]
    wrong_user_types_percentage = len(wrong_user_types) / total_individuals

    # Add event plot categories
    df = get_event_plot_categories(df)
    breakpoint()


def check_dulicates(csv_file):
    csv = pd.read_csv(csv_file)
    duplicatedRows = csv[csv.duplicated(["firstName", "lastName", "email"])]
    duplicatedRows.to_csv("Duplicates.csv", index=False)


def main():
    individual_csv = "individuals.csv"
    company_csv = "companies.csv"

    individual = check_individuals(individual_csv)
    return
    company = check_companies(company_csv)

    combinations = check_conbinations(individual_csv, company_csv)


if __name__ == "__main__":
    main()
