import datetime
import logging
import logging.config
from dotenv import load_dotenv
import pandas as pd
import plotly.graph_objects as go
from metrics import *

from jinja2 import Environment, FileSystemLoader

# loading logger
logging.config.fileConfig("NeonCRMAnalytics.log")
# loading variables from .env file
load_dotenv()


def get_quality_columns(mode: str):
    res = ["email", "timestamps", "origin"]
    if mode == "individual":
        res += [
            "firstName",
            "lastName",
            "companyName",
        ]
    elif mode == "company":
        res += [
            "companyName",
            "primaryContactAccountId",
        ]
    return res


def generate_report():
    quality_columns_individuals = get_quality_columns("individual")
    quality_columns_companies = get_quality_columns("company")
    # Set up Jinja2 environment
    env = Environment(loader=FileSystemLoader("."))
    template = env.get_template("quality_report_individual.html")

    export_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    individuals_df = pd.read_csv("individuals.csv")
    companies_df = pd.read_csv("companies.csv")

    individuals_nan = get_plotly_list_nan_values(
        individuals_df, quality_columns_individuals
    )
    organizations_nan = get_plotly_list_nan_values(
        companies_df, quality_columns_companies
    )

    individuals_fee_membership = fee_vs_member_type(individuals_df)
    organizations_fee_membership = fee_vs_member_type(companies_df)

    individuals_income_membership = total_income_by_member_type_ploty(individuals_df)
    organizations_income_membership = total_income_by_member_type_ploty(companies_df)

    individuals_membership_events_attended = membership_type_vs_events(individuals_df)
    organizations_membership_events_attended = membership_type_vs_events(companies_df)

    individuals_origin_time_created = origin_vs_timestamp_created_date(individuals_df)
    organizations_origin_time_created = origin_vs_timestamp_created_date(companies_df)

    individuals_number_memberships_type = number_of_membership_vs_membership_type(
        individuals_df
    )
    orginizations_number_memberships_type = number_of_membership_vs_membership_type(
        companies_df
    )

    rendered_html = template.render(
        export_date=export_date,
        individuals_nan=individuals_nan,
        organizations_nan=organizations_nan,
        individuals_fee_membership=individuals_fee_membership,
        organizations_fee_membership=organizations_fee_membership,
        individuals_income_membership=individuals_income_membership,
        organizations_income_membership=organizations_income_membership,
        individuals_membership_events_attended=individuals_membership_events_attended,
        organizations_membership_events_attended=organizations_membership_events_attended,
        individuals_origin_time_created=individuals_origin_time_created,
        organizations_origin_time_created=organizations_origin_time_created,
        individuals_number_memberships_type=individuals_number_memberships_type,
        orginizations_number_memberships_type=orginizations_number_memberships_type,
    )
    # Save the rendered HTML to a file
    with open("report/report.html", "w") as f:
        f.write(rendered_html)


if __name__ == "__main__":
    generate_report()
