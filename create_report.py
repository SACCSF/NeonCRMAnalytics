import datetime
import logging
import logging.config
from dotenv import load_dotenv
import pandas as pd
import plotly.graph_objects as go
from metrics import *
import json

from jinja2 import Environment, FileSystemLoader

# loading logger
logging.config.fileConfig("NeonCRMAnalytics.log")
# loading variables from .env file
load_dotenv()


def generate_report():
    quality_columns_individuals = get_quality_columns("individual")
    quality_columns_companies = get_quality_columns("company")
    # Set up Jinja2 environment
    env = Environment(loader=FileSystemLoader("."))
    template = env.get_template("report/template.html")

    export_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    # Import json file as dictionary
    with open("report/menu.json") as f:
        individuals = json.load(f)

    individuals_df = pd.read_csv("individuals.csv")
    companies_df = pd.read_csv("companies.csv")

    individual_members = get_members(individuals_df)
    company_members = get_members(companies_df)

    individual_non_members = get_non_members(individuals_df)
    company_non_members = get_non_members(companies_df)

    individual_past_members = get_past_members(individuals_df)
    company_past_members = get_past_members(companies_df)

    all_individual_members = pd.concat([individual_members, individual_past_members])
    all_company_members = pd.concat([company_members, company_past_members])

    im_fee_vs_membership = fee_vs_member_type(individual_members)
    cm_fee_vs_membership = fee_vs_member_type(company_members)

    idf_event_attendance = membership_type_vs_events(individuals_df)
    cdf_event_attendance = membership_type_vs_events(companies_df)

    im_incomplete_data = get_plotly_list_nan_values(
        individual_members, quality_columns_individuals, "individuals"
    )
    cm_incomplete_data = get_plotly_list_nan_values(
        company_members, quality_columns_companies, "organizations"
    )
    inm_incomplete_data = get_plotly_list_nan_values(
        individual_non_members, quality_columns_individuals, "individuals"
    )
    cnm_incomplete_data = get_plotly_list_nan_values(
        company_non_members, quality_columns_companies, "organizations"
    )

    im_inconsistent_data = get_name_inconsistencies(individual_members)
    inm_inconsistent_data = get_name_inconsistencies(individual_non_members)

    im_term_end_table_plot = get_31_dec_term_end_table_plot(
        individual_members, "individuals"
    )
    cm_term_end_table_plot = get_31_dec_term_end_table_plot(
        company_members, "organizations"
    )

    members_account_creation = get_account_creation_date_plot(
        individual_members, company_members
    )

    all_account_creation = get_account_creation_date_plot(
        all_individual_members, all_company_members
    )

    im_total_income = total_income_by_member_type_ploty(
        individual_members, "individuals"
    )
    cm_total_income = total_income_by_member_type_ploty(
        company_members, "organizations"
    )

    return
    # Save the rendered HTML to a file
    with open("docs/report.html", "w") as f:
        f.write(rendered_html)


if __name__ == "__main__":
    generate_report()
