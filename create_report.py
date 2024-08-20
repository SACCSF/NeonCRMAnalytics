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


def generate_report():
    quality_columns_individuals = get_quality_columns("individual")
    quality_columns_companies = get_quality_columns("company")
    # Set up Jinja2 environment
    env = Environment(loader=FileSystemLoader("."))
    template = env.get_template("report/template.html")

    export_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    individuals_df = pd.read_csv("individuals.csv")
    companies_df = pd.read_csv("companies.csv")

    individuals_members = filter_non_active_accounts(individuals_df)
    companies_members = filter_non_active_accounts(companies_df)

    individuals_nan = get_plotly_list_nan_values(
        individuals_df, quality_columns_individuals, "individuals"
    )
    organizations_nan = get_plotly_list_nan_values(
        companies_df, quality_columns_companies, "organizations"
    )

    individuals_fee_membership = fee_vs_member_type(individuals_df)
    organizations_fee_membership = fee_vs_member_type(companies_df)

    individuals_fee_membership_missmatch = fee_vs_member_type_missmatch(
        individuals_fee_membership
    )
    organizations_fee_membership_missmatch = fee_vs_member_type_missmatch(
        organizations_fee_membership
    )

    individuals_income_membership = total_income_by_member_type_ploty(individuals_df)
    organizations_income_membership = total_income_by_member_type_ploty(companies_df)

    individuals_membership_events_attended = membership_type_vs_events(individuals_df)
    organizations_membership_events_attended = membership_type_vs_events(companies_df)

    individuals_number_memberships_type = number_of_membership_vs_membership_type(
        individuals_df
    )
    orginizations_number_memberships_type = number_of_membership_vs_membership_type(
        companies_df
    )

    individuals_name_inconsistencies = get_name_inconsistencies(individuals_df)

    individuals_wrong_user_type = get_wrong_user_type_ids(individuals_df, "INDIVIDUAL")
    organizations_wrong_user_type = get_wrong_user_type_ids(
        companies_df, "COMPANY"
    )

    combined_new_account_registrations_plot = get_account_creation_date_plot(
        individuals_df, companies_df
    )

    rendered_html = template.render(
        export_date=export_date,
        individuals_df=individuals_df,
        organizations_df=companies_df,
        individuals_members=individuals_members,
        organizations_members=companies_members,
        individuals_nan=individuals_nan,
        organizations_nan=organizations_nan,
        individuals_fee_membership=individuals_fee_membership,
        organizations_fee_membership=organizations_fee_membership,
        individuals_fee_membership_missmatch=individuals_fee_membership_missmatch,
        organizations_fee_membership_missmatch=organizations_fee_membership_missmatch,
        individuals_income_membership=individuals_income_membership,
        organizations_income_membership=organizations_income_membership,
        individuals_membership_events_attended=individuals_membership_events_attended,
        organizations_membership_events_attended=organizations_membership_events_attended,
        individuals_number_memberships_type=individuals_number_memberships_type,
        orginizations_number_memberships_type=orginizations_number_memberships_type,
        individuals_name_inconsistencies=individuals_name_inconsistencies,
        individuals_wrong_user_type=individuals_wrong_user_type,
        organizations_wrong_user_type=organizations_wrong_user_type,
        combined_new_account_registrations_plot=combined_new_account_registrations_plot,
    )
    # Save the rendered HTML to a file
    with open("docs/report.html", "w") as f:
        f.write(rendered_html)


if __name__ == "__main__":
    generate_report()
