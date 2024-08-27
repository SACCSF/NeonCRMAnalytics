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
        menu_json = json.load(f)

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

    menu_json["data"]["individuals"]["feeVsMembers"]["members"]["data"] = (
        fee_vs_member_type(individual_members)
    )
    menu_json["data"]["organizations"]["feeVsMembers"]["members"]["data"] = (
        fee_vs_member_type(company_members)
    )

    menu_json["data"]["individuals"]["accountVsEvents"]["all"]["data"] = (
        membership_type_vs_events(individuals_df)
    )
    menu_json["data"]["organizations"]["accountVsEvents"]["all"]["data"] = (
        membership_type_vs_events(companies_df)
    )

    menu_json["data"]["individuals"]["incompleteData"]["members"]["data"] = (
        get_plotly_list_nan_values(
            individual_members, quality_columns_individuals, "individuals"
        )
    )
    menu_json["data"]["organizations"]["incompleteData"]["members"]["data"] = (
        get_plotly_list_nan_values(
            company_members, quality_columns_companies, "organizations"
        )
    )
    menu_json["data"]["individuals"]["incompleteData"]["nonMembers"]["data"] = (
        get_plotly_list_nan_values(
            individual_non_members, quality_columns_individuals, "individuals"
        )
    )
    menu_json["data"]["organizations"]["incompleteData"]["nonMembers"]["data"] = (
        get_plotly_list_nan_values(
            company_non_members, quality_columns_companies, "organizations"
        )
    )
    menu_json["data"]["individuals"]["incompleteData"]["all"]["data"] = (
        get_plotly_list_nan_values(
            individuals_df, quality_columns_individuals, "individuals"
        )
    )
    menu_json["data"]["organizations"]["incompleteData"]["all"]["data"] = (
        get_plotly_list_nan_values(
            companies_df, quality_columns_companies, "organizations"
        )
    )

    menu_json["data"]["individuals"]["inconsistantData"]["members"]["data"] = (
        get_name_inconsistencies(individual_members)
    )
    menu_json["data"]["individuals"]["inconsistantData"]["nonMembers"]["data"] = (
        get_name_inconsistencies(individual_non_members)
    )
    menu_json["data"]["individuals"]["inconsistantData"]["all"]["data"] = (
        get_name_inconsistencies(individuals_df)
    )

    menu_json["data"]["individuals"]["termEndDecember31"]["members"]["data"] = (
        get_31_dec_term_end_table_plot(individual_members, "individuals")
    )

    menu_json["data"]["organizations"]["termEndDecember31"]["members"]["data"] = (
        get_31_dec_term_end_table_plot(company_members, "organizations")
    )

    menu_json["data"]["individuals"]["memberCreationDate"]["members"]["data"] = (
        get_account_creation_date_plot(all_individual_members)
    )

    menu_json["data"]["organizations"]["memberCreationDate"]["members"]["data"] = (
        get_account_creation_date_plot(company_members)
    )

    menu_json["data"]["individuals"]["memberCreationDate"]["pastMembers"]["data"] = (
        get_account_creation_date_plot(individual_past_members)
    )

    menu_json["data"]["organizations"]["memberCreationDate"]["pastMembers"]["data"] = (
        get_account_creation_date_plot(company_past_members)
    )

    menu_json["data"]["individuals"]["memberCreationDate"]["both"]["data"] = (
        get_account_creation_date_plot(all_individual_members)
    )

    menu_json["data"]["organizations"]["memberCreationDate"]["both"]["data"] = (
        get_account_creation_date_plot(all_company_members)
    )

    menu_json["data"]["individuals"]["totalIncome"]["members"]["data"] = (
        total_income_by_member_type_ploty(individual_members)
    )
    menu_json["data"]["organizations"]["totalIncome"]["members"]["data"] = (
        total_income_by_member_type_ploty(company_members)
    )

    rendered_html = template.render(export_date=export_date, data=menu_json["data"])

    # Save the rendered HTML to a file
    with open("docs/report.html", "w") as f:
        f.write(rendered_html)


if __name__ == "__main__":
    generate_report()
