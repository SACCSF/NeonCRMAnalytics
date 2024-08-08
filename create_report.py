import logging
import logging.config
from dotenv import load_dotenv
import pandas as pd

# loading logger
logging.config.fileConfig("NeonCRMAnalytics.log")
# loading variables from .env file
load_dotenv()


def check_individuals(csv_file):
    df = pd.read_csv(csv_file)
    # get percentage of individuals for each column
    percentage = df.count() / len(df)
    # Exclude certain columns from percentage calculation
    percentage = percentage.drop(["accountId", "primaryContact.email3"])
    addresses = df["primaryContact.addresses"].apply(
        lambda x: eval(x) if isinstance(x, str) else []
    )
    len_addresses = addresses.apply(lambda x: len(x))
    print(len_addresses.unique())


def check_dulicates(csv_file):
    csv = pd.read_csv(csv_file)
    duplicatedRows = csv[csv.duplicated(["firstName", "lastName", "email"])]
    duplicatedRows.to_csv("Duplicates.csv", index=False)


def main():
    individual_csv = "individuals.csv"
    company_csv = "companies.csv"

    individual = check_individuals(individual_csv)
    company = check_companies(company_csv)

    combinations = check_conbinations(individual_csv, company_csv)


if __name__ == "__main__":
    main()
