import logging
import logging.config

import numpy as np
import pandas as pd

# loading logger
logging.config.fileConfig("NeonCRMAnalytics.log")

def check_content(csv_file):
    csv = pd.read_csv(csv_file)
    result = []
    for i, row in csv.iterrows():
        if(row.userType == "INDIVIDUAL"):
            if (pd.isna(row.email)):
                result.append("User " + str(row.accountId) + " has no email")
            if (pd.isna(row.firstName)):
                result.append("User " + str(row.accountId) + " has no first name")
            if (pd.isna(row.lastName)):
                result.append("User " + str(row.accountId) + " has no last name")

        if(row.userType == "COMPANY"):
            if(pd.isna(row.email)):
                result.append("Company " + str(row.accountId) + " has no email")
            if(pd.isna(row.firstName or row.lastName)):
                result.append("Company " + str(row.accountId) + " has no primary contact")

    df = pd.DataFrame(result)
    df.to_csv("Report.csv", index=False)

def main():
    check_content("out.csv")

if __name__ == '__main__':
    main()