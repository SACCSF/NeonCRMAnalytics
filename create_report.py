import logging
import logging.config
import os, ssl
import smtplib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from dotenv import load_dotenv
import pandas as pd

# loading logger
logging.config.fileConfig("NeonCRMAnalytics.log")
# loading variables from .env file
load_dotenv()

def check_content(csv_file):
    csv = pd.read_csv(csv_file)
    result = []
    for i, row in csv.iterrows():
        if(row.userType == "INDIVIDUAL"):
            if (pd.isna(row.email)):
                logging.debug("User " + str(row.accountId) + " has no email")
                result.append("User " + str(row.accountId) + " has no email")
            if (pd.isna(row.firstName)):
                logging.debug("User " + str(row.accountId) + " has no first name")
                result.append("User " + str(row.accountId) + " has no first name")
            if (pd.isna(row.lastName)):
                logging.debug("User " + str(row.accountId) + " has no last name")
                result.append("User " + str(row.accountId) + " has no last name")

        if(row.userType == "COMPANY"):
            if(pd.isna(row.email)):
                logging.debug("Company " + str(row.accountId) + " has no email")
                result.append("Company " + str(row.accountId) + " has no email")
            if(pd.isna(row.firstName or row.lastName)):
                logging.debug("Company " + str(row.accountId) + " has no primary contact")
                result.append("Company " + str(row.accountId) + " has no primary contact")

    df = pd.DataFrame(result)
    df.to_csv("Report.csv", index=False)

def check_dulicates(csv_file):
    csv = pd.read_csv(csv_file)
    duplicatedRows = csv[csv.duplicated(['firstName', 'lastName', 'email'])]
    duplicatedRows.to_csv("Duplicates.csv", index=False)

def main():
    # check_content("merged.csv")

    # check_dulicates("merged.csv")

    # Send email
    subject = "Swiss Chamber Report"
    body = "Test"
    recipients = ["flavio.waser@stud.hslu.ch"]
    #recipients = ["flavio.waser@stud.hslu.ch", "nicola.hermann@stud.hslu.ch","alex.Iruthayanesan@stud.hslu.ch"]
    #  send_email(subject, body, recipients)

if __name__ == '__main__':
    main()