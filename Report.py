import logging
import logging.config
import os
import smtplib
from email.mime.text import MIMEText
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

def send_email(subject, body, sender, recipients, password):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
       smtp_server.login(sender, password)
       smtp_server.sendmail(sender, recipients, msg.as_string())
    logging.info("E-Mail message has been sent!")

def main():
    #check_content("merged.csv")

    check_dulicates("merged.csv")

    # Send email
    subject = "Swiss Chamber Report"
    body = "Test"
    sender = "saccsf.neon@gmail.com"
    recipients = ["flavio.waser@stud.hslu.ch"]
    #recipients = ["flavio.waser@stud.hslu.ch", "nicola.hermann@stud.hslu.ch","alex.Iruthayanesan@stud.hslu.ch"]
    #send_email(subject, body, sender, recipients, os.getenv("mail_password"))
    send_email(subject, body, sender, recipients, os.getenv("mail_password"))


if __name__ == '__main__':
    main()