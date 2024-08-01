import pandas as pd

# Load the CSV files
out = pd.read_csv("out.csv")
events = pd.read_csv("events_per_person.csv")

# Merge the two DataFrames
merged = pd.merge(
    out,
    events,
    on=["accountId", "firstName", "lastName", "email", "userType", "companyName"],
    how="outer",
    validate="one_to_one",
)

merged.to_csv("merged.csv", index=False)
