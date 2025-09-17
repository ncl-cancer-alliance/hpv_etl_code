# Import Packages

import glob  # Allows dynamic file selection
import os
import pandas as pd
import utils.database_util as db
from datetime import datetime
from dotenv import load_dotenv
from snowflake.connector import connect

## Load Data

# Load all relevant Excel files
file_paths = glob.glob("./data/HPV-data-tables-*.xlsx")  # Selects all matching files

# Create an empty list to store results
dataframes = []

# Loop through all the files, transform and append

for file_name in file_paths:

    ## Load Table Data
    df = pd.read_excel(file_name, sheet_name="Local_authority", header=2)
    
    # Load Date from cell A1
    excel_data = pd.read_excel(file_name, sheet_name="Local_authority", header=None)
    


    ## Transform Data
    # Clean Borough column header
    df['Local authority'] = df['Local authority'].str.strip()
    df['Local authority'] = df['Local authority'].str.title()
    
    ##df = df[df['Local authority'].isin(['Barnet','Enfield','Islington','Haringey','Camden'])]

    # Remove columns with %
    df = df.drop(columns=[col for col in df.columns if '%' in col])
    # Remove Columns of data for 2 Doses
    df = df.drop(columns=[col for col in df.columns if '2 doses' in col])

    # Melt to reshape columns
    df_melted = df.melt(id_vars=["Local authority"], var_name="Category", value_name="Value")

    # Extract Year Group, Gender, and Metric Type
    df_melted["Year Group"] = df_melted["Category"].str.extract(r"(\d+)")  # Extracts digits in Year Group
    df_melted["Gender"] = df_melted["Category"].apply(lambda x: "Female" if "females" in x else "Male")
    df_melted["Metric"] = df_melted["Category"].apply(lambda x: "Number_Vaccinated" if "vaccinated" in x.lower() else "Number")

    # Pivot to get the correct structure
    df = df_melted.pivot(index=["Local authority", "Year Group", "Gender"], columns="Metric", values="Value").reset_index()

    # Rename columns
    df.rename(columns={"Local authority": "Local_Authority", "Year Group": "Year_Group", "Metric": "Index"}, inplace=True)

    # Add 'Academic Year' and 'Academic Year End Date' column
    date = excel_data.iloc[0, 0].split()[-1]
    df['Academic_Year_End_Date'] = date
    full_date = excel_data.iloc[0,0].split(',')[-1].strip()
    df['Academic_Year_Text'] = full_date
    # Add Extract Date column
    df["Extract_Date"] = datetime.today()

    # Append processed data to the list
    dataframes.append(df)

# Combine all years' data into one DataFrame
combined_df = pd.concat(dataframes, ignore_index=True)

# Final Cleaning (2019-2020)
combined_df = combined_df.dropna(subset=["Number", "Number_Vaccinated"])
combined_df.replace("*", None, inplace=True)
combined_df.replace("[E]", None, inplace=True)
combined_df.replace("[DS]", None, inplace=True)


## Add 'Both' Gender Category
both_df = combined_df.copy()
both_df["Gender"] = "Both"

# Group by the relevant columns while summing numeric fields
both_df = both_df.groupby(
    ["Local_Authority", "Year_Group", "Gender", "Academic_Year_End_Date", "Academic_Year_Text", "Extract_Date"],
    as_index=False
).agg({
    "Number": "sum",
    "Number_Vaccinated": "sum"
})

# Combine original dataset with 'Both' gender records
second_df = pd.concat([combined_df, both_df], ignore_index=True)


## Add 'All' Year Category
year_df = second_df.copy()
year_df["Year_Group"] = "All"

# Group by the relevant columns while summing numeric fields
year_df = year_df.groupby(
    ["Local_Authority", "Year_Group", "Gender", "Academic_Year_End_Date", "Academic_Year_Text", "Extract_Date"],
    as_index=False
).agg({
    "Number": "sum",
    "Number_Vaccinated": "sum"
})



# Combine 'All Years' dataset with 'Both' gender records
final_df = pd.concat([second_df, year_df], ignore_index=True)
final_df.columns = [c.upper() for c in final_df.columns]

#Establish Snowflake connection
load_dotenv(override = True)

database= os.getenv("DATABASE")
schema= os.getenv("SCHEMA")
table_prov = os.getenv("DESTINATION_TABLE") 

ctx = connect(
    account= os.getenv("ACCOUNT"),
    user= os.getenv("USER"),
    authenticator= os.getenv("AUTHENTICATOR"),
    role= os.getenv("ROLE"),
    warehouse= os.getenv("WAREHOUSE"),
    database= database,
    schema= schema
)

destination_prov = f"{database}.{schema}.{table_prov}"

db.upload_hpv_data(ctx, final_df, destination_prov, replace = False)
