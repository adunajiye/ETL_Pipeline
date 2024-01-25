import csv
import os
import sys
from datetime import datetime
from turtle import update
import pandas as pd
from sqlalchemy import text

# from common.base import session
# from common
import common
# from common.tables import PprRawAll
# from scripts.extracts import raw_path

# Settings
sys.path.append('scripts\\common')
base_path = os.path.abspath(__file__ + "/../../")
ref_month = datetime.today().strftime("%Y-%m-%d")
print(ref_month)


base_path = "C:/Users/user/Desktop/Raw_Csv_Data_Pipeline/Data/Raw"


# Raw path where we want to extract the new .csv data
raw_path = f"{base_path}/ppr-all.csv"
# START - Paths for new data available

# Raw path where we want to extract the new .csv data
# raw_path = f"{base_path}/data/raw/downloaded_at={ref_month}/ppr-all.csv"


# END - Paths for new data available


def transform_case(input_string):
    """
    Lowercase string fields
    """
    return input_string.lower()

def update_date_of_sale(date_input):
    """
    Update date format from DD/MM/YYYY to YYYY-MM-DD
    """
    # date_input = "23/01/2024"
    global new_format
    try:
        current_format = datetime.strptime(date_input, "%d/%m/%Y")
        new_format = current_format.strftime("%Y-%m-%d")
        print(new_format)
    except Exception as e:
        print(f"Error formatting date: '{e}'")
    return new_format
# update_date_of_sale(date_input="")


def update_price(price_input):
    """
    Return price as integer by removing:
    - "€" symbol
    - "," to convert the number into float first (e.g. from "€100,000.00" to "100000.00")
    """

    price_input = "€100,000.00"
    symbol_input = price_input.replace("€", "")
    print(symbol_input)
    price_input = float(symbol_input.replace(",", ""))
    return int(price_input)
# update_price(price_input=)


def truncate_table():
    """
    Ensure that "ppr_raw_all" table is always in empty state before running any transformations.
    And primary key (id) restarts from 1.
    """
    # session.execute(
    #     text("TRUNCATE TABLE ppr_raw_all;")
    # )
    # session.commit()


def transform_new_data():
    """
    Apply all transformations for each row in the .csv file before saving it into database
    """
    reader= pd.read_csv(raw_path,encoding='latin1')
    # print(reader)
    print(raw_path)
    # Initialize an empty list for our PprRawAll objects
    ppr_raw_objects = []
    """
    Transform Date of sales

    """
    new_date_of_sale = pd.to_datetime(reader['date_of_sale'],format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
    # print(new_date_of_sale)

    """
    Return price as integer by removing:
    - "€" symbol
    - "," to convert the number into float first (e.g. from "€100,000.00" to "100000.00")
    """

    updated_price = reader['price'].replace('[^\d.]', '', regex=True).astype(float).astype(int)
    print(updated_price)

    # for row in reader:
    #         # Apply transformations and save as PprRawAll object
    #         ppr_raw_objects.append(
    #             PprRawAll(
    #                 date_of_sale=update_date_of_sale(row["date_of_sale"]),
    #                 address=transform_case(row["address"]),
    #                 postal_code=transform_case(row["postal_code"]),
    #                 county=transform_case(row["county"]),
    #                 price=update_price(row["price"]),
    #                 description=update_description(row["description"]),
    #             )
    #         )
        # Bulk save all new processed objects and commit
        # session.bulk_save_objects(ppr_raw_objects)
        # session.commit()
transform_new_data()


def main():
    print("[Transform] Start")
    print("[Transform] Remove any old data from ppr_raw_all table")
    truncate_table()
    print("[Transform] Transform new data available in ppr_raw_all table")
    transform_new_data()
    print("[Transform] End")
