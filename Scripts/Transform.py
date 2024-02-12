import csv
import os
import sys
from datetime import datetime
from turtle import update
from marshmallow_sqlalchemy import column2field
import pandas as pd
from sqlalchemy import text
import psycopg2
from sqlalchemy import create_engine

# from common.base import session
# from common
import common
from tables import Column
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
    try:
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(
                    host="qfa-dlp-pre-prod-postgresql-fra-do-user-13185867-0.b.db.ondigitalocean.com",
                    database="qfa_db",
                    user="doadmin",
                    password="AVNS_GeCGRs_qkQt-JHiY12N",
                    port=25060)

        cur = conn.cursor()   
        
        """
        Apply all transformations for each row in the .csv file before saving it into database
        """
        reader= pd.read_csv(raw_path,encoding='latin1')
        # print(reader.head(3))
        # print(raw_path)
        # Initialize an empty list for our PprRawAll objects
        ppr_raw_objects = []
        """
        Transform Date of sales

        """
        new_date_of_sale = pd.to_datetime(reader['date_of_sale'],format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
        # print(new_date_of_sale.head(5))

        """
        Return price as integer by removing:
        - "€" symbol
        - "," to convert the number into float first (e.g. from "€100,000.00" to "100000.00")
        """

        updated_price = reader['price'].replace('[^\d.]', '', regex=True).astype(float).astype(int)
        # print(updated_price.head(5))

        
        """
        Concat Transformed Series to dataframe and delete pre existing columns
        """

        reader.drop('date_of_sale', axis=1, inplace=True)
        print(reader.drop('price',axis =1,inplace=True))
        # print(reader)

        new_added_sales_date =pd.concat([reader,new_date_of_sale],axis=1)
        # print(new_added_sales_date)

        price_added = pd.concat([new_added_sales_date,updated_price],axis=1)
        #

        # Rename Headers
        price_added.rename(columns={'date_of_sale': 'new_updated_sale_date'}, inplace=True)
        price_added.rename(columns={'price': 'new_price'}, inplace=True)
        
        # print(price_added.head(3))

         # Create a SQLAlchemy engine for easier DataFrame to PostgreSQL loading
        engine = create_engine('postgresql://doadmin:AVNS_GeCGRs_qkQt-JHiY12N@qfa-dlp-pre-prod-postgresql-fra-do-user-13185867-0.b.db.ondigitalocean.com:25060/qfa_db')
        # print(engine)


        table_name = 'ETL_Table'
        column_mapping = {
            'id':'Id',
            'address': 'Address',
            'postal_code': 'Postal_Code',
            'county': 'Country',
            'description': 'Description',
            'new_updated_sale_date': 'Date',
            'new_price': 'Price'
        }
        print(column_mapping)
        price_added = price_added.rename(columns=column_mapping)
        print(price_added.head(1))

        if f"SQL Query:{price_added.to_sql(table_name,engine, if_exists='append',index = False,method='multi',chunksize=5)}":
            print(f"Data loaded successfully:{price_added.head(4)}")
        # else:
        #     print(f"Table Does not exist...")
            
        # #     print(f"DataFrame after insertion:")
        #     response = pd.read_sql(f"SELECT * FROM {table_name}", engine)
        #     # print(response)
        #     if response == None:
        #         print(f"Dataframe not found in database....")
        #     else:
        #         print(f"Dataframe Found in Database...")
            # Commit changes to the database
            # conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        print('Database connection closed.')
transform_new_data()


def main():
    print("[Transform] Start")
    print("[Transform] Remove any old data from ppr_raw_all table")
    truncate_table()
    print("[Transform] Transform new data available in ppr_raw_all table")
    transform_new_data()
    print("[Transform] End")
# main()


