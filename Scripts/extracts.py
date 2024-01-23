import os
import csv
import tempfile
from datetime import datetime
from zipfile import ZipFile
import pandas as pd

import requests

# Settings
base_path = os.path.abspath(__file__ + "C:/Users/user/Desktop/Raw_Csv_Data_Pipeline/Data/Raw")
ref_month = datetime.today().strftime("%Y-%m-%d")

# START - Paths for new data available

# External website file url
source_url = "https://assets.datacamp.com/production/repositories/5899/datasets/66691278303f789ca4acd3c6406baa5fc6adaf28/PPR-ALL.zip"

base_path = "C:/Users/user/Desktop/Raw_Csv_Data_Pipeline/Data/Raw"


# Raw path where we want to extract the new .csv data
raw_path = f"{base_path}/ppr-all.csv"




def create_folder_if_not_exists(path):
    path = "C:/Users/user/Desktop/Raw_Csv_Data_Pipeline/Data/Raw/"
    """
    Create a new folder if it doesn't exists
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
# create_folder_if_not_exists()



def download_snapshot():
    """
    Download the new dataset from the source
    """
    create_folder_if_not_exists(raw_path)
    with open(raw_path, "wb") as source_ppr:
        # df = pd.read_csv(raw_path)
        # print(df)
        response = requests.get(source_url, verify=False)
        print(response.text)
        source_ppr.write(response.content)
# download_snapshot()

def save_new_raw_data():
    """
    Save new raw data from the source
    """

    create_folder_if_not_exists(raw_path)
    with tempfile.TemporaryDirectory() as dirpath:
        with ZipFile(
            raw_path,
            "r",
        ) as zipfile:
            names_list = zipfile.namelist()
            csv_file_path = zipfile.extract(names_list[0], path=dirpath)
            # Open the CSV file in read mode
            with open(csv_file_path, mode="r", encoding="windows-1252") as csv_file:
                reader = csv.DictReader(csv_file)

                row = next(reader)  # Get first row from reader
                print("[Extract] First row example:", row)

                # Open the CSV file in write mode
                with open(
                    raw_path,
                    mode="w",
                    encoding="windows-1252"
                ) as csv_file:
                    # Rename field names so they're ready for the next step
                    fieldnames = {
                        "Date of Sale (dd/mm/yyyy)": "date_of_sale",
                        "Address": "address",
                        "Postal Code": "postal_code",
                        "County": "county",
                        "Price (€)": "price",
                        "Description of Property": "description",
                    }
                    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                    # Write headers as first line
                    writer.writerow(fieldnames)
                    for row in reader:
                        # Write all rows in file
                        writer.writerow(row)

# Main function called inside the execute.py script
def main():
    print("[Extract] Start")
    print("[Extract] Downloading snapshot")
    download_snapshot()
    print(f"[Extract] Saving data from '{base_path}' to '{raw_path}'")
    save_new_raw_data()
    print(f"[Extract] End")
main()

