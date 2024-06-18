import processor
import BCS_SSMS_connector
import mailer
import os
import sys
import argparse
import pandas as pd
import numpy as np
import json
import PGS_connector
from datetime import datetime 
import logging


current_time = datetime.now()
fcurrent_time = current_time.strftime("%Y-%m-%d-%H-%M-%S")
log_file = os.path.join("D:\\Price_mapping_Automation\\Logging_information", f"Pricing_automation_runner_{fcurrent_time}")
logging.basicConfig(filename=log_file, level=logging.DEBUG)





def runner(suppliers):


        
    current_time = datetime.now()
    day = current_time.day
    month =  current_time.strftime("%b")
    year = current_time.year


    logging.info("The next process - process the data by sending in the suppliers file")

    # get the process_flag to download the data from the db as a csv file
    checkerob = processor.checker()
    process_flag, connection = checkerob.main(suppliers)

    logging.info(f"Processed the data for each suppliers - result {process_flag}")

    table_name = ""
    output_file = f"D:\\Replenishment_reports\\Replenishment_report_{day}_{month}_{year}.csv"

    if process_flag == 1:

        # download the data as a csv file
        csv_file = PGS_connector.load_data_csv(connection, table_name, output_file)

        logging.info("Loaded data from the database to local csv files")

        # send in mails with the file as the attachment
        #result = mailer.send_email(csv_file)
        result = True


    # runs only if the result is true- mails have been sent
    if result:
        print("Process finished - files saved!!")


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description= "Replenishment checks")
    suppliers_file = parser.add_argument("--suppliers_file", help="Give the suppliers file")

    args = parser.parse_args()
    suppliers_file = args.suppliers_file

    
    """# open the suppliers file that has supplier ids
    with open(suppliers_file, "r+") as sup_file:
        suppliers = json.load(sup_file)
"""
    runner(suppliers_file)
        