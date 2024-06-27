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
log_file = os.path.join("D:\\Item_replenishment_report_automation\\Logging_information", f"Runner_{fcurrent_time}")
logging.basicConfig(filename=log_file, level=logging.INFO)





def runner(suppliers, new_loop):

    current_time = datetime.now()
    start_timef = current_time.strftime("%Y-%m-%d-%H-%M-%S")
    start_time = datetime.strptime(start_timef, "%Y-%m-%d-%H-%M-%S")
    
    print("_________________________________________________________")
    print(" ")
    print(f"Process started - {fcurrent_time}")
        
    current_time = datetime.now()
    day = current_time.day
    month =  current_time.strftime("%b")
    year = current_time.year


    logging.info("The next process - process the data by sending in the suppliers file")

    # get the process_flag to download the data from the db as a csv file
    checkerob = processor.checker()
    process_flag = checkerob.main(suppliers, new_loop)

    logging.info(f"Processed the data for each suppliers - result {process_flag}")

    table_name = "replenishment_items"
    output_file = f"Replenishment_report_{day}_{month}_{year}.xlsx"
    output_folder = f"D:\\Replenishment_reports\\Replenishment_reports_{day}_{month}_{year}"



    if process_flag == 1:

        # get the connection variable
        connection = PGS_connector.connect_to_postgres()
        
        # download the data as a csv file
        csv_file = PGS_connector.load_data_csv(connection, table_name, output_file, output_folder)

        logging.info("Loaded data from the database to local csv files")

        # send in mails with the file as the attachment
        #result = mailer.send_email(csv_file)
        result = True

    else:
        raise ValueError("Failed loading data into PGS database")


    # runs only if the result is true- mails have been sent
    if result:
        print("Process finished - files saved!!")
        logging.info("Process finished - files saved!!")

    current_time = datetime.now()
    end_timef = current_time.strftime("%Y-%m-%d-%H-%M-%S")
    end_time = datetime.strptime(end_timef, "%Y-%m-%d-%H-%M-%S")

    # Calculate the difference between end_time and start_time
    time_difference = end_time - start_time

    # Calculate total elapsed minutes and remaining seconds
    total_minutes = int(time_difference.total_seconds() // 60)
    hours = total_minutes // 60
    minutes = total_minutes % 60
    seconds = time_difference.total_seconds()
    # Calculate remaining seconds
    remaining_seconds = round(seconds % 60, 0)

    print(f"Process ended - {fcurrent_time}")

    print("_____________________________________________________________")
    print("")
    print(f"Time taken : {hours} hrs : {minutes} mins : {remaining_seconds} sec")


if __name__ == "__main__":
    
    
    parser = argparse.ArgumentParser(description= "Replenishment checks")
    suppliers_file = parser.add_argument("--suppliers_file", help="Give the suppliers file", required=True)
    new_loop = parser.add_argument("--new_loop", help="Give the suppliers file", required=True)


    args = parser.parse_args()
    suppliers_file = args.suppliers_file
    new_loop = args.new_loop


        
    """# open the suppliers file that has supplier ids
    with open(suppliers_file, "r+") as sup_file:
        suppliers = json.load(sup_file)
"""
    runner(suppliers_file, new_loop)
        