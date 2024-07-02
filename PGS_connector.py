import pyodbc
import pandas as pd
import warnings
import dask.dataframe as dd
import csv
import psycopg2
import logging
from datetime import datetime
import os


current_time = datetime.now()
fcurrent_time = current_time.strftime("%Y-%m-%d-%H-%M-%S")
log_file = os.path.join("D:\\Item_replenishment_report_automation\\Logging_information", f"PGS_connector_{fcurrent_time}.log")
logging.basicConfig(filename=log_file, level=logging.DEBUG)


# Connect to your PostgreSQL database
def connect_to_postgres():

    dbname = 'BCS_items'
    user = 'postgres'
    password = 'post@BCS'
    host = 'localhost' 
    port = '5432'  # Default PostgreSQL port is 5432

    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        logging.info("Connected to the PostgreSQL database successfully!")
        return conn
    
    except psycopg2.Error as e:
        logging.error("Unable to connect to the database")
        logging.error(e)

        raise ValueError(e)   
        return None



def insert_data_into_db(df, conn, new_loop):


    if new_loop == "yes" :
        
        # mapping the curson through connection
        cursor = conn.cursor()
    
        sql = """
        delete from replenishment_items
        """

        print("Executed the new loop command")

        # Execute the INSERT statement with the values from the current row
        cursor.execute(sql)

        # close the cursor
        cursor.close()


    
    else :

        
        # mapping the curson through connection
        cursor = conn.cursor()


        logging.info("Connected to PGS database, now will insert the data.")


        # insert into table command in here
        df = df[df["discrepancy_type"] != "All right"]

        
        # declare variables here
        for index, row in df.iterrows():
            location_id = row['location_id']
            location_name = row['location_name']
            inv_mast_uid = row['inv_mast_uid']
            item_id = row['item_id']
            item_desc = row['item_desc']
            on_vendor_price_book = row['on_vendor_price_book']
            product_type = row['product_type']
            primary_supplier_id = row['primary_supplier_id']
            supplier_name = row['supplier_name']
            replenishment_method = row['replenishment_method']
            replenishment_location = row['replenishment_location']
            inv_min = row['inv_min']
            inv_max = row['inv_max']
            stockable = row['stockable']
            sellable = row['sellable']
            buyable = row['buy']
            qty_on_hand = row['qty_on_hand']
            track_bins = row['track_bins']
            primary_bin = row['primary_bin']
            repl_loc_review = row['repl_loc_review']
            repl_meth_review = row['repl_meth_review']
            track_bin_review = row['track_bin_review']
            prefix = row["Prefix_of_company"]
            discrepancy_type = row["discrepancy_type"]
            
            #print(f"This is the row data: {buyable}")

            # Assuming you have a table named "replenishment" with corresponding columns
            # Construct the SQL INSERT statement

            try:
                sql = """
                INSERT INTO replenishment_items (location_id, location_name, inv_mast_uid, item_id, item_desc,
                                            on_vendor_price_book, product_type, primary_supplier_id, supplier_name,
                                            replenishment_method, replenishment_location, inv_min, inv_max,
                                            stockable, sellable, buyable, qty_on_hand, track_bins, primary_bin,
                                            repl_loc_review, repl_meth_review, track_bin_review, prefix, discrepancy_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                # Execute the INSERT statement with the values from the current row
                cursor.execute(sql, (location_id, location_name, inv_mast_uid, item_id, item_desc,
                                    on_vendor_price_book, product_type, primary_supplier_id, supplier_name,
                                    replenishment_method, replenishment_location, inv_min, inv_max,
                                    stockable, sellable, buyable, qty_on_hand, track_bins, primary_bin,
                                    repl_loc_review, repl_meth_review, track_bin_review, prefix, discrepancy_type))

                
                # Commit the transaction to persist the changes
                conn.commit()

            except psycopg2.Error as e:
                conn.rollback()  # Rollback any changes if an error occurs
                print(f"Error inserting data into database: {e}")

                raise ValueError(e)

            #finally:
                #cursor.close()

        
    # returns true only if successful
    return True



def load_data_csv(connection, table_name, output_file, output_folder):

    cursor = connection.cursor()

    try:

        query = f"SELECT * FROM {table_name}"
        tot_df = pd.read_sql(query, connection)

        loc_ids = tot_df["location_id"].unique().tolist()

        # print(f"{loc_ids}")

        # Check if the directory exists
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
            print(f"Created directory: {output_folder}")


        for loc in loc_ids:
            df = tot_df[tot_df["location_id"] == loc]

            repl_loc_review = df[df["repl_loc_review"] == "Y"]
            repl_meth_review = df[df["repl_meth_review"] == "Y"]
            track_bin_review = df[df["track_bin_review"] == "Y"]
            
            # Convert location_id to string without decimals
            #loc_str = str(int(float(loc)))
            loc_of = f"{loc}_{output_file}"
            
            

            loc_output_file = os.path.join(output_folder, loc_of)

            with pd.ExcelWriter(loc_output_file) as writer:
                df.to_excel(writer, sheet_name = f"{loc}_All_data", index = False)
                repl_loc_review.to_excel(writer, sheet_name=f"{loc}_repl_loc_review", index=False)
                repl_meth_review.to_excel(writer, sheet_name=f"{loc}_repl_meth_review", index=False)
                track_bin_review.to_excel(writer, sheet_name=f"{loc}_track_bin_review", index=False)
                


        # df.to_excel(output_file, index=False)
        logging.info(f"Data from table '{table_name}' successfully exported to '{output_file}'")
        
        

        """
        
        with open(output_file, 'w') as f:
            cursor.copy_expert(f"COPY {table_name} TO STDOUT WITH CSV HEADER", f)
        logging.info(f"Data from table '{table_name}' successfully exported to '{output_file}'")
        """

    except psycopg2.Error as e:
        logging.error(f"Error exporting data from table '{table_name}' to CSV file")
        logging.error(e)

        raise ValueError

    finally:

        if cursor:
            cursor.close()
        
        if connection:
            connection.close()

        logging.info("Database connection and curson closed")
    # if needed
    # connection.close()

    return output_folder