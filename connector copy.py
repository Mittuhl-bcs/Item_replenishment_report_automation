import pyodbc
import pandas as pd
import warnings
import dask.dataframe as dd
import csv


# Suppress warnings
warnings.filterwarnings("ignore", category=UserWarning)



def connect_db(query):

    server = "10.240.1.129"
    database = "asp_BUILDCONT"
    username = "buildcont_reports"
    password = "ASP4664bu"


    # connect with credentials
    connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    connection = pyodbc.connect(connection_string)

    # delete it while using automation
    # cursor object
    cursor = connection.cursor()

    """
    query = f"select top 10 * from bcs_view_master_data_sales_by_class

    order by class_id1, class_id2, class_id3{supplier_id}

    """

    # delete it while using automation
    # execute the query
    cursor.execute(query)

    # unblock it while using automation
    # read data into DataFrame
    #df = pd.read_sql_query(query, connection)

    """ This is not needed for the automation scope - use it when executing individual script

    # specify the file path to save the CSV file
    csv_file_path = "data.csv"

    # open the CSV file in write mode using the csv.writer
    with open(csv_file_path, "w", newline='') as csv_file:
        # create a csv writer object
        csv_writer = csv.writer(csv_file)

        # write the column headers to the CSV file
        csv_writer.writerow([column[0] for column in cursor.description])

        # iterate over the rows fetched from the cursor and write them to the CSV file
        for row in cursor:
            csv_writer.writerow(row)
    """


    # closing after the process end
    # delete it while using automation
    cursor.close()

    connection.close() 

    
    # return df
