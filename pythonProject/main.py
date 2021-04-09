import pandas as pd
from test.tests_extract import *
from test.tests_transform import *
import datetime
import os.path
from transform.transform_data import *
from load.load import create_table, connect,copy_from_file, is_table_exist, is_table_empty, truncate_table

def main():
    """
    Main function called to execute the ETL pipeline:
    Extract the data
    Transform the data
    Save the output data
    Load the output data in the postgresql DB
    """
    try:
        f = open(os.environ.get('FILEPATH'))
        print("LOG: File open sucessfully... ", datetime.datetime.now())
        bookings = pd.read_csv(os.environ.get('FILEPATH'))

        #Create a test runner
        runner = unittest.TextTestRunner()

        # test number of columns
        # test colnames
        runner.run(suite_extract())

        # test data quality : if there's only one currency per country
        # test data quality : if there's only one currency per restaurant
        runner.run(suite_transform())

        # run the transformation function
        output_data = transform_data(bookings)

        # Save the dataframe to disk
        output_data.to_csv(os.environ.get('OUTPUTPATH'), index=False, header=False, sep=";")
        print("LOG: Output File write sucessfully... ", datetime.datetime.now())

        # create a dictionary of connection parameters
        params_dic = {
            "host": "db",
            "database": os.environ.get('POSTGRESQL_DATABASE'),
            "user": os.environ.get('POSTGRESQL_USERNAME'),
            "password": os.environ.get('POSTGRESQL_PASSWORD'),
            "port": os.environ.get('PORT')
        }

        #create connection to the db
        conn = connect(params_dic)

        if is_table_exist(conn) == False:
            print("run creation")
            create_table(conn)

        if is_table_empty(conn) == False :
            truncate_table(conn)

        #copy the outputfile to the newly created table
        copy_from_file(conn, os.environ.get('OUTPUTPATH'), "monthly_restaurants_report")
        if conn is not None:
            conn.close()
            print("LOG: Connection closed... ", datetime.datetime.now())

    except IOError:
        print("ERROR: FILE NOT ACCESSIBLE")
    finally:
        f.close()


if __name__ == '__main__':
    main()
