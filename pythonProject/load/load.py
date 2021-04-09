from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import psycopg2
import datetime


def connect(params_conn):
    """ Create a connector to the PostgreSQL database server

    params_conn: dictionary of DB host, database, user, password and port
    """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('LOG: Connecting to the PostgreSQL database... ', datetime.datetime.now())
        conn = psycopg2.connect(**params_conn)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        quit()
    print("LOG: Connection successful... ", datetime.datetime.now())
    return conn

def is_table_exist(conn):
    """
    Check if the destination table exists
    :param conn: DB Connector
    :return: Boolean
    """
    command = ("""SELECT EXISTS 
                    (
                        SELECT 1 
                        FROM pg_tables
                        WHERE schemaname = 'public'
                        AND tablename = 'monthly_restaurants_report'
                    );
    """)
    cur = conn.cursor()
    try:
        cur.execute(command)
        result = cur.fetchall()
        result[0][0]
        # close communication with the PostgreSQL database server
        cur.close()
        return result[0][0]
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)

def create_table(conn):
    """
    Create the destination table in the DB

    conn: DB connector
    """
    command = ("""CREATE TABLE monthly_restaurants_report (
                    "restaurant_id" VARCHAR(255) NOT NULL,
                    "restaurant_name" VARCHAR(255) NOT NULL,
                    "country" VARCHAR(127) NOT NULL,
                    "month" VARCHAR(127) NOT NULL,
                    "number_of_bookings" INTEGER,
                    "number_of_guests" INTEGER,
                    "amount" VARCHAR(255) NOT NULL
    )
    """)
    cur = conn.cursor()
    try:
        # create table
        cur.execute(command)
        print('LOG: Creation of monthly_restaurants_report successfully... ', datetime.datetime.now())
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def is_table_empty(conn):
    """
    Check if the destination table is empty
    :param conn:
    :return: Boolean
    """
    command = ("""SELECT count(*) FROM public.monthly_restaurants_report """)
    cursor = conn.cursor()
    try:
        cursor.execute(command)
        result = cursor.fetchall()
        cursor.close()
        return result[0][0] == 0
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()


def truncate_table(conn):
    """
    Empty the destination table to avoid to have duplicate output data
    :param conn: DB connector
    """
    command = ("""TRUNCATE TABLE public.monthly_restaurants_report """)
    cursor = conn.cursor()
    try:
        cursor.execute(command)
        print("LOG: Truncate monthly_restaurants_report... ", datetime.datetime.now())
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()


def copy_from_file(conn, filepath, table):
    """
    Load the csv file from filepath and use copy_from() to copy it to the table

    conn: DB connector
    filepath: the output file path
    table: destination table

    """
    f = open(filepath, 'r')
    cursor = conn.cursor()
    try:
        # copy data from the csv file to the table
        cursor.copy_from(f, table, sep=";")
        # commit the changes
        conn.commit()
        print("LOG: "+filepath + " has been copied sucessfully in " + table,"... ",datetime.datetime.now())
    except (Exception, psycopg2.DatabaseError) as error:
        print("ERROR: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()
