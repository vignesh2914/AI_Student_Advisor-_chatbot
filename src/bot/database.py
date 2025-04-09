import os
import sys

import mysql.connector as conn
from dotenv import load_dotenv
from mysql.connector.connection import MySQLConnection
from mysql.connector.cursor import MySQLCursor

from src.bot.exception import CustomException
from src.bot.logger import logging


def configure():
    load_dotenv()


configure()
host = os.getenv("database_host_name")
user = os.getenv("database_user_name")
password = os.getenv("database_user_password")
database = os.getenv("database_name")


def create_database(host:str ,user:str, password:str) -> bool:
    """
    This function connects to a MySQL server using the provided credentials,
    and then attempts to create a database named 'student_database'. If the database
    already exists, no changes are made.

    Args:
        host (str): The hostname of the MySQL server.
        user (str): The username used to authenticate with the MySQL server.
        password (str): The password used to authenticate with the MySQL server.
    """
    try:
        mydb = conn.connect(host=host, user=user, password=password)
        cursor = mydb.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS student_database")
        logging.info("Database created successfully")
        return True
    except Exception as e:
        logging.error(f"An error occurred while creating database: {e}")
        raise CustomException(e, sys)


def connect_to_mysql_database(host:str, user:str, password:str, database:str) -> MySQLConnection:
    """
    Connects to MySQL database using provided credentials.
    If the connection is successful, it returns the MySQLConnection object.

    Args:
        host (str): The hostname or IP address of the MySQL server.
        user (str): The username used to authenticate with the MySQL server.
        password (str): The password used to authenticate with the MySQL server.
        database (str): The name of the database to connect to.

    Returns:
        MySQLConnection: A connection object to interact with the MySQL database.
    """
    try:
        mydb = conn.connect(host=host, user=user, password=password, database=database)
        logging.info("Connected to MySQL successfully!")
        return mydb
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise CustomException(e,sys)
    

def create_cursor_object(mydb: MySQLConnection) -> MySQLCursor:
    """
    Creates a cursor object from a MySQLConnection object.
    The cursor object allows for the execution of SQL queries

    Args:
        mydb (MySQLConnection): The MySQLConnection object to obtain the cursor from.

    Returns:
        MySQLCursor: A cursor object for interacting with the MySQL database.
    """
    try:
        cursor = mydb.cursor()
        logging.info("Cursor object obtained successfully!")
        return cursor
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise CustomException(e,sys)


def create_tables(host: str, user: str, password: str, database: str) -> bool:
    """
    Creates the necessary tables in the specified MySQL database if they do not already exist.
    """
    try:
        mydb = connect_to_mysql_database(host, user, password, database)
        cursor = create_cursor_object(mydb)

        table_queries = [

            # Table 1: students
            """
            CREATE TABLE IF NOT EXISTS students (
                emplid VARCHAR(20),
                name_display VARCHAR(100),
                acad_career VARCHAR(50),
                institution VARCHAR(50),
                strm VARCHAR(10),
                class_nbr VARCHAR(20),
                unt_taken FLOAT,
                acad_prog VARCHAR(50),
                descr VARCHAR(100),
                crse_id VARCHAR(20),
                crse_grade_off VARCHAR(5),
                course_title_long VARCHAR(255),
                cum_gpa FLOAT,
                subject VARCHAR(50),
                catalog_nbr VARCHAR(20),
                acad_org VARCHAR(50),
                PRIMARY KEY (emplid, strm, class_nbr)
            )
            """,
            # Table 2: academic_programs
            """
            CREATE TABLE IF NOT EXISTS academic_programs (
                emplid VARCHAR(20),
                acad_career VARCHAR(50),
                acad_prog VARCHAR(50),
                prog_status VARCHAR(20),
                prog_action VARCHAR(20),
                admit_term VARCHAR(10),
                campus VARCHAR(50)
            )
            """,

            # Table 3: class_schedule
            """
            CREATE TABLE IF NOT EXISTS class_schedule (
                course_id VARCHAR(20),
                term VARCHAR(10),
                offer_nbr VARCHAR(10),
                acad_group VARCHAR(50),
                subject VARCHAR(50),
                catalog VARCHAR(20),
                descr VARCHAR(100),
                class_nbr VARCHAR(20),
                cap_enrl INT,
                tot_enrl INT,
                acad_org VARCHAR(50),
                campus VARCHAR(50),
                section VARCHAR(10),
                id VARCHAR(20),
                role VARCHAR(50),
                facil_id VARCHAR(50),
                mtg_start VARCHAR(20),
                mtg_end VARCHAR(20),
                mon VARCHAR(20),
                tue VARCHAR(20),
                wed VARCHAR(20),
                thurs VARCHAR(20),
                fri VARCHAR(20),
                sat VARCHAR(20),
                sun VARCHAR(20),
                display_name VARCHAR(100)
            )
            """,

            # Table 4: term_history
            """
            CREATE TABLE IF NOT EXISTS term_history (
                emplid VARCHAR(20),
                tot_taken_prgrss FLOAT,
                tot_passd_prgrss FLOAT,
                acad_career VARCHAR(50),
                institution VARCHAR(50),
                acad_prog_primary VARCHAR(50),
                strm VARCHAR(10),
                term_gpa FLOAT
            )
            """
        ]

        for query in table_queries:
            cursor.execute(query)

        logging.info("All tables created successfully.")
        return True

    except Exception as e:
        logging.error(f"An error occurred while creating tables: {e}")
        raise CustomException(e, sys)


if __name__ == "__main__":
    create_database(host, user, password)
    create_tables(host, user, password, database)
