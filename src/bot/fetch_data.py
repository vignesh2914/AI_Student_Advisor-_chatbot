import pandas as pd
import os
from sqlalchemy import create_engine
from urllib.parse import quote
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# MySQL credentials from .env
MYSQL_USER = os.getenv("database_user_name")
MYSQL_PASSWORD = quote(os.getenv("database_user_password"))
MYSQL_HOST = os.getenv("database_host_name")
MYSQL_DATABASE = os.getenv("database_name")
MYSQL_PORT = "3306"

# SQLAlchemy engine
engine = create_engine(f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")

# Excel files and their corresponding MySQL table names
data_files = {
    "students": "STUDENTS_ENROLLEMENT.xlsx",
    "acad_prog": "ACAD_PROG.xlsx",
    "class_schedule": "CLASS_SCHEDULE_4764.xlsx",
    "term_history": "TERM_HISTORY.xlsx"
}

# Mapping of table_name -> primary_key
primary_keys = {
    "students": "emplid",
    "acad_prog": "acad_prog",          # change if needed
    "class_schedule": "class_nbr",     # example assumption
    "term_history": "emplid"           # assuming same key as students
}

def insert_dataframe_to_db(df: pd.DataFrame, table_name: str, primary_key: str = None):
    """
    Inserts a DataFrame into a MySQL table, skipping rows that already exist based on the primary key.
    """
    try:
        if primary_key and primary_key in df.columns:
            existing_keys_query = f"SELECT {primary_key} FROM {table_name}"
            existing_keys = pd.read_sql(existing_keys_query, con=engine)
            existing_keys_set = set(existing_keys[primary_key].astype(str).unique())

            df[primary_key] = df[primary_key].astype(str)
            df = df[~df[primary_key].isin(existing_keys_set)]

        if not df.empty:
            df.to_sql(table_name, con=engine, if_exists='append', index=False)
            logger.info(f"Inserted {len(df)} new rows into '{table_name}'")
        else:
            logger.info(f"No new rows to insert into '{table_name}'")
    except Exception as e:
        logger.error(f"Failed to insert into '{table_name}': {e}")

def fetch_and_insert():
    for table_name, filename in data_files.items():
        try:
            file_path = os.path.join("data", filename)

            if filename == "CLASS_SCHEDULE_4764.xlsx":
                df = pd.read_excel(file_path, skiprows=1)
            else:
                df = pd.read_excel(file_path)

            df.columns = df.columns.str.lower().str.strip()

            primary_key = primary_keys.get(table_name)
            insert_dataframe_to_db(df, table_name, primary_key)
        except Exception as e:
            logger.error(f"❌ Error processing '{filename}': {e}")

if __name__ == "__main__":
    fetch_and_insert()
