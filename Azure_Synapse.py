import pandas as pd
from sqlalchemy import create_engine
import urllib
import json

config_file_path = "config.json"
with open(config_file_path) as config_file:
    config = json.load(config_file)
# server = config['SSMS']['server']
# database = config['SSMS']['database']
# username = config['SSMS']['username']
# password = config['SSMS']['password']
server = config['SSMS']['server']
database = config['SSMS']['database']
username = config['SSMS']['username']
password = config['SSMS']['password']
def log_message(message):
    print(f"{message}")
log_message("Creating connection string")
connection_string = (
    f"mssql+pyodbc://{username}:{urllib.parse.quote_plus(password)}@{server}:1433/{database}?"
    "driver=ODBC+Driver+17+for+SQL+Server&"
    "Authentication=ActiveDirectoryPassword"
)
log_message("Creating SQLAlchemy engine")
engine = create_engine(connection_string)
def query_to_dataframe(query, engine):
    log_message(f"Executing query: {query}")
    with engine.connect() as connection:
        log_message("Connection established Successfully, running query")
        df = pd.read_sql(query, connection)
        log_message("Query executed Successfully")
        return df
query = "SELECT TOP 10 * FROM ref.d_rateplan_detail WHERE load_date='2024-05-22'"
try:
    log_message("Starting data fetch operation")
    df = query_to_dataframe(query, engine)
    log_message("Data fetched successfully")
    print(df)
except Exception as e:
    log_message(f"Error executing query: {e}")
