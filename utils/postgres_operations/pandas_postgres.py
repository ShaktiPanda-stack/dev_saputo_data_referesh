import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os

def get_postgres_engine():
    """
    Creates a SQLAlchemy engine for PostgreSQL.
    
    :return: SQLAlchemy engine.
    """
    postgres_url = os.environ['postgres_url']
    postgres_user = os.environ['postgres_user']
    postgres_password = os.environ['postgres_password']

    engine = create_engine(f"postgresql://{postgres_user}:{postgres_password}@{postgres_url}")
    return engine

def read_from_postgres_pandas(table_name):
    """
    Reads data from a PostgreSQL table into a Pandas DataFrame.

    :param table_name: The table name (schema.table) to read from.
    :return: Pandas DataFrame containing the table data.
    """
    engine = get_postgres_engine()
    query = f"SELECT * FROM {table_name}"
    
    df = pd.read_sql(query, engine)
    return df


def write_to_postgres_pandas(df, table_name, mode="replace"):
    """
    Writes a Pandas DataFrame to a PostgreSQL table.

    :param df: Pandas DataFrame to be written.
    :param table_name: The target PostgreSQL table (schema.table).
    :param mode: Write mode - "replace" (overwrite), "append", or "fail".
    """
    engine = get_postgres_engine()
    
    df.to_sql(name=table_name, con=engine, if_exists=mode, index=False, chunksize=10000)