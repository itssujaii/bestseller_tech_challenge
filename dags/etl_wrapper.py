import pandas as pd
import os
from logging_config import logger  
from sqlalchemy import text, create_engine

def create_connection(db_file):
    """Create a database connection and return the engine."""
    engine = create_engine(f'sqlite:///{db_file}')
    logger.info(f"Database engine created for file: {db_file}")
    return engine

def create_table(engine):
    """Create the database table if it doesn't exist."""
    create_table_sql = """ CREATE TABLE IF NOT EXISTS Customer_Sale_Analytics(
        InvoiceNo INTEGER NOT NULL,
        StockCode TEXT NOT NULL,
        description TEXT,
        quantity INTEGER,
        InvoiceDate DATE,
        UnitPrice REAL,
        CustomerID INTEGER NOT NULL,
        country TEXT NOT NULL,
        PRIMARY KEY (InvoiceNo, StockCode, CustomerID, InvoiceDate)
    ); """
    with engine.connect() as connection:
        connection.execute(text(create_table_sql))
        logger.info("Customer_Sale_Analytics table created successfully.")

def extract_data(file_path):
    """Extract data from a CSV file."""
    df = pd.read_csv(file_path)
    logger.info("Data extracted successfully from %s", file_path)
    return df

def transform_data(df):
    """Transform data by adding a UnitPrice column."""
    df['UnitPrice'] = df['UnitPrice'] * df['Quantity']  
    logger.info("Data transformed successfully with UnitPrice column.")
    return df

def load_data(engine, df):
    """Load data into the Customer_Sale_Analytics table in the database."""
    df_cleaned = df.drop_duplicates()    
    df_cleaned.to_sql('Customer_Sale_Analytics', engine, if_exists='replace', index=False)
    logger.info("Data loaded successfully into the 'Customer_Sale_Analytics' table.")

def run_etl(db_file, csv_file):
    """Run the ETL process."""
    engine = create_connection(db_file)
    create_table(engine)
    df = extract_data(csv_file)
    df = transform_data(df)
    load_data(engine, df)
    logger.info("ETL process completed successfully.")
