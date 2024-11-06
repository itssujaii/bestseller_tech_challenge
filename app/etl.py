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
    try:
        with engine.connect() as connection:
            connection.execute(text(create_table_sql))
            logger.info("Customer_Sale_Analytics table created successfully.")
            
            # Confirm table creation by listing tables
            tables = connection.execute(text("SELECT name FROM sqlite_master WHERE type='table';")).fetchall()
            logger.debug(f"Tables present in the database: {tables}")
            
            if ('Customer_Sale_Analytics',) not in tables:
                logger.error("Table Customer_Sale_Analytics was not created as expected.")
    except Exception as e:
        logger.error(f"Error creating Customer_Sale_Analytics table: {e}")
        raise

def extract_data(file_path):
    """Extract data from a CSV file."""
    try:
        df = pd.read_csv(file_path)
        logger.info("Data extracted successfully from %s", file_path)
        return df
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Error during extraction from {file_path}: {e}")
        raise

def transform_data(df):
    """Transform data by adding a UnitPrice column."""
    try:
        df['UnitPrice'] = df['UnitPrice'] * df['Quantity']  
        logger.info("Data transformed successfully with UnitPrice column.")
        return df
    except Exception as e:
        logger.error(f"Error during transformation: {e}")
        raise

def load_data(engine, df):
    """Load data into the Customer_Sale_Analytics table in the database."""
    try:
        # Remove duplicate rows from the DataFrame
        df_cleaned = df.drop_duplicates()    
        
        # Load the cleaned data into the SQL table
        df_cleaned.to_sql('Customer_Sale_Analytics', engine, if_exists='replace', index=False)
        logger.info("Data loaded successfully into the 'Customer_Sale_Analytics' table.")
    
    except Exception as e:
        logger.error(f"Error during loading into 'Customer_Sale_Analytics' table: {e}")
        raise

def main():
    """Main function to orchestrate the ETL process."""
    db_file = '/app/ecommerce.db'  # Explicit path for SQLite file

    # Remove existing database file if it exists
    if os.path.exists(db_file):
        try:
            os.remove(db_file)
            logger.info(f"Existing database file '{db_file}' removed.")
        except OSError as e:
            logger.warning(f"Could not remove '{db_file}': {e}")

    # Create a new database connection and table
    engine = create_connection(db_file)
    create_table(engine)

    # Confirm table creation
    with engine.connect() as connection:
        tables = connection.execute(text("SELECT name FROM sqlite_master WHERE type='table';")).fetchall()
        logger.info(f"Tables in database: {tables}")  # Check if 'Customer_Sale_Analytics' is listed here

    # Change to the correct path for your data directory
    data_file_path = '/app/data/bestseller.csv'  # Update if your data directory is different
    df = extract_data(data_file_path)
    
    # Validate DataFrame
    if df.isnull().values.any():
        logger.warning("Data contains null values. Please check the input data.")

    df = transform_data(df)
    load_data(engine, df)

    logger.info("ETL process completed successfully.")

if __name__ == "__main__":
    main()
