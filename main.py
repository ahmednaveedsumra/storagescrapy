import pandas as pd
import json  # Import json to use json.dumps
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


def load_parquet_to_db(parquet_file_path, db_connection_string, table_name):
    try:
        # Read the Parquet file into a DataFrame
        df = pd.read_parquet(parquet_file_path)
        print("Parquet file loaded successfully.")

        # Convert 'location' column to JSON strings if it exists
        if 'location' in df.columns:
            df['location'] = df['location'].apply(json.dumps)  # Convert dictionaries to JSON strings

        # Connect to the database
        engine = create_engine(db_connection_string)
        with engine.connect() as connection:
            # Insert the DataFrame into the specified table
            df.to_sql(table_name, con=engine, if_exists='replace', index=False, chunksize=5000)
            print(f"Data inserted successfully into {table_name} table.")

            # Optional: Add additional columns or indexes (if needed)
            alter_table_query = text(f"""
                ALTER TABLE {table_name}
                ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST,
                ADD COLUMN created DATETIME DEFAULT CURRENT_TIMESTAMP,
                ADD COLUMN updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                ADD INDEX index_created(created),
                ADD INDEX idx_updated(updated);
            """)
            connection.execute(alter_table_query)
            print("Table structure altered successfully.")

    except SQLAlchemyError as e:
        print(f"Database operation failed: {e}")
    except Exception as e:
        print(f"Failed to load Parquet file or insert data: {e}")


if __name__ == '__main__':
    # Define file path, connection string, and target table name
    parquet_file_path = './storage/output.parquet'  # Path to your Parquet file
    db_connection_string = "mysql+mysqlconnector://root:ahmad09102@localhost/exceldata"  # Update with your DB credentials
    table_name = 'parquet_data'

    # Call the function to load data into the database
    load_parquet_to_db(parquet_file_path, db_connection_string, table_name)
