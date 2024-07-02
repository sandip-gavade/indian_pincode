# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


import pandas as pd
import mysql.connector
from mysql.connector import errorcode

# MySQL connection configuration
config = {
    'user': 'root',
    'password': 'mysql123',
    'host': 'localhost',
    'database': 'wholemart'
}

# File path to the large CSV file
file_path = 'pincode.csv'

# Create a connection to the MySQL database
try:
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    # Create the pincode_data table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS pincode_data (
        Pincode INT,
        District VARCHAR(255),
        StateName VARCHAR(255)
    )
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Define chunk size
    chunk_size = 1000  # Adjust this size based on your memory and performance requirements

    # Read and process the CSV file in chunks
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        # Filter the required columns
        df_filtered = chunk[['Pincode', 'District', 'StateName']]

        # Insert data into the pincode_data table
        insert_query = """
        INSERT INTO pincode_data (Pincode, District, StateName)
        VALUES (%s, %s, %s)
        """
        cursor.executemany(insert_query, df_filtered.values.tolist())
        conn.commit()

    print("Data inserted successfully!")

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        print("MySQL connection is closed")
