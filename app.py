"""
    Author: Javier A. Dastas
    lab: lab-sql-python-connetion
    Objective(s):
        This lab allows to practice and apply the concepts and techniques taught in class.
            - Upon completion of this lab, you will be able to:
            - Write a Python script to connect to a relational database using the appropriate Python 
              library and query it using SQL commands.
"""

import pymysql
import pandas as pd
from sqlalchemy import create_engine

def connect_to_database():
    """Establishes a connection to the Sakila database."""
    try:
        engine = create_engine('mysql+pymysql://root:root@localhost/sakila')
        connection = engine.connect()
        print("Connection successfully established.")
        return engine
    except Exception as e:
        print("Error connecting to the database:", e)
        return None

def rentals_month(engine, month, year):
    """
    Retrieves rental data for a specified month and year from the Sakila database.

    Arguments:
        engine: SQLAlchemy engine object to connect to the database.
        month: Integer representing the month.
        year: Integer representing the year.

    Returns:
        Pandas DataFrame containing rental data for the specified month and year.
    """
    query = f"""
    SELECT customer_id, COUNT(*) AS rental_count
    FROM rental
    WHERE MONTH(rental_date) = {month} AND YEAR(rental_date) = {year}
    GROUP BY customer_id;
    """
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        print("Error executing query:", e)
        return None

def rental_count_month(df, month, year):
    """
    Processes rental data to count the number of rentals made by each customer.

    Arguments:
        df: Pandas DataFrame containing rental data for a specific month and year.
        month: Integer representing the month.
        year: Integer representing the year.

    Returns:
        Pandas DataFrame containing customer_id and their rental counts, with a column named accordingly.
    """
    column_name = f"rentals_{str(month).zfill(2)}_{year}"
    df[column_name] = df['rental_count']
    return df[['customer_id', column_name]]

def compare_rentals(df1, df2):
    """
    Compares rental data between two months and calculates the difference for each customer.

    Arguments:
        df1: Pandas DataFrame containing rental counts for the first month.
        df2: Pandas DataFrame containing rental counts for the second month.

    Returns:
        Combined DataFrame with a new 'difference' column.
    """
    combined_df = pd.merge(df1, df2, on='customer_id', how='outer').fillna(0)
    combined_df['difference'] = combined_df.iloc[:, 1] - combined_df.iloc[:, 2]
    return combined_df

# Main script
if __name__ == "__main__":
    # Step 1: Connect to the database
    engine = connect_to_database()

    if engine:
        # Step 2: Get rental data for May and June 2005
        may_rentals = rentals_month(engine, 5, 2005)
        june_rentals = rentals_month(engine, 6, 2005)

        if may_rentals is not None and june_rentals is not None:
            # Step 3: Process rental counts for each month
            may_counts = rental_count_month(may_rentals, 5, 2005)
            june_counts = rental_count_month(june_rentals, 6, 2005)

            # Step 4: Compare rentals between the two months
            comparison = compare_rentals(may_counts, june_counts)

            # Display the result
            print("\nComparison of Rentals between May and June 2005:")
            print(comparison)

        else:
            print("Failed to retrieve rental data.")
    else:
        print("Database connection was not established.")
