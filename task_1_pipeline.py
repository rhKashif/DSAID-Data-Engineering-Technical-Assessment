"""
Assessment 1: Data Pipelines 

Script for processing dataset1.csv and dataset2.csv as part of Data Engineering Technical Assessment

This script reads the datasets, processes them according to the specified requirements, and outputs a single combined CSV file.

Author: Hassan Kashif
"""

import pandas as pd
from pandas import DataFrame

def read_dataset(csv_path: str) -> DataFrame:
    """
    Reads a CSV file into a pandas DataFrame.

    Args:
        csv_path (str): The path to the CSV file

    Returns:
        DataFrame: A pandas DataFrame containing data from the CSV file
    """
    return pd.read_csv(csv_path)

def split_name(df: DataFrame) -> DataFrame:
    """
    Splits the 'name' field into 'first_name' and 'last_name'.

    Args: 
        df (DataFrame): The DataFrame with a 'name' column to be split.

    Returns:
        DataFrame: The DataFrame with added 'first_name' and 'last_name' columns.
    """
    df[['first_name', 'last_name']] = df['name'].str.split(' ', 1, expand=True)
    return df

def remove_leading_zeros(df):
    """
    Removes leading zeros from the 'price' field.

    Args: 
        df (DataFrame): The DataFrame with a 'price' column where leading zeros are to be removed.

    Returns:
        DataFrame: The DataFrame with modified 'price' field.
    """
    df['price'] = df['price'].apply(lambda x: x.lstrip('0') if isinstance(x, str) else x)
    return df

def drop_rows_without_name(df):
    """
    Drops rows from the DataFrame where the 'name' field is missing.

    Args: 
        df (DataFrame): The DataFrame from which rows with missing 'name' are to be dropped.

    Returns:
        DataFrame: The DataFrame with rows missing 'name' dropped.
    """
    df.dropna(subset=['name'], inplace=True)
    return df

def main():
    """
    Execute the script functions to process data in the provided CSV files
    """
    df1 = read_dataset('dataset1.csv')
    df1 = split_name(df1)
    df1 = remove_leading_zeros(df1)
    df1 = drop_rows_without_name(df1)
    print(df1)

if __name__ == "__main__":
    main()