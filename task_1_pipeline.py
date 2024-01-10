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
    df[['first_name', 'last_name']] = df['name'].str.split(pat=' ',n=1, expand=True)
    return df


def remove_leading_zeros(df: DataFrame) -> DataFrame:
    """
    Removes leading zeros from the 'price' field.

    Args: 
        df (DataFrame): The DataFrame with a 'price' column where leading zeros are to be removed.

    Returns:
        DataFrame: The DataFrame with modified 'price' field.
    """
    df['price'] = df['price'].apply(lambda x: x.lstrip('0') if isinstance(x, str) else x)
    return df


def drop_rows_without_name(df: DataFrame) -> DataFrame:
    """
    Drops rows from the DataFrame where the 'name' field is missing.

    Args: 
        df (DataFrame): The DataFrame from which rows with missing 'name' are to be dropped.

    Returns:
        DataFrame: The DataFrame with rows missing 'name' dropped.
    """
    df.dropna(subset=['name'], inplace=True)
    return df


def add_above_100_column(df: DataFrame) -> DataFrame:
    """
    Adds a new column 'above_100' to indicate if 'price' is greater than 100.

    Args: 
        df (DataFrame): The DataFrame to add the 'above_100' column to.

    Returns:
        DataFrame: The DataFrame with the new 'above_100' column.
    """
    df['above_100'] = df['price'].astype(float) > 100
    return df


def process_dataset(df: DataFrame) -> DataFrame:
    """
    Processes the dataset by applying a series of transformation functions.

    Args: 
        df (DataFrame): A DataFrame to process.

    Returns:
        DataFrame: A processed DataFrame adhering to the specified requirements.
    """
    df = split_name(df)
    df = remove_leading_zeros(df)
    df = drop_rows_without_name(df)
    df = add_above_100_column(df)
    return df


def combine_datasets(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """
    Combines two DataFrames into one.

    Args:
        df1 (DataFrame): The first DataFrame to combine.
        df2 (DataFrame): The second DataFrame to combine.

    Returns:
        DataFrame: A combined DataFrame from df1 and df2.
    """
    return pd.concat([df1, df2], ignore_index=True)


def save_processed_dataset(df, output_csv_path):
    """
    Saves the processed dataset to a CSV file.

    Args:
        df (DataFrame): The DataFrame to be saved.
        output_csv_path (str): The file path where the CSV file will be saved.it 
    """
    df.to_csv(output_csv_path, index=False)


def main():
    """
    Execute the script functions to process data in the provided CSV files
    """
    df1 = read_dataset('dataset1.csv')
    df2 = read_dataset('dataset2.csv')

    processed_df1 = process_dataset(df1)
    processed_df2 = process_dataset(df2)

    combined_df = combine_datasets(processed_df1, processed_df2)

    save_processed_dataset(combined_df, 'processed_combined_dataset.csv')

    print(combined_df)


if __name__ == "__main__":
    main()