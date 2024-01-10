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

def main():
    """
    Execute the script functions to process data in the provided CSV files
    """
    df1 = read_dataset('dataset1.csv')
    print(df1)

if __name__ == "__main__":
    main()