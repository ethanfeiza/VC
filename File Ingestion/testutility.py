import logging
import os
import io
import subprocess
import yaml
import pandas as pd
import datetime 
import gc
import re
import gzip


################
# File Reading #
################

def read_config_file(filepath):
    with open(filepath, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.error(exc)


def replacer(string, char):
    pattern = char + '{2,}'
    string = re.sub(pattern, char, string) 
    return string

def col_header_val(df,table_config):
    '''
    replace whitespaces in the column
    and standardized column names
    '''
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace('[^\w]','_',regex=True)
    df.columns = list(map(lambda x: x.strip('_'), list(df.columns)))
    df.columns = list(map(lambda x: replacer(x,'_'), list(df.columns)))
    expected_col = list(map(lambda x: x.lower(),  table_config['columns']))
    expected_col.sort()
    df.columns =list(map(lambda x: x.lower(), list(df.columns)))
    df = df.reindex(sorted(df.columns), axis=1)
    if len(df.columns) == len(expected_col) and list(expected_col)  == list(df.columns):
        print("column name and column length validation passed")
        return 1
    else:
        print("column name and column length validation failed")
        mismatched_columns_file = list(set(df.columns).difference(expected_col))
        print("Following File columns are not in the YAML file",mismatched_columns_file)
        missing_YAML_file = list(set(expected_col).difference(df.columns))
        print("Following YAML columns are not in the file uploaded",missing_YAML_file)
        logging.info(f'df columns: {df.columns}')
        logging.info(f'expected columns: {expected_col}')
        return 0
    
def pipe_file(df, config_data):
    # Verify that the 'outbound_delimiter' key exists in config_data
    if 'outbound_delimiter' in config_data:
        delimiter = config_data['outbound_delimiter']
    else:
        delimiter = '|'

    # Verify that the 'file_name' key exists in config_data
    if 'file_name' in config_data:
        file_name = config_data['file_name']
    else:
        file_name = 'output'

    # Check if the directory 'output' exists, if not, create it
    output_dir = 'output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Define the output file path
    output_file = os.path.join(output_dir, f"{file_name}.gz")

    try:
        # Write the DataFrame to a pipe-separated text file in gz format
        with gzip.open(output_file, 'wt', encoding='utf-8') as file:
            df.to_csv(file, sep=delimiter, index=False)
        print(f"Data written to {output_file}")
    except Exception as e:
        print(f"Error writing data to {output_file}: {str(e)}")

def create_summary(df):
    # Get the total number of rows and columns in the DataFrame
    num_rows, num_columns = df.shape

    # Calculate the file size of the DataFrame in bytes
    file_size = df.memory_usage(deep=True).sum()

    # Convert the file size to a human-readable format (e.g., KB, MB, GB)
    file_size_readable = sizeof_fmt(file_size)

    summary = {
        'Total Rows': num_rows,
        'Total Columns': num_columns,
        'File Size': file_size_readable
    }

    return summary

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f %s%s" % (num, 'Yi', suffix)
