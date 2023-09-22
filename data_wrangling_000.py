#=======================================================================================
"""
Data Wrangling

This file contains functions that read the csv files into pandas dataframes,
filter some explicitly irrelevant data, and make other initial transoformations and 
mergers. I preface all functions with 'dw' to indicate that they are data wrangling functions.

"""
#=======================================================================================
import os
import glob
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

#=======================================================================================
# Initial data wrangling
#=======================================================================================
#---------------------------------------------------------------------------------------
# This function ommitted from notebook
#
# def dw_get_csv_files_in_folder():
#    """
#    This function creates a list of all the .csv files in the current folder.
#    It relies on importing the glob module: import glob. It returns the resulting list.
#    """ 
#    
#
#    csv_files = glob.glob('*.csv')
#    return csv_files

#---------------------------------------------------------------------------------------

"""  
function acting up in notebook, omitting
#
#def dw_load_csv_files_as_dataframes(csv_files_list):
#    
    Loads CSV files into pandas dataframes, drops duplicate rows,
    generates dataframe names, and converts column names and
    dataframe names to lowercase.

    Parameters:
   - csv_files_list: A list of csv file paths.

    Returns:
    dataframes of those csv files
    
    # List to hold the DataFrames
    dataframes = []

    # Loop through each CSV file and process it
    for csv_file in csv_files_list:
        try:
            # Read the CSV file into a dataframe
            df = pd.read_csv(csv_file)

            # Remove duplicate rows from the dataframe
            df = df.drop_duplicates()

            # Extract the filename (without the '.csv' extension) for dataframe naming
            df_name = os.path.splitext(os.path.basename(csv_file))[0] + '_initial'

            # Convert dataframe name to lowercase
            df_name = df_name.lower()

            # Convert column names to lowercase
            df.columns = df.columns.str.lower()

            # Assign the dataframe to a variable with the desired name
            globals()[df_name] = df

            # Add the dataframe to the list
            dataframes.append(df)
        
        except Exception as e:
            print(f"Error processing {csv_file}: {e}")

    return dataframes


def process_and_print_dataframes(dataframes_list):
    
    This is a quick fix function to work around the buggy one above.
    This takes a list of dataframes, removes duplicate rows, puts
    columns in lowercase, and prints out the head of the dataframes.

    
    for df in dataframes_list:
        # Remove duplicate rows
        df = df.drop_duplicates()
        
        # Convert column names to lowercase
        df.columns = df.columns.str.lower()
        
        # Print the head of the processed DataFrame
        print(df.head())

"""


def process_dataframe(df):
    # Drop duplicate rows
    df = df.drop_duplicates()

    # Convert column names to lowercase
    df.columns = df.columns.str.lower()

    return df


#=======================================================================================
# airport_codes data wrangling
#=======================================================================================
def dw_subset_airport_codes_for_m_l_airports_US_only(df):
    """
    This function Subsets the airport_codes_initial dataframe.   
    Eliminates airports non US airports. 
    Retains only medium and large airports.
    
    Parameters:
    a dataframe: the intednded df is the initial airport codes df

    Returns:
    a relatively clean df: the cleaned and subsetted airport codes dataframe.
    """
    # Perform basic subseting of airport_codes
    df_clean = df[
        (df['iso_country'] == 'US') &
        (df['type'].isin(['medium_airport', 'large_airport']))
    ]
    
    return df_clean

#---------------------------------------------------------------------------------------
def dw_subset_airport_codes_for_merger(df):
    """This function subsets airport codes,
    further retaining only its columns 'type' and 'iata_code'.
    this is done in preparation for merging with
    the other dataframes"""

    # Subset the dataframe by keeping only 'type' and 'iata_code' columns
    subset_df = df[['type', 'iata_code']]
    
    return subset_df

#---------------------------------------------------------------------------------------



#=======================================================================================
# flights data wrangling
#=======================================================================================

def dw_subset_flights_not_cancelled_only(df):
    """
    Subsets the flights_initial dataframe.   
    Retains only non-cancelled flights.

    Parameters:
    df (dataframe): The initial flights dataframe is the intended input.

    Returns:
    df cleaner (dataframe): The subsetted dataframe with only non-canceld flights.
    """

    # subset for only non cancelled flights
    flights_clean = df[df['cancelled'] == 0.0]

    return flights_clean


#---------------------------------------------------------------------------------------
def dw_convert_distance_column_to_int(df):
    """
    Converts the data type of the 'distance' column in a dataframe to dtype int.
    Non-integer values are converted to NaN. (An attempt at a function to convert
    to float values continued to generate errors.)

    Parameters:
    - df: The input dataframe.

    Returns:
    - df: The resulting dataframe with the converted 'distance' column.
    """
    column_name = 'distance'
    
    if column_name in df.columns:
        try:
            df[column_name] = pd.to_numeric(df[column_name], errors='coerce', downcast='integer')
            print(f"Successfully converted '{column_name}' column to dtype int.")
        except ValueError:
            print(f"An error occurred while converting '{column_name}' column. Converting non-integer values to NaN.")
            df[column_name] = np.nan
    else:
        print(f"No '{column_name}' column found in the dataframe.")
    
    return df


#---------------------------------------------------------------------------------------
def dw_convert_air_time_column_to_float(df):
    """
    Converts the data type of the 'air_time' column (originally in flights dataframe) 
    to dtype float. Non-float values are converted to NaN and will be ignored when aggregating.

    Parameters:
    - df: The input dataframe (flights).

    Returns:
    - df: The resulting dataframe with the converted 'air_time' column.
    """
    column_name = 'air_time'
    
    if column_name in df.columns:
        try:
            df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
            print(f"Successfully converted '{column_name}' column to dtype float.")
        except ValueError:
            print(f"An error occurred while converting '{column_name}' column. Converting non-float values to NaN.")
            df[column_name] = np.nan
    else:
        print(f"No '{column_name}' column found in the dataframe.")
    
    return df



#=======================================================================================
# tickets data wrangling
#=======================================================================================
#---------------------------------------------------------------------------------------
def dw_subset_tickets_roundtrip_only(df):
    """
    Subsets the tickets_initial dataframe to   
    retains only round trip flights from the sample set.

    Parameters:
    df (dataframe): the intended input is the initial tickets dataframe.

    Returns:
    df clean (dataframe): the subsetted tickets dataframe.
    """
    tickets_clean = df[df['roundtrip'] == 1.0]

    return tickets_clean

#---------------------------------------------------------------------------------------

def dw_convert_itin_fare_column_to_float(df):
    """
    Converts the datatype of any 'itin_fare' column of a dataframe
    to datatype float, replacing non-numeric values with NaN.

    Parameters:
    df: dataframe
    Returns:
    df: df with 'itin_fare' column now as a float datatype.
    """
    if 'itin_fare' in df.columns:
        df['itin_fare'] = pd.to_numeric(df['itin_fare'], errors='coerce').astype(float)
    return df

#---------------------------------------------------------------------------------------
def dw_replace_itin_fare_with_group_mean(df):
    """
    Replace 'itin_fare' values of 11.0 with mean values 
    when groupping by'reporting_carrier' 
    and ommitting 11.0 values from mean calculation.

    This function is intended to mitigage the influence of the 
    27k+ 11.0 values in the itin_fare columne of the tickets dataset.

    Parameters:
        df: The input dataframe with problem 11.0 values in 'itin_fare'.

    Returns:
        df: A new dataframe with problem 'itin_fare' values replaced by group means.
    """
    grouped_means = df[df['itin_fare'] != 11.0].groupby('reporting_carrier')['itin_fare'].mean()
    df_copy = df.copy()

    for index, row in df_copy.iterrows():
        if row['itin_fare'] == 11.0:
            carrier = row['reporting_carrier']
            if carrier in grouped_means:
                df_copy.at[index, 'itin_fare'] = grouped_means[carrier]

    return df_copy


#---------------------------------------------------------------------------------------
def dw_transform_calculate_mean_fare_by_route_to_merge_with_flights(df):
    """
    Calculate the mean 'itin_fare' 
    for each 'fe_route' in the input dataframe.
    
    This is a transformation of the tickets dataframe 
    that will be used to merge with the airport codes and flights dataframes. 

    Parameters:
    dataframe: Input dataframe with 'fe_route' and 'itin_fare' columns.
    
    Returns:
    pandas.Series: A series containing the mean fare for each route. This 
    """
    grouped = df.groupby('fe_route')['itin_fare'].mean().reset_index()
    
    return grouped


#=======================================================================================
# merging
#=======================================================================================
def dw_merge_dataframes_with_origin_destination_sizes(df1, df2, df3):
    """
    Merge three dataframes sequentially to ensure that
    route data can be mapped to airport size data.

    Parameters:
    - df1: The first dataframe with a column 'origin';
            this is originally from the flights table.
    - df2: The second dataframe to be merged with df1;
            this is originally from the airport codes table.
            It supplies the airport size of the origin flight.
    - df3: The third dataframe to be merged with 
            the result of df1 and df2. This is also 
            from the airport codes table.
            It suppose the airport size of the destination flight.

    Returns:
    - merged_result: The merged result dataframe. I inner merge for expediency.

    The function performs the following steps:
    1. Merge df2 with df1 on 'iata_code' in df2 and 'origin' in df1. 
       Columns in df2 receive the '_origin' suffix.
    2. Merge df3 with the result of step 1 on 'iata_code' in df3 and 'destination' 
        in the result. Columns in df3 receive the '_destination' suffix.
    The final merged result is returned.
    """

    # Step 1: Merge df2 with df1
    merged_step1 = pd.merge(df1, df2, left_on='origin', right_on='iata_code', how='inner', suffixes=('', '_origin'))

    # Step 2: Merge df3 with the result of step 1
    merged_result = pd.merge(merged_step1, df3, left_on='destination', right_on='iata_code', how='inner', suffixes=('_origin', '_destination'))

    return merged_result

#---------------------------------------------------------------------------------------
def dw_merge_dataframes_with_fe_route(df1, df2):
    """
    Merges two dataframes based on the 'fe_route' column. 
    I use an inner join for expediency.

    parameters:
    - df1: The first dataframe (with flights and route data).
    - df2: The second dataframe (with aggregated tickets and route data).

    Returns:
    - merged_df: The merged dataframe.
    """
    merged_df = df1.merge(df2, on='fe_route', how='inner')
    
    return merged_df

#=======================================================================================
# some final wrangling 
#=======================================================================================


def dw_transform_calculate_varied_grouped_means_with_count(df):
    """
    This function performs grouping and aggregation transformations on the entire merged
    dataframe. It groups the dataframe by the 'fe_route' column and calculates mean values
    for 'distance', 'occupancy_rate', 'air_time', 'dep_delay',
    'arr_delay', 'fe_route_airport_operations_cost' (redundant calculation), 
    'fe_mean_route_fare_per_passenger' (redundant calculation) columns. 
    It also counts distinct values for 'op_carrier' for each route and 
    names this column 'fe_route_distinct_op_carrier_count'.
    Additionally, it counts the number of rows per 'fe_route'.

    Parameters:
    - df: The dataframe that merges data from the original 
      airport codes, flights, and tickets datasets.

    Returns:
    - grouped_data: A dataframe containing the mean values for various specified columns,
      the count of distinct 'op_carrier' values, and the count of rows per 'fe_route'.
    """
    grouped_data = df.groupby('fe_route').agg({
        'air_time': 'mean',
        'distance': 'mean',
        'occupancy_rate': 'mean',
        'dep_delay': 'mean',
        'arr_delay': 'mean',
        'fe_route_airport_operations_cost': 'mean', # this is redundant but not harmful
        'fe_mean_route_fare_per_passenger': 'mean', # this is redundant but not harmful
        'op_carrier': 'nunique'
    }).reset_index()
    
    # Count of rows per 'fe_route'
    grouped_data['fe_number_of_flights_per_route'] = df.groupby('fe_route').size().values
    
    return grouped_data


#---------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------
