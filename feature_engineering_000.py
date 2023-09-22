

# FEATURE ENGINEERING

#-----------------------------------------------------------------------------------
def fe_create_route(df):
    """
    Creates a new 'fe_route' column by combining and sorting 'origin' and 'destination',
    separated by an underscore, and adds it to the dataframe.

    Parameters:
    - df: The input dataframe.

    Returns:
    - df: The input dataframe with the added 'fe_route' column.
    """
    df['fe_route'] = df.apply(lambda row: '_'.join(sorted([row['origin'], row['destination']])), axis=1)
    
    return df
#----------------------------------------------------------------------------------
def fe_create_mean_route_fare_per_passenger(df):
    """
    This function 'creates' a new column called
    'fe_mean_route_fare_per_passenger'. It does this by simply 
    renaming a column called 'itin_fare' after 
    the dw_calculate_mean_fare_by_route(df) function has
    transformed a tickets dataframe to perform a mean calculation
    on 'itin_fare' when grouping by 'route'.
    The value is a mean per passenger for the round trip route 
    since the 'itin_values' are per passenger for the round trip."""

    # Rename the 'itin_fare' column to 'fe_mean_route_fare_per_passenger'
    df.rename(columns={'itin_fare': 'fe_mean_route_fare_per_passenger'}, inplace=True)
    
    return df   


#----------------------------------------------------------------------------------

def fe_calculate_route_airport_operations_cost(df):
    """
    This function creates new columns 'airport_origin_cost' and 'airport_destination_cost'
    based on airport sizes and then calculates the 'fe_route_airport_operations_cost' column
    by summing these values.

    I assume that the round trip only incurs two airport charges.

    parameter:
    df: the input dataframe
    
    returns:
    df: the input dataframe now with the added columns.
    """
    df['airport_origin_cost'] = df['type_origin'].apply(lambda x: 10000 if x == 'large_airport' else (5000 if x == 'medium_airport' else 0))
    df['airport_destination_cost'] = df['type_destination'].apply(lambda x: 10000 if x == 'large_airport' else (5000 if x == 'medium_airport' else 0))
    df['fe_route_airport_operations_cost'] = df['airport_origin_cost'] + df['airport_destination_cost']
    
    return df

#--------------------------------------------------------------------------

def fe_create_multiple_mean_values_with_count(df):
    """
    This function is applied to a dataframe that has been
    grouped by 'route' and over which mean and count values have been 
    calculated for the columns below via the 
    dw_calculate_varied_grouped_means_with_count(df) function:
    - 'air_time'
    - 'distance' 
    - 'occupancy_rate'
    - 'dep_delay'
    - 'arr_delay'   
    - 'op_carrier'
    This current function simply renames these columns 
    so that it is clear that they are feature engineering 
    columns with aggregated mean (or count) values for each route.

    Parameters:
    - df: The input dataframe. Intended to be a df over which
    dw_calculate_varied_grouped_means_with_count(df) has been applied.

    Returns:
    - df: A dataframe with renamed columns to make explicit
    that these columns are data engineered, aggregate values.
    """
    df.rename(columns={
        'itin_fare': 'fe_mean_route_fare_per_passenger',
        'air_time': 'fe_route_mean_air_time',
        'distance': 'fe_route_mean_distance',
        'occupancy_rate': 'fe_route_mean_occupancy_rate',
        'dep_delay': 'fe_route_mean_dep_delay',
        'arr_delay': 'fe_route_mean_arr_delay',
        'op_carrier': 'fe_route_distinct_op_carrier_count'
                        }, inplace=True)    

    return df



#--------------------------------------------------------------------------

def fe_calculate_route_delay_cost(df):
    """
    Calculates the value for the 'fe_route_delay_cost' column based on
    charge for 15+ min airport delays and adds it to the dataframe. It uses the
    mean departure and mean arrival delay columns for this calculation.
    It ads these values in the column ['fe_route_delay_cost'].

    Parameters:
    - df: The input dataframe.

    Returns:
    - df: The input dataframe with the added 'fe_route_delay_cost' column.
    """
    def calculate_delay_cost(value):
        adjusted_value = value - 15
        if adjusted_value <= 0:
            return 0
        else:
            return adjusted_value * 75

    df['fe_route_delay_cost'] = (df['fe_route_mean_dep_delay'].apply(calculate_delay_cost) +
                                 df['fe_route_mean_arr_delay'].apply(calculate_delay_cost))
    
    return df

#--------------------------------------------------------------------------

def fe_calculate_round_trip_route_dio_cost(df):
    """
    Calculates the round trip route depreciation, insurance, 
    and other cost and adds it to the dataframe as the column
    ['fe_round_trip_route_dio_cost'].

    Parameters:
    - df: The input dataframe.

    Returns:
    - df: The input dataframe with the added 'fe_round_trip_route_dio_cost' column.
    """
    total_route_distance = df['fe_route_mean_distance'] * 2
    df['fe_round_trip_route_dio_cost'] = total_route_distance * 1.18
    
    return df

#-------------------------------------------------------------------

def fe_calculate_round_trip_route_fomc_cost(df):
    """
    Calculates the round trip route fuel, oil, maintenance, 
    and crew cost and adds it to the dataframe as the column
    ['fe_round_trip_route_fomc_cost'].

    Parameters:
    - df: The input dataframe.

    Returns:
    - df: The input dataframe with the added ['fe_round_trip_route_fomc_cost']column.
    """
    total_route_distance = df['fe_route_mean_distance'] * 2
    df['fe_round_trip_route_fomc_cost'] = total_route_distance * 8
    
    return df

#-------------------------------------------------------------------

def fe_calculate_round_trip_route_fare_revenue(df):
    """
    Calculates the round trip fare revenue for the route and creates the column ['fe_round_trip_route_fare_revenue'].
    It does this my multiplying the total route passenger number (400 = 200 departure flight + 200 return flight) by mean route occupancy rate.
    it then multiplies that value by the mean fare price for the route.

    Parameters:
    - df: The input dataframe.

    Returns:
    - df: The input dataframe with the added ['fe_round_trip_route_fare_revenue']  column.
    """
    adjusted_passenger_count = df['fe_route_mean_occupancy_rate'] * 400
    df['fe_round_trip_route_fare_revenue'] = (df['fe_mean_route_fare_per_passenger'] * adjusted_passenger_count) 
    
    return df

#--------------------------------------------------------------------------


def fe_calculate_round_trip_route_baggage_revenue(df):
    """
    Calculates the round trip baggage revenue for the route and creates the column ['fe_round_trip_route_baggage_revenue'].
    It does this my multiplying the total route passenger number (400 = 200 departure flight + 200 return flight) by mean route occupancy rate and discounts this number by half (in line with the prompt estimate that 50% of passengers carry 1 bag each way). It then multiplies this by the 70 total round trip baggage charge. For simplicity I assume this estimate fully captures anticipated baggage revenue for a round trip flight on a route.

    Parameters:
    - df: The input dataframe.

    Returns:
    - df: The input dataframe with the added '['fe_round_trip_route_baggage_revenue'] column.
    """
    adjusted_bag_carrying_passenger_count = (df['fe_route_mean_occupancy_rate'] * 400 * 0.5)
    df['fe_round_trip_route_baggage_revenue'] = (    adjusted_bag_carrying_passenger_count * 70) 
    
    return df

#--------------------------------------------------------------------------

def fe_calculate_round_trip_total_revenue(df):
    """
    Calculates the round trip total revenue by adding baggage revenue and fare revenue,
    and adds the result to the dataframe.

    Parameters:
    - df: The input dataframe.

    Returns:
    - df: The input dataframe with the added 'fe_round_trip_total_revenue' column.
    """
    df['fe_round_trip_total_revenue'] = df['fe_round_trip_route_baggage_revenue'] + df['fe_round_trip_route_fare_revenue']
    
    return df

#--------------------------------------------------------------------------

def fe_calculate_round_trip_total_variable_cost(df):
    """
    Calculates the round trip total variable cost by adding various cost components,
    and adds the result to the dataframe.

    Parameters:
    - df: The input dataframe.

    Returns:
    - df: The input dataframe with the added 'fe_round_trip_total_variable_cost' column.
    """
    df['fe_round_trip_total_variable_cost'] = (df['fe_round_trip_route_fomc_cost'] +
                                               df['fe_round_trip_route_dio_cost'] +
                                               df['fe_route_delay_cost'] +
                                               df['fe_route_airport_operations_cost'])
    
    return df



#--------------------------------------------------------------------------
def fe_calculate_per_round_trip_route_profit(df):
    """
    Calculates the per-round trip route profit by subtracting total variable cost (per round trip) from total revenue (per round trip), and adds the result to the dataframe.

    Parameters:
    - df: The input dataframe.

    Returns:
    - df: The input dataframe with the added 'fe_per_round_trip_route_profit' column.
    """
    df['fe_per_round_trip_route_profit'] = df['fe_round_trip_total_revenue'] - df['fe_round_trip_total_variable_cost']
    
    return df


#--------------------------------------------------------------------------

def fe_calculate_break_even_point_in_number_of_round_trip_flights_for_route(df):
    """
    Calculates the break-even point in terms of the number of round trip flights needed to cover the fixed cost of the airplane, and adds the result to the dataframe.

    Parameters:
    - df: The input dataframe.

    Returns:
    - df: The input dataframe with the added 'fe_break_even_point_in_number_of_round_trip_flights_for_route' column.
    """
    fixed_cost_of_airplane = 90000000
    df['fe_break_even_point_in_number_of_round_trip_flights_for_route'] = fixed_cost_of_airplane / df['fe_per_round_trip_route_profit']
    
    return df


#--------------------------------------------------------------------------


#? a function to standardize the values of various columns

#--------------------------------------------------------------------------

def fe_calculate_total_profit_for_route_2019q1(df):
    """
    Calculates the total profit for each route in the given dataframe for 2019 Q1.

    Parameters:
    - df: The input dataframe containing 'fe_per_round_trip_route_profit' and 'fe_number_of_flights_per_route' columns.

    Returns:
    - df: The input dataframe with the added 'fe_total_profit_for_route_2019q1' column.
    """
    df['fe_total_profit_for_route_2019q1'] = df['fe_per_round_trip_route_profit'] * df['fe_number_of_flights_per_route']
    return df

#--------------------------------------------------------------------------

#--------------------------------------------------------------------------




