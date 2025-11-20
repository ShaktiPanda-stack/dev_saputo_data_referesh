from pyspark.sql import DataFrame
from typing import List, Dict

def filter_dataframe_columns(
    dataframes: Dict[str, DataFrame],
    common_columns: List[str]
) -> Dict[str, DataFrame]:
    """
    Filter dataframes to keep only common columns and columns derived from variable names.
    
    Parameters:
    -----------
    dataframes : Dict[str, DataFrame]
        Dictionary where keys are variable names (e.g., 'cost_to_serve_df') 
        and values are the corresponding DataFrames
    common_columns : List[str]
        List of common column names to keep in all dataframes
    
    Returns:
    --------
    Dict[str, DataFrame]
        Dictionary with same keys but filtered DataFrames
    
    Example:
    --------
    dataframes = {
        'cost_to_serve_df': cost_to_serve_df,
        'cost_to_serve_pct_sales_df': cost_to_serve_pct_sales_df
    }
    common_columns = ['customer_id', 'date', 'region']
    
    filtered_dfs = filter_dataframe_columns(dataframes, common_columns)
    """
    
    filtered_dataframes = {}
    
    for var_name, df in dataframes.items():
        # Extract the column name from variable name by removing '_df' suffix
        if var_name.endswith('_df'):
            derived_column = var_name[:-3]  # Remove '_df'
        else:
            derived_column = var_name
        
        # Build list of columns to keep
        columns_to_keep = common_columns.copy()
        
        # Add derived column if it exists in the dataframe
        if derived_column in df.columns:
            columns_to_keep.append(derived_column)
        
        # Filter to only existing columns (in case some common columns don't exist)
        existing_columns = [col for col in columns_to_keep if col in df.columns]
        
        # Select only the required columns
        filtered_dataframes[var_name] = df.select(existing_columns)
    
    return filtered_dataframes