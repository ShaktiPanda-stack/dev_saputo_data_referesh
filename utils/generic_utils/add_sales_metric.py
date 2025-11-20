import re
from pyspark.sql.functions import col,when, lit,sum,count
from pyspark.sql import DataFrame
from typing import List



def add_sales_metrics(df: DataFrame, 
                     outcome_metric: List[str], 
                     weight_column: str = 'Order Quantity - Pounds',
                     total_sales_column: str = "Net Revenue") -> DataFrame:
    """
    Add % Sales and $/Lb columns for specified sales columns.
    
    Parameters:
    -----------
    df : DataFrame
        Input PySpark DataFrame
    sales_columns : List[str]
        List of column names containing sales values
    weight_column : str, default 'weight_lbs'
        Column name containing weight in pounds
    total_sales_column : str, default 'total_sales'
        Column name for total sales (if not exists, will be calculated)
    
    Returns:
    --------
    DataFrame
        DataFrame with additional % Sales and $/Lb columns
    """
    
    # Start with the original dataframe
    result_df = df

    
    # Add % Sales and $/Lb columns for each sales column
    for sales_col in outcome_metric:
        
        # Create % Sales column
        pct_sales_col = f"{sales_col}_pct_sales"
        result_df = result_df.withColumn(
            pct_sales_col,
            when(col(total_sales_column) != 0, 
                 (col(sales_col) / col(total_sales_column)) * 100)
            .otherwise(0)
        )
        
        # Create $/Lb column
        dollar_per_lb_col = f"{sales_col}_per_lb"
        result_df = result_df.withColumn(
            dollar_per_lb_col,
            when(col(weight_column) != 0, 
                 col(sales_col) / col(weight_column))
            .otherwise(0)
        )
    
    return result_df


def add_other_metrics(df,group_cols,sum_cols ):

    # # Example inputs
    # group_cols = dimension_columns      # columns to group by
    # sum_cols = ["Net Sales Raw", "Order Quantity - Pounds"]            # numeric columns to sum

    # Group by and aggregate
    agg_df = df.groupBy(group_cols).agg(
        *[sum(col_name).alias(f"{col_name}_sum") for col_name in sum_cols],
        count("*").alias("total_records"),
    )

    return agg_df




