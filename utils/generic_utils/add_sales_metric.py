import re
from pyspark.sql.functions import col,when, lit,sum,count
from pyspark.sql import DataFrame
from typing import List
from functools import reduce




def add_sales_metrics(df: DataFrame, 
                     outcome_metric: List[str], 
                     weight_column: str = 'Order Quantity - Pounds',
                     total_sales_column: str = "net_revenue") -> DataFrame:
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

def add_other_metrics_fiscal_year(df, group_cols, sum_cols, fiscal_year_col='Fiscal Year'):
    """
    Calculate sum metrics with fiscal year segmentation
    
    Args:
        df: Input DataFrame
        group_cols: List of columns to group by
        sum_cols: List of numeric columns to sum
        fiscal_year_col: Name of the fiscal year column in the source data
    
    Returns:
        DataFrame with sum metrics segmented by fiscal year and overall ("All")
    """
    
    # Helper function to build aggregation expressions
    def build_agg_exprs(sum_cols):
        agg_exprs = [sum(col_name).alias(f"{col_name}_sum") for col_name in sum_cols]
        agg_exprs.append(count("*").alias("total_records"))
        return agg_exprs
    
    # Get distinct fiscal years from the data
    fiscal_years = [row[0] for row in df.select(fiscal_year_col).distinct().collect()]
    fiscal_years.sort()  # Sort years for consistent ordering
    
    # Build aggregation expressions
    agg_exprs = build_agg_exprs(sum_cols)
    
    # Calculate metrics for "All" (no fiscal year filtering)
    result_all = df.groupBy(group_cols).agg(*agg_exprs)
    result_all = result_all.withColumn("fiscal_year", lit("All"))
    
    # Calculate metrics for each individual fiscal year
    yearly_results = []
    for year in fiscal_years:
        filtered_df = df.filter(col(fiscal_year_col) == year)
        result_year = filtered_df.groupBy(group_cols).agg(*agg_exprs)
        result_year = result_year.withColumn("fiscal_year", lit(str(year)))
        yearly_results.append(result_year)
    
    # Combine all results: yearly results first, then "All"
    all_results = yearly_results + [result_all]
    final_result = reduce(lambda df1, df2: df1.union(df2), all_results)
    
    # Reorder columns to place "fiscal_year" right after group_cols
    column_order = group_cols + ["fiscal_year"] + [c for c in final_result.columns if c not in group_cols and c != "fiscal_year"]
    final_result = final_result.select(*column_order)
    
    return final_result



