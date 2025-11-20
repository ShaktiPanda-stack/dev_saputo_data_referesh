from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Dict
from pyspark.sql.functions import col,when, lit,percentile_approx
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def calculate_quantiles_multi_df_metrics(df_metric_mapping: Dict[DataFrame, str], 
                                        quantiles_range: tuple = (5, 95),
                                        interval: int = 5,
                                        df_names: List[str] = None) -> DataFrame:
    """
    Calculate quantiles for different metrics across multiple DataFrames.
    Returns DataFrame with percentiles as columns and dataframe_metric as rows.
    
    OPTIMIZED VERSION: Uses pure DataFrame operations without collecting intermediate results.
    
    Parameters:
    -----------
    df_metric_mapping : Dict[DataFrame, str]
        Dictionary with DataFrames as keys and metric column names as values
        Example: {df1: 'sales', df2: 'cost', df3: 'revenue'}
    quantiles_range : tuple, default (5, 95)
        Start and end percentiles (5th to 95th percentile)
    interval : int, default 5
        Interval between quantiles (every 5%)
    df_names : List[str], optional
        List of names for DataFrames (for labeling). If None, uses df1, df2, etc.
    
    Returns:
    --------
    DataFrame
        DataFrame with rows as 'dataframe_metric' and columns as percentile values
        Example columns: dataframe_metric, p5, p10, p15, ..., p95
    """
    
    # Generate quantile percentages (5, 10, 15, ..., 95)
    start_pct, end_pct = quantiles_range
    quantile_percentages = list(range(start_pct, end_pct + interval, interval))
    quantile_decimals = [q/100.0 for q in quantile_percentages]
    
    # Initialize list to collect result DataFrames
    result_dfs = []
    
    # Generate DataFrame names if not provided
    if df_names is None:
        df_names = [f'df{i+1}' for i in range(len(df_metric_mapping))]
    
    # Process each DataFrame and its corresponding metric
    for i, (df, metric_column) in enumerate(df_metric_mapping.items()):
        
        df_name = df_names[i] if i < len(df_names) else f'df{i+1}'
        
        # Check if metric column exists
        if metric_column not in df.columns:
            print(f"Warning: Column '{metric_column}' not found in DataFrame '{df_name}'. Skipping...")
            continue
        
        # Calculate all quantiles for this DataFrame's metric using agg
        quantile_exprs = []
        for j, decimal in enumerate(quantile_decimals):
            quantile_exprs.append(
                percentile_approx(col(metric_column), decimal).alias(f'p{quantile_percentages[j]}')
            )
        
        # Use agg() to calculate all quantiles in one go - returns a single-row DataFrame
        quantile_df = df.agg(*quantile_exprs)
        
        # Add the dataframe_metric column as a literal
        quantile_df = quantile_df.withColumn("dataframe_metric", lit(metric_column))
        
        # Reorder columns to put dataframe_metric first
        column_order = ["dataframe_metric"] + [f'p{p}' for p in quantile_percentages]
        quantile_df = quantile_df.select(*column_order)
        
        result_dfs.append(quantile_df)
    
    # Combine all results using union
    if result_dfs:
        final_result = result_dfs[0]
        for df in result_dfs[1:]:
            final_result = final_result.union(df)
        return final_result
    else:
        # Return empty DataFrame with schema if no data
        spark = SparkSession.getActiveSession()
        schema_fields = [StructField("dataframe_metric", StringType(), True)]
        for percentage in quantile_percentages:
            schema_fields.append(StructField(f"p{percentage}", DoubleType(), True))
        
        schema = StructType(schema_fields)
        return spark.createDataFrame([], schema)



# def calculate_quantiles_multi_df_metrics(df_metric_mapping: Dict[DataFrame, str], 
#                                         quantiles_range: tuple = (5, 95),
#                                         interval: int = 5,
#                                         df_names: List[str] = None) -> DataFrame:
#     """
#     Calculate quantiles for different metrics across multiple DataFrames.
#     Returns DataFrame with percentiles as columns and dataframe_metric as rows.
    
#     Parameters:
#     -----------
#     df_metric_mapping : Dict[DataFrame, str]
#         Dictionary with DataFrames as keys and metric column names as values
#         Example: {df1: 'sales', df2: 'cost', df3: 'revenue'}
#     quantiles_range : tuple, default (30, 95)
#         Start and end percentiles (30th to 95th percentile)
#     interval : int, default 5
#         Interval between quantiles (every 5%)
#     df_names : List[str], optional
#         List of names for DataFrames (for labeling). If None, uses df1, df2, etc.
    
#     Returns:
#     --------
#     DataFrame
#         DataFrame with rows as 'dataframe_metric' and columns as percentile values
#         Example columns: dataframe_metric, p30, p35, p40, ..., p95
#     """
    
#     # Generate quantile percentages (30, 35, 40, ..., 95)
#     start_pct, end_pct = quantiles_range
#     quantile_percentages = list(range(start_pct, end_pct + interval, interval))
#     quantile_decimals = [q/100.0 for q in quantile_percentages]
    
#     # Initialize result list to collect all quantile data
#     result_data = []
    
#     # Generate DataFrame names if not provided
#     if df_names is None:
#         df_names = [f'df{i+1}' for i in range(len(df_metric_mapping))]
    
#     # Process each DataFrame and its corresponding metric
#     for i, (df, metric_column) in enumerate(df_metric_mapping.items()):
        
#         df_name = df_names[i] if i < len(df_names) else f'df{i+1}'
        
#         # Check if metric column exists
#         if metric_column not in df.columns:
#             print(f"Warning: Column '{metric_column}' not found in DataFrame '{df_name}'. Skipping...")
#             continue
        
#         # Calculate all quantiles for this DataFrame's metric
#         quantile_exprs = []
#         for j, decimal in enumerate(quantile_decimals):
#             quantile_exprs.append(
#                 percentile_approx(col(metric_column), decimal).alias(f'p{quantile_percentages[j]}')
#             )
        
#         # Execute quantile calculations
#         quantile_results = df.select(*quantile_exprs).collect()[0]
        
#         # Create a single row with all quantile values as columns
#         row_data = {
#             'dataframe_metric': f"{metric_column}"
#         }
        
#         # Add each percentile as a column
#         for j, percentage in enumerate(quantile_percentages):
#             quantile_value = quantile_results[f'p{percentage}']
#             row_data[f'p{percentage}'] = float(quantile_value) if quantile_value is not None else None
        
#         result_data.append(row_data)
    
#     # Create the result DataFrame
#     spark = SparkSession.getActiveSession()
    
#     # Define schema - dataframe_metric + all percentile columns
#     schema_fields = [StructField("dataframe_metric", StringType(), True)]
#     for percentage in quantile_percentages:
#         schema_fields.append(StructField(f"p{percentage}", DoubleType(), True))
    
#     schema = StructType(schema_fields)
    
#     # Create DataFrame from result data
#     if result_data:
#         result_df = spark.createDataFrame(result_data, schema)
#         return result_df
#     else:
#         # Return empty DataFrame with schema if no data
#         return spark.createDataFrame([], schema)




# def calculate_quantiles_with_cohorts(df_metric_mapping: Dict[DataFrame, str], 
#                                    cohort_column: str,
#                                    quantiles_range: tuple = (10, 10),
#                                    interval: int = 5,
#                                    df_names: List[str] = None) -> DataFrame:
#     """
#     Calculate quantiles for different metrics across multiple DataFrames by cohort groups.
#     Returns DataFrame with percentiles as columns and dataframe_metric_cohort as rows.
    
#     Parameters:
#     -----------
#     df_metric_mapping : Dict[DataFrame, str]
#         Dictionary with DataFrames as keys and metric column names as values
#         Example: {df1: 'sales', df2: 'cost', df3: 'revenue'}
#     cohort_column : str
#         Column name to group by (e.g., 'category', 'segment', 'region')
#     quantiles_range : tuple, default (30, 95)
#         Start and end percentiles (30th to 95th percentile)
#     interval : int, default 5
#         Interval between quantiles (every 5%)
#     df_names : List[str], optional
#         List of names for DataFrames (for labeling). If None, uses df1, df2, etc.
    
#     Returns:
#     --------
#     DataFrame
#         DataFrame with rows as 'dataframe_metric_cohort' and columns as percentile values
#         Example rows: sales_data_sales_high, sales_data_sales_medium, cost_data_cost_premium
#     """
    
#     # Generate quantile percentages (30, 35, 40, ..., 95)
#     start_pct, end_pct = quantiles_range
#     quantile_percentages = list(range(start_pct, end_pct + interval, interval))
#     quantile_decimals = [q/100.0 for q in quantile_percentages]
    
#     # Initialize result list to collect all quantile data
#     result_data = []
    
#     # Generate DataFrame names if not provided
#     if df_names is None:
#         df_names = [f'df{i+1}' for i in range(len(df_metric_mapping))]
    
#     # Process each DataFrame and its corresponding metric
#     for i, (df, metric_column) in enumerate(df_metric_mapping.items()):
        
#         df_name = df_names[i] if i < len(df_names) else f'df{i+1}'
        
#         # Check if metric column and cohort column exist
#         if metric_column not in df.columns:
#             print(f"Warning: Column '{metric_column}' not found in DataFrame '{df_name}'. Skipping...")
#             continue
            
#         if cohort_column not in df.columns:
#             print(f"Warning: Cohort column '{cohort_column}' not found in DataFrame '{df_name}'. Skipping...")
#             continue
        
#         # Get unique cohort values
#         cohort_values = [row[cohort_column] for row in df.select(cohort_column).distinct().collect()]
        
#         # Calculate quantiles for each cohort
#         for cohort_value in cohort_values:
            
#             # Filter data for this cohort
#             cohort_df = df.filter(col(cohort_column) == cohort_value)
            
#             cohort_count = cohort_df.count()
#             # Skip if no data for this cohort
#             if cohort_df.count() == 0:
#                 continue
            
#             # Calculate all quantiles for this DataFrame's metric within this cohort
#             quantile_exprs = []
#             for j, decimal in enumerate(quantile_decimals):
#                 quantile_exprs.append(
#                     percentile_approx(col(metric_column), decimal).alias(f'p{quantile_percentages[j]}')
#                 )
            
#             # Execute quantile calculations
#             quantile_results = cohort_df.select(*quantile_exprs).collect()[0]
            
#             # Create a single row with all quantile values as columns
#             row_data = {
#                 'dataframe_metric_cohort': f"{metric_column}_{cohort_value}",
#                 'record_count': cohort_count  
#             }
            
#             # Add each percentile as a column
#             for j, percentage in enumerate(quantile_percentages):
#                 quantile_value = quantile_results[f'p{percentage}']
#                 row_data[f'p{percentage}'] = float(quantile_value) if quantile_value is not None else None
            
#             result_data.append(row_data)
    
#     # Create the result DataFrame
#     spark = SparkSession.getActiveSession()
    
#     # Define schema - dataframe_metric_cohort + all percentile columns
#     schema_fields = [StructField("dataframe_metric_cohort", StringType(), True),
#                      StructField("record_count", IntegerType(), True)]
#     for percentage in quantile_percentages:
#         schema_fields.append(StructField(f"p{percentage}", DoubleType(), True))
    
#     schema = StructType(schema_fields)
    
#     # Create DataFrame from result data
#     if result_data:
#         result_df = spark.createDataFrame(result_data, schema)
#         return result_df
#     else:
#         # Return empty DataFrame with schema if no data
#         return spark.createDataFrame([], schema)
    


def calculate_quantiles_with_cohorts_opt(df_metric_mapping: Dict[DataFrame, str], 
                                   cohort_column: str,
                                   quantiles_range: tuple = (5, 15),
                                   interval: int = 5,
                                   df_names: List[str] = None) -> DataFrame:
    """
    Calculate quantiles for different metrics across multiple DataFrames by cohort groups.
    Returns DataFrame with percentiles as columns and dataframe_metric_cohort as rows.
    
    OPTIMIZED VERSION: Uses groupBy instead of filtering each cohort individually.
    This reduces N separate Spark jobs to 1 job per DataFrame.
    
    Parameters:
    -----------
    df_metric_mapping : Dict[DataFrame, str]
        Dictionary with DataFrames as keys and metric column names as values
        Example: {df1: 'sales', df2: 'cost', df3: 'revenue'}
    cohort_column : str
        Column name to group by (e.g., 'category', 'segment', 'region')
    quantiles_range : tuple, default (30, 95)
        Start and end percentiles (30th to 95th percentile)
    interval : int, default 5
        Interval between quantiles (every 5%)
    df_names : List[str], optional
        List of names for DataFrames (for labeling). If None, uses df1, df2, etc.
    
    Returns:
    --------
    DataFrame
        DataFrame with rows as 'dataframe_metric_cohort' and columns as percentile values
        Example rows: sales_data_sales_high, sales_data_sales_medium, cost_data_cost_premium
    """
    
    # Generate quantile percentages (30, 35, 40, ..., 95)
    start_pct, end_pct = quantiles_range
    quantile_percentages = list(range(start_pct, end_pct + interval, interval))
    quantile_decimals = [q/100.0 for q in quantile_percentages]
    
    # Initialize list to collect result DataFrames
    result_dfs = []
    
    # Generate DataFrame names if not provided
    if df_names is None:
        df_names = [f'df{i+1}' for i in range(len(df_metric_mapping))]
    
    # Process each DataFrame and its corresponding metric
    for i, (df, metric_column) in enumerate(df_metric_mapping.items()):
        
        df_name = df_names[i] if i < len(df_names) else f'df{i+1}'
        
        # Check if metric column and cohort column exist
        if metric_column not in df.columns:
            print(f"Warning: Column '{metric_column}' not found in DataFrame '{df_name}'. Skipping...")
            continue
            
        if cohort_column not in df.columns:
            print(f"Warning: Cohort column '{cohort_column}' not found in DataFrame '{df_name}'. Skipping...")
            continue
        
        # OPTIMIZATION: Calculate all quantiles for all cohorts in ONE operation
        quantile_exprs = []
        
        for j, decimal in enumerate(quantile_decimals):
            quantile_exprs.append(
                percentile_approx(col(metric_column), decimal).alias(f'p{quantile_percentages[j]}')
            )
        
        # Single groupBy operation for all cohorts
        cohort_quantiles = df.groupBy(cohort_column).agg(*quantile_exprs)
        
        # Add the dataframe_metric_cohort column
        cohort_quantiles = cohort_quantiles.withColumn(
            "dataframe_metric_cohort",
            F.concat(lit(f"{metric_column}_"), col(cohort_column))
        )
        
        # Select and reorder columns
        column_order = ["dataframe_metric_cohort"] + [f'p{p}' for p in quantile_percentages]
        cohort_quantiles = cohort_quantiles.select(*column_order)
        
        result_dfs.append(cohort_quantiles)
    
    # Combine all results using union
    if result_dfs:
        final_result = result_dfs[0]
        for df in result_dfs[1:]:
            final_result = final_result.union(df)
        return final_result
    else:
        # Return empty DataFrame with schema if no data
        spark = SparkSession.getActiveSession()
        schema_fields = [StructField("dataframe_metric_cohort", StringType(), True)]
        for percentage in quantile_percentages:
            schema_fields.append(StructField(f"p{percentage}", DoubleType(), True))
        
        schema = StructType(schema_fields)
        return spark.createDataFrame([], schema)
