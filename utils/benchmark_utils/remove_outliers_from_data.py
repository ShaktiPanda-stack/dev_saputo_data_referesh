from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Dict
from pyspark.sql.functions import col,when, lit,percentile_approx


def remove_outliers_by_metric(df: DataFrame, 

                             metric_columns: List[str], 
                             method: str = 'iqr',
                             factor: float = 1.5) -> Dict[str, DataFrame]:
    """
    Generic function to remove outliers for each metric separately.
    Returns separate DataFrame for each metric after outlier treatment.
    
    Parameters:
    -----------
    df : DataFrame
        Input PySpark DataFrame
    metric_columns : List[str]
        List of numeric columns to check for outliers
    method : str, default 'iqr'
        Method for outlier detection ('iqr' or 'percentile')
    factor : float, default 1.5
        Multiplier for outlier bounds (1.5 for IQR)
    
    Returns:
    --------
    Dict[str, DataFrame]
        Dictionary with metric names as keys and cleaned DataFrames as values
    """
    
    result_dfs = {}
    
    for column in metric_columns:
        
        if method == 'iqr':
            # Calculate Q1, Q3, and IQR for this specific metric
            quantiles = df.select(
                percentile_approx(col(column), 0.25).alias('q1'),
                percentile_approx(col(column), 0.75).alias('q3')
            ).collect()[0]
            
            q1 = float(quantiles['q1'])
            q3 = float(quantiles['q3'])
            iqr = q3 - q1
            
            # Define bounds
            lower_bound = q1 - (factor * iqr)
            upper_bound = q3 + (factor * iqr)
            
            # Filter out outliers for this metric only
            clean_df = df.filter(
                (col(column) >= lower_bound) & 
                (col(column) <= upper_bound)
            )
        
        elif method == 'percentile':
            # Calculate 5th and 95th percentiles for this metric
            bounds = df.select(
                percentile_approx(col(column), 0.05).alias('p5'),
                percentile_approx(col(column), 0.95).alias('p95')
            ).collect()[0]
            
            # Filter out outliers for this metric only
            clean_df = df.filter(
                (col(column) >= bounds['p5']) & 
                (col(column) <= bounds['p95'])
            )
        
        # Store the cleaned DataFrame for this metric
        result_dfs[column] = clean_df
    
    return result_dfs


def remove_outliers_by_metric_cohort(df: DataFrame, 
                             metric_columns: List[str],
                             cohort_column: str,
                             method: str = 'iqr',
                             factor: float = 1.5) -> Dict[str, DataFrame]:
    """
    Generic function to remove outliers for each metric separately within each cohort.
    Returns separate DataFrame for each metric after outlier treatment.
    
    Parameters:
    -----------
    df : DataFrame
        Input PySpark DataFrame
    metric_columns : List[str]
        List of numeric columns to check for outliers
    cohort_column : str
        Column name to group by for cohort-level outlier detection
    method : str, default 'iqr'
        Method for outlier detection ('iqr' or 'percentile')
    factor : float, default 1.5
        Multiplier for outlier bounds (1.5 for IQR)
    
    Returns:
    --------
    Dict[str, DataFrame]
        Dictionary with metric names as keys and cleaned DataFrames as values
    """
    
    result_dfs = {}
    
    for column in metric_columns:
        
        if method == 'iqr':
            # Calculate Q1, Q3, and IQR for this specific metric within each cohort
            cohort_bounds = df.groupBy(cohort_column).agg(
                percentile_approx(col(column), 0.25).alias('q1'),
                percentile_approx(col(column), 0.75).alias('q3')
            ).withColumn('iqr', col('q3') - col('q1')) \
             .withColumn('lower_bound', col('q1') - (lit(factor) * col('iqr'))) \
             .withColumn('upper_bound', col('q3') + (lit(factor) * col('iqr'))) \
             .select(cohort_column, 'lower_bound', 'upper_bound')
            
            # Join bounds back to original data and filter outliers
            clean_df = df.join(cohort_bounds, on=cohort_column, how='inner') \
                         .filter((col(column) >= col('lower_bound')) & 
                                (col(column) <= col('upper_bound'))) \
                         .drop('lower_bound', 'upper_bound')
        
        elif method == 'percentile':
            # Calculate 5th and 95th percentiles for this metric within each cohort
            cohort_bounds = df.groupBy(cohort_column).agg(
                percentile_approx(col(column), 0.05).alias('p5'),
                percentile_approx(col(column), 0.95).alias('p95')
            ).select(cohort_column, col('p5').alias('lower_bound'), col('p95').alias('upper_bound'))
            
            # Join bounds back to original data and filter outliers
            clean_df = df.join(cohort_bounds, on=cohort_column, how='inner') \
                         .filter((col(column) >= col('lower_bound')) & 
                                (col(column) <= col('upper_bound'))) \
                         .drop('lower_bound', 'upper_bound')
        
        # Store the cleaned DataFrame for this metric
        result_dfs[column] = clean_df
    
    return result_dfs
