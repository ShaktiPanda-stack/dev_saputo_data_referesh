from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Dict
from pyspark.sql.functions import col,when, lit,percentile_approx,udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import re


def find_nearest_percentile_benchmark(
    overall_percentile_df: DataFrame,
    cohort_percentile_df: DataFrame,
    target_percentile: str = 'p10',
    metric_column: str = 'Metric'
) -> DataFrame:
    """
    Compare cohort-based percentiles with overall percentiles and find the nearest match.
    
    Parameters:
    -----------
    overall_percentile_df : DataFrame
        DataFrame with overall percentiles (columns: Metric, p5, p10, p15, ..., p95)
    cohort_percentile_df : DataFrame
        DataFrame with cohort-based percentiles (columns: Metric + Cohort, p75, p80, p85, p90, p95)
    target_percentile : str, default 'p90'
        The percentile column from cohort_percentile_df to compare (e.g., 'p90', 'p95')
    metric_column : str, default 'Metric'
        The name of the metric identifier column in overall_percentile_df
    
    Returns:
    --------
    DataFrame
        Cohort percentile DataFrame with two additional columns:
        - Final Percentile: The nearest percentile from overall distribution
        - Final Benchmark: The benchmark value at that percentile
    """
    
    # Step 1: Extract the base metric name from cohort metric column
    # Pattern: "TotalCOGS_dollar_per_lb_High | Aerosol" -> extract "TotalCOGS_dollar_per_lb"
    def extract_base_metric(metric_cohort_str):
        """Extract base metric name before the underscore and cohort indicator"""
        if metric_cohort_str is None:
            return None
        # Split by ' | ' to separate metric from cohort
        parts = metric_cohort_str.split(' | ')
        if len(parts) > 0:
            metric_part = parts[0]
            # Remove the cohort indicator (e.g., "_High", "_Medium", "_Low")
            # Match pattern: remove last underscore and word after it
            base_metric = re.sub(r'_[^_]+$', '', metric_part)
            return base_metric
        return metric_cohort_str
    
    extract_base_metric_udf = udf(extract_base_metric, StringType())
    
    # Step 2: Get percentile columns from overall_percentile_df
    # Identify columns that start with 'p' and are followed by numbers
    percentile_columns = [c for c in overall_percentile_df.columns 
                          if c.startswith('p') and c[1:].isdigit()]
    percentile_columns.sort(key=lambda x: int(x[1:]))  # Sort by percentile number
    
    # Step 3: Validate target_percentile exists in cohort_percentile_df
    if target_percentile not in cohort_percentile_df.columns:
        raise ValueError(f"Target percentile '{target_percentile}' not found in cohort_percentile_df")
    
    # Step 4: Collect overall percentiles as a lookup dictionary
    # Structure: {metric: {p5: value, p10: value, ...}}
    overall_data = overall_percentile_df.collect()
    overall_lookup = {}
    
    for row in overall_data:
        metric_name = row[metric_column]
        percentile_values = {}
        for pc in percentile_columns:
            percentile_values[pc] = float(row[pc]) if row[pc] is not None else None
        overall_lookup[metric_name] = percentile_values
    
    # Step 5: Define UDF to find nearest percentile
    def find_nearest(base_metric, target_value):
        """
        Find the nearest percentile in overall distribution for given target value.
        Returns tuple: (percentile_name, percentile_value)
        """
        if base_metric not in overall_lookup or target_value is None:
            return (None, None)
        
        percentile_dict = overall_lookup[base_metric]
        min_diff = float('inf')
        nearest_percentile = None
        nearest_value = None
        
        for pc, val in percentile_dict.items():
            if val is not None:
                # diff = abs(float(val) - float(target_value))
                diff = val - float(target_value)
                if diff < 0:
                    diff = -diff

                if diff < min_diff:
                    min_diff = diff
                    nearest_percentile = pc
                    nearest_value = val
        
        return (nearest_percentile, nearest_value)
    
    # Create UDFs for percentile name and value
    find_nearest_percentile_udf = udf(
        lambda base_metric, target_value: find_nearest(base_metric, target_value)[0],
        StringType()
    )
    
    find_nearest_value_udf = udf(
        lambda base_metric, target_value: find_nearest(base_metric, target_value)[1],
        DoubleType()
    )
    
    # Step 6: Get the first column name (assuming it's the metric+cohort column)
    cohort_metric_column = cohort_percentile_df.columns[0]
    
    # Step 7: Add base metric extraction and nearest percentile matching
    result_df = cohort_percentile_df.withColumn(
        'base_metric',
        extract_base_metric_udf(col(cohort_metric_column))
    ).withColumn(
        'Final Percentile',
        find_nearest_percentile_udf(col('base_metric'), col(target_percentile))
    ).withColumn(
        'Final Benchmark',
        find_nearest_value_udf(col('base_metric'), col(target_percentile))
    ).drop('base_metric')  # Drop the intermediate column
    
    return result_df