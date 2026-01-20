from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Dict, Union
from pyspark.sql.functions import col,when, lit,percentile_approx,udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import re


def find_nearest_percentile_benchmark(
    overall_percentile_df: DataFrame,
    cohort_percentile_df: DataFrame,
    target_percentile_mapping: Union[str, Dict[str, str]] = 'p10',
    overall_metric_column: str = 'dataframe_metric',
    overall_order_type_column: str = 'order_type'
) -> DataFrame:
    """
    Compare cohort-based percentiles with overall percentiles (by order type) and find the nearest match.
   
    Parameters:
    -----------
    overall_percentile_df : DataFrame
        DataFrame with overall percentiles by order type
        Columns: dataframe_metric, order_type, p5, p10, p15, ..., p95
    cohort_percentile_df : DataFrame
        DataFrame with cohort-based percentiles (combined column format)
        Columns: dataframe_metric_cohort_ordertype, p5, p10, ..., p95
        Example values: "TotalCOGS_dollar_per_lb_High_New", "TotalCOGS_dollar_per_lb_Low_Existing"
    target_percentile_mapping : Union[str, Dict[str, str]], default 'p10'
        Either:
        - A single percentile string (e.g., 'p10') to use for all order types
        - A dictionary mapping order types to percentiles (e.g., {'New': 'p10', 'Existing': 'p15'})
    overall_metric_column : str, default 'dataframe_metric'
        The name of the metric column in overall_percentile_df
    overall_order_type_column : str, default 'order_type'
        The name of the order type column in overall_percentile_df
   
    Returns:
    --------
    DataFrame
        Cohort percentile DataFrame with two additional columns:
        - Final Percentile: The nearest percentile from overall distribution
        - Final Benchmark: The benchmark value at that percentile
    """
    from pyspark.sql import functions as F
   
    # Step 1: Get percentile columns
    percentile_columns = [c for c in overall_percentile_df.columns
                          if c.startswith('p') and c[1:].isdigit()]
    percentile_columns.sort(key=lambda x: int(x[1:]))
   
    # Step 2: Validate target_percentile_mapping
    if isinstance(target_percentile_mapping, str):
        if target_percentile_mapping not in cohort_percentile_df.columns:
            raise ValueError(f"Target percentile '{target_percentile_mapping}' not found in cohort_percentile_df")
        percentile_map = {}
        default_percentile = target_percentile_mapping
    elif isinstance(target_percentile_mapping, dict):
        for ot, perc in target_percentile_mapping.items():
            if perc not in cohort_percentile_df.columns:
                raise ValueError(f"Percentile '{perc}' for order type '{ot}' not found in cohort_percentile_df")
        percentile_map = target_percentile_mapping
        default_percentile = list(target_percentile_mapping.values())[0] if target_percentile_mapping else 'p10'
    else:
        raise ValueError("target_percentile_mapping must be either a string or a dictionary")
   
    # Step 3: Collect overall percentiles as a lookup dictionary
    # Structure: {(metric, order_type): {p5: value, p10: value, ...}}
    overall_data = overall_percentile_df.collect()
    overall_lookup = {}
   
    for row in overall_data:
        metric_name = row[overall_metric_column]
        order_type = row[overall_order_type_column]
        key = (metric_name, order_type)
       
        percentile_values = {}
        for pc in percentile_columns:
            percentile_values[pc] = float(row[pc]) if row[pc] is not None else None
        overall_lookup[key] = percentile_values
   
    # Step 4: Parse the combined column
    # Expected format: "metric_cohort_ordertype"
    # Example: "TotalCOGS_dollar_per_lb_High_New" -> metric="TotalCOGS_dollar_per_lb", cohort="High", order_type="New"
    cohort_metric_column = cohort_percentile_df.columns[0]
   
    # Use regex to split: capture everything before last two underscores
    # Pattern: (.*?)_([^_]+)_([^_]+)$ means: (anything)_(word)_(word)
    split_pattern = r'^(.+?)_([^_]+)_([^_]+)$'
   
    # Apply regex split - this captures: metric, cohort, order_type
    working_df = cohort_percentile_df.withColumn(
        'base_metric',
        F.regexp_extract(F.col(cohort_metric_column), split_pattern, 1)
    ).withColumn(
        'cohort',
        F.regexp_extract(F.col(cohort_metric_column), split_pattern, 2)
    ).withColumn(
        'order_type_extracted',
        F.regexp_extract(F.col(cohort_metric_column), split_pattern, 3)
    )
   
    # Step 5: Determine which percentile column to use based on order type
    # Build a CASE expression to select the right percentile column
    if percentile_map:
        # Build CASE WHEN for each order type
        case_expr = None
        for order_type_val, perc_col in percentile_map.items():
            if case_expr is None:
                case_expr = F.when(F.col('order_type_extracted') == order_type_val, F.col(perc_col))
            else:
                case_expr = case_expr.when(F.col('order_type_extracted') == order_type_val, F.col(perc_col))
        # Add default
        case_expr = case_expr.otherwise(F.col(default_percentile))
        working_df = working_df.withColumn('target_value', case_expr)
    else:
        # Use single percentile for all
        working_df = working_df.withColumn('target_value', F.col(default_percentile))
   
    # Step 6: Find nearest percentile - using UDF with broadcast
    def find_nearest(base_metric, order_type, target_value):
        """Find the nearest percentile in overall distribution"""
        if target_value is None or base_metric is None or order_type is None:
            return (None, None)
       
        key = (base_metric, order_type)
        if key not in overall_lookup:
            return (None, None)
       
        percentile_dict = overall_lookup[key]
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
       
        # return (nearest_percentile, nearest_value)
        return (
                    str(nearest_percentile) if nearest_percentile is not None else None,
                    float(nearest_value) if nearest_value is not None else None
                )
   
   
   
    find_nearest_udf = F.udf(
        lambda bm, ot, tv: find_nearest(bm, ot, tv),
        StructType([
            StructField("percentile", StringType(), True),
            StructField("value", DoubleType(), True)
        ])
    )
   
    result_df = working_df.withColumn(
        'nearest_result',
        find_nearest_udf(F.col('base_metric'), F.col('order_type_extracted'), F.col('target_value'))
    ).withColumn(
        'Final Percentile',
        F.col('nearest_result.percentile')
    ).withColumn(
        'Final Benchmark',
        F.col('nearest_result.value')
    )
   
    # Step 7: Clean up intermediate columns and drop old Final columns if they exist
    cols_to_drop = ['base_metric', 'cohort', 'order_type_extracted', 'target_value', 'nearest_result']
   
    # Drop columns that exist
    for col_name in cols_to_drop:
        if col_name in result_df.columns:
            result_df = result_df.drop(col_name)
   
    return result_df



# from pyspark.sql import DataFrame
# from pyspark.sql import functions as F
# from pyspark.sql.window import Window
# from pyspark.sql.functions import col, lit, udf
# from pyspark.sql.types import StringType, DoubleType
# import re


def find_nearest_percentile_benchmark_p90   (
    overall_percentile_df: DataFrame,
    cohort_percentile_df: DataFrame,
    metric_column: str = 'Metric'
) -> DataFrame:
    """
    Compare cohort-based percentiles with overall percentiles and find the nearest match for p90.

    Parameters:
    -----------
    overall_percentile_df : DataFrame
        DataFrame with overall percentiles (Metric, p5, p10, ... p95)
    cohort_percentile_df : DataFrame
        DataFrame with cohort-based percentiles (Metric + Cohort, p75, p80, p85, p90, p95)
    metric_column : str
        The metric identifier column in overall_percentile_df

    Returns:
    --------
    DataFrame
        Cohort percentile DataFrame with:
        - Final Percentile p90: nearest overall percentile
        - Final Benchmark p90: benchmark value at that percentile
    """

    target_percentile = 'p90'

    # Extract base metric name
    def extract_base_metric(metric_cohort_str):
        if metric_cohort_str is None:
            return None
        parts = metric_cohort_str.split(' | ')
        if parts:
            base = re.sub(r'_[^_]+$', '', parts[0])
            return base
        return metric_cohort_str

    extract_base_metric_udf = udf(extract_base_metric, StringType())

    # Get percentile columns
    percentile_columns = [c for c in overall_percentile_df.columns if c.startswith('p') and c[1:].isdigit()]
    percentile_columns.sort(key=lambda x: int(x[1:]))

    if target_percentile not in cohort_percentile_df.columns:
        raise ValueError(f"Target percentile '{target_percentile}' not found in cohort_percentile_df.")

    # Create lookup dictionary
    overall_lookup = {}
    for row in overall_percentile_df.collect():
        metric = row[metric_column]
        overall_lookup[metric] = {
            pc: float(row[pc]) if row[pc] is not None else None for pc in percentile_columns
        }

    # UDF logic to find closest percentile
    def find_nearest(base_metric, target_val):
        if base_metric not in overall_lookup or target_val is None:
            return (None, None)

        percentiles = overall_lookup[base_metric]
        min_diff = float('inf')
        nearest_pc = None
        nearest_val = None

        for pc, val in percentiles.items():
            if val is not None:
                diff = abs(val - float(target_val))
                if diff < min_diff:
                    min_diff = diff
                    nearest_pc = pc
                    nearest_val = val

        return (nearest_pc, nearest_val)

    find_percentile_udf = udf(
        lambda base_metric, value: find_nearest(base_metric, value)[0],
        StringType()
    )

    find_value_udf = udf(
        lambda base_metric, value: find_nearest(base_metric, value)[1],
        DoubleType()
    )

    cohort_metric_col = cohort_percentile_df.columns[0]

    result_df = cohort_percentile_df.withColumn(
        "base_metric", extract_base_metric_udf(col(cohort_metric_col))
    ).withColumn(
        "Final Percentile p90", find_percentile_udf(col("base_metric"), col(target_percentile))
    ).withColumn(
        "Final Benchmark p90", find_value_udf(col("base_metric"), col(target_percentile))
    ).drop("base_metric")

    return result_df
