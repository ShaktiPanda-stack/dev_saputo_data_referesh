from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, sum as _sum, year, month, 
    lag, when, row_number, lit, concat_ws, date_trunc, abs as abs_, date_format, substring
)
from typing import List

def analyze_cost_metrics(
    df: DataFrame,
    cost_metrics: List[str],
    dimensions: List[str],
    date_column: str = "transaction_date"
) -> DataFrame:
    """
    Analyze transactional data to identify worsening performance and worst months.
    Works with the full scope of data provided (no fiscal year filtering).
    
    Parameters:
    -----------
    df : DataFrame
        Input transactional data with date column and cost metrics
    cost_metrics : List[str]
        List of cost metric column names to analyze
    dimensions : List[str]
        List of dimension columns to group by
    date_column : str
        Name of the date column (default: "transaction_date")
    
    Returns:
    --------
    DataFrame with columns:
        - All dimension columns
        - months_worsening_perf_{metric_name}: Count of months with increasing costs for each metric
        - worst_month_{metric_name}: Month-Year with highest cost (format: MMM 'YY) for each metric
        - worst_month_{metric_name}_value: The maximum cost value for each metric
    """
    
    # Truncate date to month level and extract year/month
    df_monthly = df.withColumn("month_start", date_trunc("month", col(date_column))) \
                   .withColumn("year", year(col(date_column))) \
                   .withColumn("month", month(col(date_column)))
    
    # Process each cost metric
    result_dfs = []
    
    for metric in cost_metrics:
        # Aggregate by dimensions and month
        monthly_agg = df_monthly.groupBy(dimensions + ["month_start", "year", "month"]) \
                                .agg(_sum(col(metric)).alias("total_cost"))
        
        # Define window for calculating month-over-month changes
        window_spec = Window.partitionBy(dimensions).orderBy("month_start")
        
        # Calculate previous month's cost
        monthly_agg = monthly_agg.withColumn(
            "prev_month_cost",
            lag("total_cost", 1).over(window_spec)
        )
        
        # Identify months with worsening performance (increasing costs)
        monthly_agg = monthly_agg.withColumn(
            "is_worsening",
            when(
                (col("prev_month_cost").isNotNull()) & 
                (abs_(col("total_cost")) > abs_(col("prev_month_cost"))),
                1
            ).otherwise(0)
        )
        
        # Calculate months worsening performance
        worsening_count = monthly_agg.groupBy(dimensions) \
                                     .agg(_sum("is_worsening").alias("months_worsening_performance"))
        
        # Find worst month (month with highest cost)
        window_max = Window.partitionBy(dimensions).orderBy(col("total_cost"))
        
        # Create month-year string in "MMM 'YY" format - CHANGED
        from pyspark.sql.functions import date_format, substring
        
        monthly_agg = monthly_agg.withColumn(
            "month_year",
            concat_ws(" '", 
                     date_format(col("month_start"), "MMM"),
                     substring(col("year").cast("string"), 3, 2)
            )
        )
        
        worst_month_df = monthly_agg.withColumn("rank", row_number().over(window_max)) \
                                    .filter(col("rank") == 1) \
                                    .select(
                                        *dimensions,
                                        col("month_year").alias("worst_month_year"),
                                        col("total_cost").alias("worst_month_value")
                                    )
        
        # Rename columns with metric suffix - CHANGED
        metric_result = worsening_count.join(worst_month_df, on=dimensions, how="inner") \
                                       .withColumnRenamed("months_worsening_performance", f"months_worsening_perf_{metric}") \
                                       .withColumnRenamed("worst_month_year", f"worst_month_{metric}") \
                                       .withColumnRenamed("worst_month_value", f"worst_month_{metric}_value")
        
        result_dfs.append(metric_result)
    
    # Join all metric results (instead of union) - CHANGED
    final_result = result_dfs[0]
    for i in range(1, len(result_dfs)):
        final_result = final_result.join(result_dfs[i], on=dimensions, how="outer")
    
    return final_result


def analyze_best_months(
    df: DataFrame,
    cost_metrics: List[str],
    dimensions: List[str],
    date_column: str = "transaction_date"
) -> DataFrame:
    """
    Analyze transactional data to identify improving performance and best months.
    This is the exact opposite of analyze_cost_metrics().
    
    For each metric, creates:
        - months_improving_perf_<metric>: Count of months where cost DECREASED
        - best_month_<metric>: Month-Year with LOWEST cost
        - best_month_<metric>_value: Minimum monthly cost
    """

    # Truncate date to month level and extract year/month
    df_monthly = df.withColumn("month_start", date_trunc("month", col(date_column))) \
                   .withColumn("year", year(col(date_column))) \
                   .withColumn("month", month(col(date_column)))

    result_dfs = []

    for metric in cost_metrics:
        # Aggregate by dimensions and month
        monthly_agg = df_monthly.groupBy(dimensions + ["month_start", "year", "month"]) \
                                .agg(_sum(col(metric)).alias("total_cost"))

        # Window for month-over-month
        window_spec = Window.partitionBy(dimensions).orderBy("month_start")

        # Previous month value
        monthly_agg = monthly_agg.withColumn(
            "prev_month_cost",
            lag("total_cost", 1).over(window_spec)
        )

        # Identify improving months (cost DECREASES)
        monthly_agg = monthly_agg.withColumn(
            "is_improving",
            when(
                (col("prev_month_cost").isNotNull()) &
                (abs_(col("total_cost")) < abs_(col("prev_month_cost"))),
                1
            ).otherwise(0)
        )

        # Count improving months
        improving_count = monthly_agg.groupBy(dimensions) \
                                     .agg(_sum("is_improving").alias("months_improving_performance"))

        # Create month-year format: "MMM 'YY"
        monthly_agg = monthly_agg.withColumn(
            "month_year",
            concat_ws(" '",
                date_format(col("month_start"), "MMM"),
                substring(col("year").cast("string"), 3, 2)
            )
        )

        # Window for BEST month (minimum cost)
        window_min = Window.partitionBy(dimensions).orderBy(col("total_cost").asc())

        best_month_df = monthly_agg.withColumn("rank", row_number().over(window_min)) \
                                   .filter(col("rank") == 1) \
                                   .select(
                                        *dimensions,
                                        col("month_year").alias("best_month_year"),
                                        col("total_cost").alias("best_month_value")
                                   )

        # Add metric name as suffix
        metric_result = improving_count.join(best_month_df, on=dimensions, how="inner") \
                                       .withColumnRenamed("months_improving_performance", f"months_improving_perf_{metric}") \
                                       .withColumnRenamed("best_month_year", f"best_month_{metric}") \
                                       .withColumnRenamed("best_month_value", f"best_month_{metric}_value")

        result_dfs.append(metric_result)

    # Combine output for all metrics
    final_result = result_dfs[0]
    for i in range(1, len(result_dfs)):
        final_result = final_result.join(result_dfs[i], on=dimensions, how="outer")

    return final_result






