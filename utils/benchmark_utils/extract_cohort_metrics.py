from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, split, when, size, array_join, slice, concat, lit, regexp_replace

def transform_dataframe(df: DataFrame) -> DataFrame:
    """
    Transforms dataframe by splitting the 'dataframe_metric_cohort_ordertype' column
    into 'Metric', 'cost format', and 'cohort' columns.
   
    Logic:
    1. Detect cost format indicator (_per_lb or _pct_sales) in the part before pipe
    2. Metric: everything before the cost format indicator
    3. Cost format: $/lb for _per_lb, % Sales for _pct_sales, Actual $ otherwise
    4. Cohort: everything after the cost format indicator (including pipe and what follows)
   
    Args:
        df: Input DataFrame with 'dataframe_metric_cohort_ordertype' column
       
    Returns:
        Transformed DataFrame with split columns
    """
   
    # Check if contains cost format indicators
    df_transformed = df.withColumn(
        "has_per_lb",
        col("dataframe_metric_cohort_ordertype").contains("_per_lb_")
    ).withColumn(
        "has_pct_sales",
        col("dataframe_metric_cohort_ordertype").contains("_pct_sales_")
    )
   
    # Determine cost format
    df_transformed = df_transformed.withColumn(
        "cost format",
        when(col("has_per_lb"), "$/lb")
        .when(col("has_pct_sales"), "% Sales")
        .otherwise("Actual $")
    )
   
    # Extract Metric and Cohort based on cost format
    df_transformed = df_transformed.withColumn(
        "Metric",
        when(col("has_per_lb"),
             regexp_replace(
                 split(col("dataframe_metric_cohort_ordertype"), "_per_lb_")[0],
                 "^(.*)$", "$1"
             )
        ).when(col("has_pct_sales"),
             regexp_replace(
                 split(col("dataframe_metric_cohort_ordertype"), "_pct_sales_")[0],
                 "^(.*)$", "$1"
             )
        ).otherwise(
             # For Actual $: everything before the last underscore before pipe
             regexp_replace(
                 col("dataframe_metric_cohort_ordertype"),
                 "^(.+)_([^_]+\\s*\\|.*)$", "$1"
             )
        )
    ).withColumn(
        "cohort",
        when(col("has_per_lb"),
             regexp_replace(
                 col("dataframe_metric_cohort_ordertype"),
                 "^.+_per_lb_(.*)$", "$1"
             )
        ).when(col("has_pct_sales"),
             regexp_replace(
                 col("dataframe_metric_cohort_ordertype"),
                 "^.+_pct_sales_(.*)$", "$1"
             )
        ).otherwise(
             # For Actual $: everything after the last underscore before pipe
             regexp_replace(
                 col("dataframe_metric_cohort_ordertype"),
                 "^.+_([^_]+\\s*\\|.*)$", "$1"
             )
        )
    )
   
    # Remove underscore and everything after it from cohort
    df_transformed = df_transformed.withColumn(
        "cohort",
        regexp_replace(col("cohort"), "_.*$", "")
    )
   
    # Clean up and reorder columns
    other_cols = [c for c in df.columns if c != "dataframe_metric_cohort_ordertype"]
    output_cols = ["dataframe_metric_cohort_ordertype", "Metric", "cost format", "cohort"] + other_cols
   
    df_transformed = df_transformed.drop("has_per_lb", "has_pct_sales")
   
    return df_transformed.select(output_cols)
 