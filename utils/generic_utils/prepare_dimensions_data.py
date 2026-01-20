import re
from pyspark.sql.functions import col,when, lit
from pyspark.sql import DataFrame



def prepare_dimension_data_generic(df: DataFrame, dimension_cols: list) -> DataFrame:
    """
    Prepares dimension data by selecting columns, dropping duplicates, mapping indicator values,
    and filling missing/null values with 'No <Column Name>' for all columns.

    Args:
        df (DataFrame): Input raw PySpark DataFrame.
        dimension_cols (list): List of columns to select from raw data.

    Returns:
        DataFrame: Processed dimension DataFrame.
    """
    
    #Select columns and drop duplicates
    dim_df = df.select(dimension_cols).dropDuplicates()
    # dim_df = df.select(dimension_cols)
    
    # Map indicator columns
    indicator_mappings = {
        "Kosher Indicator": {1: "Kosher", 0: "Non - Kosher"},
        "Organic Indicator": {1: "Organic", 0: "Non - Organic"},
        "Non-GMO Indicator": {1: "Non - GMO", 0: "GMO"},
        "Lactose Free Indicator": {1: "Lactose - Free", 0: "Contains Lactose"}
    }
    
    for col_name, mapping in indicator_mappings.items():
        if col_name in dim_df.columns:
            dim_df = dim_df.withColumn(
                col_name,
                when(col(col_name) == 1, mapping[1])
                .when(col(col_name) == 0, mapping[0])
                .otherwise("Other")
            )
    
    # Fill missing/null values for **all columns**
    for c in dim_df.columns:
        dim_df = dim_df.withColumn(c, when(col(c).isNull(), lit(f"No {c}")).otherwise(col(c)))
    
    return dim_df