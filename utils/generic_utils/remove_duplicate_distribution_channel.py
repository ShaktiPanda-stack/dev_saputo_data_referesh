from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def get_grouped_counts(df: DataFrame) -> DataFrame:
    """
    Group by L3 Customer Name, IBP Group 2 Text, and Distribution Channel Text 
    and return record counts for each combination.
    
    Args:
        df: Input PySpark DataFrame
        
    Returns:
        DataFrame with grouping columns and count
    """
    grouped_df = df.groupBy(
        "L3 Customer Name", 
        "IBP Group 2 Text", 
        "Distribution Channel Text"
    ).agg(
        F.count("*").alias("record_count")
    ).orderBy("L3 Customer Name", "IBP Group 2 Text", "Distribution Channel Text")
    
    return grouped_df


def replace_lower_frequency_channels(df: DataFrame) -> DataFrame:
    """
    Replace lower occurring distribution channels with higher frequency ones
    for each L3 Customer Name and IBP Group 2 Text combination.
    
    This ensures each (L3 Customer Name, IBP Group 2 Text) pair has only one 
    unique Distribution Channel Text.
    
    Args:
        df: Input PySpark DataFrame
        
    Returns:
        DataFrame with standardized distribution channels
    """
    # Step 1: Count frequency of each distribution channel per (L3 Customer Name, IBP Group 2 Text)
    channel_freq = df.groupBy(
        "L3 Customer Name", 
        "IBP Group 2 Text", 
        "Distribution Channel Text"
    ).agg(
        F.count("*").alias("channel_frequency")
    )
    
    # Step 2: Find the most frequent distribution channel for each (L3 Customer Name, IBP Group 2 Text)
    window_spec = Window.partitionBy("L3 Customer Name", "IBP Group 2 Text").orderBy(
        F.desc("channel_frequency"),
        F.asc("Distribution Channel Text")  # Tie-breaker for equal frequencies
    )
    
    most_frequent_channel = channel_freq.withColumn(
        "rank", 
        F.row_number().over(window_spec)
    ).filter(
        F.col("rank") == 1
    ).select(
        "L3 Customer Name",
        "IBP Group 2 Text",
        F.col("Distribution Channel Text").alias("primary_channel")
    )
    
    # Step 3: Join back to original dataframe and replace Distribution Channel Text
    result_df = df.join(
        most_frequent_channel,
        on=["L3 Customer Name", "IBP Group 2 Text"],
        how="left"
    ).withColumn(
        "Distribution Channel Text",
        F.col("primary_channel")
    ).drop("primary_channel")
    
    return result_df


def deduplicate_distribution_channels(df: DataFrame, 
                                       show_before_stats: bool = True,
                                       show_after_stats: bool = True) -> DataFrame:
    """
    Complete pipeline to deduplicate distribution channels.
    
    Args:
        df: Input PySpark DataFrame
        show_before_stats: Whether to display statistics before deduplication
        show_after_stats: Whether to display statistics after deduplication
        
    Returns:
        DataFrame with deduplicated distribution channels
    """
    if show_before_stats:
        print("=== BEFORE DEDUPLICATION ===")
        before_stats = get_grouped_counts(df)
        # before_stats.show(100, truncate=False)
        
        # Show how many distribution channels per (L3 Customer Name, IBP Group 2 Text)
        duplicates = df.groupBy("L3 Customer Name", "IBP Group 2 Text").agg(
            F.countDistinct("Distribution Channel Text").alias("num_channels")
        ).filter(F.col("num_channels") > 1)
        
        print(f"\nNumber of (L3 Customer Name, IBP Group 2 Text) pairs with multiple channels: {duplicates.count()}")
        if duplicates.count() > 0:
            print("\nPairs with multiple distribution channels:")
            duplicates.show(20, truncate=False)
    
    # Perform deduplication
    result_df = replace_lower_frequency_channels(df)
    
    if show_after_stats:
        print("\n=== AFTER DEDUPLICATION ===")
        after_stats = get_grouped_counts(result_df)
        # after_stats.show(100, truncate=False)
        
        # Verify uniqueness
        duplicates_after = result_df.groupBy("L3 Customer Name", "IBP Group 2 Text").agg(
            F.countDistinct("Distribution Channel Text").alias("num_channels")
        ).filter(F.col("num_channels") > 1)
        
        print(f"\nNumber of (L3 Customer Name, IBP Group 2 Text) pairs with multiple channels: {duplicates_after.count()}")
        print("✓ Each (L3 Customer Name, IBP Group 2 Text) now has a unique Distribution Channel Text")
    
    return result_df


