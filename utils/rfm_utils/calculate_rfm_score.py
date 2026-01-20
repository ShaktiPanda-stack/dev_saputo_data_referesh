# from pyspark.sql import DataFrame
# from pyspark.sql import functions as F
# from pyspark.sql.window import Window
# from datetime import datetime


# def rfm_analysis(
#     df: DataFrame,
#     transaction_date_col: str,
#     monetary_col: str,
#     dimension_cols: list = None,
#     reference_date: str = None,
#     n_bins: int = 3
# ) -> DataFrame:
#     """
#     Perform RFM (Recency, Frequency, Monetary) analysis on transactional data.
    
#     Parameters:
#     -----------
#     df : DataFrame
#         Input PySpark DataFrame with transactional data
#     transaction_date_col : str
#         Column name for transaction date
#     monetary_col : str
#         Column name for monetary value (e.g., transaction amount)
#     dimension_cols : list, optional
#         Additional dimension columns to group by (e.g., ['region', 'product_category'])
#         If None, only customer_id will be used
#     reference_date : str, optional
#         Reference date for recency calculation in 'YYYY-MM-DD' format
#         If None, uses the maximum date in the dataset
#     n_bins : int, default=3
#         Number of bins for scoring (default is 3 for tertiles)
    
#     Returns:
#     --------
#     DataFrame
#         DataFrame with RFM scores and combined RFM segment
        
#     Example:
#     --------
#     >>> result_df = rfm_analysis(
#     ...     df=transactions_df,
#     ...     transaction_date_col='transaction_date',
#     ...     monetary_col='amount',
#     ...     dimension_cols=['region', 'channel'],
#     ...     reference_date='2024-12-31'
#     ... )
#     """
    
#     # Initialize dimension columns
#     if dimension_cols is None:
#         dimension_cols = []
    
#     group_cols = dimension_cols
    
#     # Determine reference date for recency calculation
#     if reference_date is None:
#         ref_date = df.agg(F.max(transaction_date_col)).collect()[0][0]
#     else:
#         ref_date = datetime.strptime(reference_date, '%Y-%m-%d').date()
    
#     # Calculate RFM metrics
#     rfm_df = df.groupBy(group_cols).agg(
#         # Recency: Days since last transaction (lower is better)
#         F.datediff(F.lit(ref_date), F.max(transaction_date_col)).alias('recency'),
        
#         # Frequency: Number of transactions (higher is better)
#         F.count(transaction_date_col).alias('frequency'),
        
#         # Monetary: Total monetary value (higher is better)
#         F.sum(monetary_col).alias('monetary')
#     )
    
#     # Create percentile buckets for scoring
#     # For Recency: Lower values (more recent) should get higher scores
#     # For Frequency & Monetary: Higher values should get higher scores
    
#     # Define quantiles based on n_bins
#     quantiles = [i / n_bins for i in range(1, n_bins)]
    
#     # Calculate percentile thresholds for each metric across all groups
#     recency_percentiles = rfm_df.approxQuantile('recency', quantiles, 0.01)
#     frequency_percentiles = rfm_df.approxQuantile('frequency', quantiles, 0.01)
#     monetary_percentiles = rfm_df.approxQuantile('monetary', quantiles, 0.01)
    
#     # Function to create scoring conditions
#     def create_score_conditions(col_name, percentiles, reverse=False):
#         """
#         Create CASE-WHEN conditions for scoring.
#         reverse=True for recency (lower values get higher scores)
#         reverse=False for frequency and monetary (higher values get higher scores)
#         """
#         conditions = None
        
#         if reverse:
#             # For Recency: Lower values (more recent) = Higher scores
#             for i, threshold in enumerate(percentiles):
#                 score = n_bins - i
#                 if conditions is None:
#                     conditions = F.when(F.col(col_name) <= threshold, score)
#                 else:
#                     conditions = conditions.when(F.col(col_name) <= threshold, score)
#             conditions = conditions.otherwise(1)
#         else:
#             # For Frequency & Monetary: Higher values = Higher scores
#             for i, threshold in enumerate(percentiles):
#                 score = i + 1
#                 if conditions is None:
#                     conditions = F.when(F.col(col_name) <= threshold, score)
#                 else:
#                     conditions = conditions.when(F.col(col_name) <= threshold, score)
#             conditions = conditions.otherwise(n_bins)
        
#         return conditions
    
#     # Apply scoring
#     rfm_scored = rfm_df.withColumn(
#         'R_score',
#         create_score_conditions('recency', recency_percentiles, reverse=True)
#     ).withColumn(
#         'F_score',
#         create_score_conditions('frequency', frequency_percentiles, reverse=False)
#     )
#     # .withColumn(
#     #     'M_score',
#     #     create_score_conditions('monetary', monetary_percentiles, reverse=False)
#     # )
    
#     # Create combined RFM score (e.g., "333", "321", etc.)
#     rfm_final = rfm_scored.withColumn(
#         'RFM_score',
#         F.concat(
#             F.col('R_score').cast('string'),
#             F.col('F_score').cast('string'),
#             F.col('M_score').cast('string')
#         )
#     )
    
#     # Add RFM segment labels (optional but useful)
#     # rfm_final = rfm_final.withColumn(
#     #     'RFM_segment',
#     #     F.when((F.col('R_score') == n_bins) & (F.col('F_score') == n_bins) & (F.col('M_score') == n_bins), 'Champions')
#     #     .when((F.col('R_score') >= n_bins - 1) & (F.col('F_score') >= n_bins - 1), 'Loyal Customers')
#     #     .when((F.col('R_score') >= n_bins - 1) & (F.col('F_score') <= 2), 'Potential Loyalists')
#     #     .when((F.col('R_score') == n_bins) & (F.col('F_score') == 1), 'New Customers')
#     #     .when((F.col('R_score') >= 2) & (F.col('F_score') >= 2) & (F.col('M_score') >= 2), 'Promising')
#     #     .when((F.col('R_score') >= 2) & (F.col('F_score') <= 2), 'Need Attention')
#     #     .when((F.col('R_score') <= 2) & (F.col('F_score') >= n_bins - 1), 'At Risk')
#     #     .when((F.col('R_score') == 1) & (F.col('F_score') == n_bins), 'Cant Lose Them')
#     #     .when((F.col('R_score') <= 2) & (F.col('F_score') <= 2), 'Hibernating')
#     #     .otherwise('Lost')
#     # )

#         # Add RFM segment labels (optional but useful)
#     rfm_final = rfm_final.withColumn(
#         'RF_segment',
#         F.when((F.col('R_score') == n_bins) & (F.col('F_score') == n_bins) , 'Champions')
#         .when((F.col('R_score') >= n_bins - 1) & (F.col('F_score') >= n_bins - 1), 'Loyal Customers')
#         .when((F.col('R_score') >= n_bins - 1) & (F.col('F_score') == n_bins), 'Loyal Customers')
#         .when((F.col('R_score') == n_bins) & (F.col('F_score') == n_bins-1), 'Loyal Customers')
#         .when((F.col('R_score') == n_bins -1 ) & (F.col('F_score') >= n_bins -2) , 'Potential Loyalist')
#         .when((F.col('R_score') == n_bins) & (F.col('F_score') == n_bins -2), 'Potential Loyalist')
#         .when((F.col('R_score') == n_bins -2) & (F.col('F_score') == n_bins - 1), 'At Risk')
#         .when((F.col('R_score') == n_bins -2) & (F.col('F_score') == n_bins), 'At Risk')
#         .when((F.col('R_score') == n_bins -2) & (F.col('F_score') <= n_bins-2), 'Hibernating')
#         .otherwise('Hibernating')
#     )

    
#     # Select final columns in a logical order
#     final_cols = (
#         group_cols + 
#         ['recency', 'frequency', 'monetary', 
#          'R_score', 'F_score',  'RF_score', 'RF_segment']
#     )

    
    
#     return rfm_final.select(final_cols)





# def cost_rfm_pipeline(
#     df: DataFrame,
#     cost_cols: list,
#     dimension_cols: list,
#     transaction_date_col: str,
#     reference_date: str = None,
#     n_bins: int = 3
# ) -> DataFrame:
#     """
#     Performs RFM scoring for each cost metric column where cost < benchmark.

#     Parameters
#     ----------
#     df : DataFrame
#         Input transactional data containing cost and benchmark columns.
#     cost_cols : list
#         List of cost column names (e.g., ['cost_a', 'cost_b']).
#         Each must have a corresponding benchmark column (e.g., 'cost_a_benchmark').
#     dimension_cols : list
#         Columns used for grouping in RFM analysis (e.g., ['customer_id', 'region']).
#     transaction_date_col : str
#         Column name representing transaction date.
#     reference_date : str, optional
#         Reference date for recency calculation ('YYYY-MM-DD'). Defaults to max(transaction_date_col).
#     n_bins : int, default=3
#         Number of bins for RFM scoring.

#     Returns
#     -------
#     DataFrame
#         Original DataFrame with new columns appended:
#         - `<metric>_rfm`: RFM score string (e.g., "321"), or "000" if no valid RFM score.
#     """

#     result_df = df
    

#     for cost_col in cost_cols:
#         benchmark_col = f"{cost_col}_benchmark"
#         filtered_df = df.filter(F.col(cost_col) < F.col(benchmark_col))

#         # Run RFM analysis on the filtered subset
#         rfm_result = rfm_analysis(
#             df=filtered_df,
#             transaction_date_col=transaction_date_col,
#             monetary_col=cost_col,
#             dimension_cols=dimension_cols,
#             reference_date=reference_date,
#             n_bins=n_bins
#         ).select(
#             *dimension_cols,
#             # F.col("RF_score").alias(f"{cost_col}_rf"),
#             F.col("RF_segment").alias(f"{cost_col}_segment"),
#             F.col("recency").alias(f"{cost_col}_recency"),
#             F.col("frequency").alias(f"{cost_col}_frequency")
#         )

#         # Join RFM results back to original DataFrame
#         result_df = (
#             result_df.join(rfm_result, on=dimension_cols, how="left")
#             .withColumn(
#                 f"{cost_col}_rf",
#                 F.coalesce(F.col(f"{cost_col}_rfm"), F.lit("000"))
#             )
#         )

#     # Drop all cost and benchmark columns
#     cols_to_drop = cost_cols + [f"{c}_benchmark" for c in cost_cols]
#     result_df = result_df.drop(*cols_to_drop)
#     result_df = result_df.drop('Invoice Date', 'Calendar Date','distribution_ibp','RF_score')


#     # Drop duplicates
#     result_df = result_df.dropDuplicates()

#     return result_df



# def cost_rfm_pipeline_recency_frequency(
#     df: DataFrame,
#     cost_cols: list,
#     dimension_cols: list,
#     transaction_date_col: str,
#     reference_date: str = None,
#     n_bins: int = 3
# ) -> DataFrame:
#     """
#     Performs RFM analysis for each cost metric column where cost < benchmark,
#     returning Recency and Frequency values.

#     Parameters
#     ----------
#     df : DataFrame
#         Input transactional data containing cost and benchmark columns.
#     cost_cols : list
#         List of cost column names (e.g., ['cost_a', 'cost_b']).
#         Each must have a corresponding benchmark column (e.g., 'cost_a_benchmark').
#     dimension_cols : list
#         Columns used for grouping in RFM analysis (e.g., ['customer_id', 'region']).
#     transaction_date_col : str
#         Column name representing transaction date.
#     reference_date : str, optional
#         Reference date for recency calculation ('YYYY-MM-DD'). Defaults to max(transaction_date_col).
#     n_bins : int, default=3
#         Number of bins for RFM scoring.

#     Returns
#     -------
#     DataFrame
#         Original DataFrame with new columns appended:
#         - `<metric>_recency`: Days since last transaction (null if no valid data).
#         - `<metric>_frequency`: Number of transactions (null if no valid data).
#     """

#     result_df = df
    

#     for cost_col in cost_cols:
#         benchmark_col = f"{cost_col}_benchmark"
#         filtered_df = df.filter(F.col(cost_col) < F.col(benchmark_col))

# #     revenue_like_metrics = [
# #     "net_revenue", "net_revenue_pct_sales", "net_revenue_per_lb"
   
# # ]

# #     for cost_col in cost_cols:
# #         benchmark_col = f"{cost_col}_benchmark"

# #     # Apply correct logic based on metric type
# #         if cost_col in revenue_like_metrics:
# #             # Revenue/profit → HIGHER is better
# #             filtered_df = df.filter(F.col(cost_col) > F.col(benchmark_col))
# #         else:
# #             # Cost → LOWER is better
# #             filtered_df = df.filter(F.col(cost_col) < F.col(benchmark_col))

# #         # Skip if no qualifying rows
# #         if filtered_df.head(1) is None or filtered_df.head(1) == []:
# #             print(f"⚠️ Skipping '{cost_col}' because no rows satisfy comparison rule.")
# #             result_df = (
# #                 result_df.withColumn(f"{cost_col}_recency", F.lit(0))
# #                         .withColumn(f"{cost_col}_frequency", F.lit(0))
# #             )
# #             continue


#         # Run RFM analysis on the filtered subset
#         rfm_result = rfm_analysis(
#             df=filtered_df,
#             transaction_date_col=transaction_date_col,
#             monetary_col=cost_col,
#             dimension_cols=dimension_cols,
#             reference_date=reference_date,
#             n_bins=n_bins
#         ).select(
#             *dimension_cols,
#             # CHANGE 1: Select 'recency' and 'frequency' instead of 'RFM_score'
#             F.col("recency").alias(f"{cost_col}_recency"),
#             F.col("frequency").alias(f"{cost_col}_frequency")
#         )

#         # Join RFM results back to original DataFrame
#         result_df = (
#             result_df.join(rfm_result, on=dimension_cols, how="left")
#             # CHANGE 2: Handle null values for both recency and frequency columns
#             .withColumn(
#                 f"{cost_col}_recency",
#                 F.coalesce(F.col(f"{cost_col}_recency"), F.lit(0))
#             )
#             .withColumn(
#                 f"{cost_col}_frequency",
#                 F.coalesce(F.col(f"{cost_col}_frequency"), F.lit(0))
#             )
#         )

#     # Drop all cost and benchmark columns
#     cols_to_drop = cost_cols + [f"{c}_benchmark" for c in cost_cols]
#     result_df = result_df.drop(*cols_to_drop)
#     result_df = result_df.drop('Invoice Date', 'Calendar Date','distribution_ibp')


#     # Drop duplicates
#     result_df = result_df.dropDuplicates()

#     return result_df


# def cost_rfm_pipeline_good(
#     df: DataFrame,
#     cost_cols: list,
#     dimension_cols: list,
#     transaction_date_col: str,
#     reference_date: str = None,
#     n_bins: int = 3
# ) -> DataFrame:
#     """
#     Performs RFM scoring for each cost metric column where cost < benchmark.

#     Parameters
#     ----------
#     df : DataFrame
#         Input transactional data containing cost and benchmark columns.
#     cost_cols : list
#         List of cost column names (e.g., ['cost_a', 'cost_b']).
#         Each must have a corresponding benchmark column (e.g., 'cost_a_benchmark').
#     dimension_cols : list
#         Columns used for grouping in RFM analysis (e.g., ['customer_id', 'region']).
#     transaction_date_col : str
#         Column name representing transaction date.
#     reference_date : str, optional
#         Reference date for recency calculation ('YYYY-MM-DD'). Defaults to max(transaction_date_col).
#     n_bins : int, default=3
#         Number of bins for RFM scoring.

#     Returns
#     -------
#     DataFrame
#         Original DataFrame with new columns appended:
#         - `<metric>_rfm`: RFM score string (e.g., "321"), or "000" if no valid RFM score.
#     """

#     result_df = df
    

#     for cost_col in cost_cols:
#         benchmark_col = f"{cost_col}_benchmark"
#         filtered_df = df.filter(F.col(cost_col) > F.col(benchmark_col))

#         # Run RFM analysis on the filtered subset
#         rfm_result = rfm_analysis(
#             df=filtered_df,
#             transaction_date_col=transaction_date_col,
#             monetary_col=cost_col,
#             dimension_cols=dimension_cols,
#             reference_date=reference_date,
#             n_bins=n_bins
#         ).select(
#             *dimension_cols,
#             F.col("RF_score").alias(f"{cost_col}_rf"),
#             F.col("RF_segment").alias(f"{cost_col}_segment"),
#             # F.col("RFM_score").alias(f"{cost_col}_rf"),
#             # F.col("RFM_segment").alias(f"{cost_col}_segment"),
#             F.col("recency").alias(f"{cost_col}_recency"),
#             F.col("frequency").alias(f"{cost_col}_frequency")
#         )

#         # Join RFM results back to original DataFrame
#         result_df = (
#             result_df.join(rfm_result, on=dimension_cols, how="left")
#             .withColumn(
#                 f"{cost_col}_rf",
#                 F.coalesce(F.col(f"{cost_col}_rf"), F.lit("00"))
#             )
#         )

#     # Drop all cost and benchmark columns
#     cols_to_drop = cost_cols + [f"{c}_benchmark" for c in cost_cols]
#     result_df = result_df.drop(*cols_to_drop)
#     result_df = result_df.drop('Invoice Date', 'Calendar Date','distribution_ibp','RF_score')


#     # Drop duplicates
#     result_df = result_df.dropDuplicates()

#     return result_df





# def cost_rfm_pipeline_recency_frequency(
#     df: DataFrame,
#     cost_cols: list,
#     dimension_cols: list,
#     transaction_date_col: str,
#     reference_date: str = None,
#     n_bins: int = 3
# ) -> DataFrame:
#     """
#     Performs RFM analysis for each cost metric column where cost < benchmark,
#     returning Recency and Frequency values.

#     Parameters
#     ----------
#     df : DataFrame
#         Input transactional data containing cost and benchmark columns.
#     cost_cols : list
#         List of cost column names (e.g., ['cost_a', 'cost_b']).
#         Each must have a corresponding benchmark column (e.g., 'cost_a_benchmark').
#     dimension_cols : list
#         Columns used for grouping in RFM analysis (e.g., ['customer_id', 'region']).
#     transaction_date_col : str
#         Column name representing transaction date.
#     reference_date : str, optional
#         Reference date for recency calculation ('YYYY-MM-DD'). Defaults to max(transaction_date_col).
#     n_bins : int, default=3
#         Number of bins for RFM scoring.

#     Returns
#     -------
#     DataFrame
#         Original DataFrame with new columns appended:
#         - `<metric>_recency`: Days since last transaction (null if no valid data).
#         - `<metric>_frequency`: Number of transactions (null if no valid data).
#     """

#     result_df = df
    

#     for cost_col in cost_cols:
#         benchmark_col = f"{cost_col}_benchmark"
#         filtered_df = df.filter(F.col(cost_col) < F.col(benchmark_col))

#         # Run RFM analysis on the filtered subset
#         rfm_result = rfm_analysis(
#             df=filtered_df,
#             transaction_date_col=transaction_date_col,
#             monetary_col=cost_col,
#             dimension_cols=dimension_cols,
#             reference_date=reference_date,
#             n_bins=n_bins
#         ).select(
#             *dimension_cols,
#             # CHANGE 1: Select 'recency' and 'frequency' instead of 'RFM_score'
#             F.col("recency").alias(f"{cost_col}_recency"),
#             F.col("frequency").alias(f"{cost_col}_frequency")
#         )

#         # Join RFM results back to original DataFrame
#         result_df = (
#             result_df.join(rfm_result, on=dimension_cols, how="left")
#             # CHANGE 2: Handle null values for both recency and frequency columns
#             .withColumn(
#                 f"{cost_col}_recency",
#                 F.coalesce(F.col(f"{cost_col}_recency"), F.lit(0))
#             )
#             .withColumn(
#                 f"{cost_col}_frequency",
#                 F.coalesce(F.col(f"{cost_col}_frequency"), F.lit(0))
#             )
#         )

#     # Drop all cost and benchmark columns
#     cols_to_drop = cost_cols + [f"{c}_benchmark" for c in cost_cols]
#     result_df = result_df.drop(*cols_to_drop)
#     result_df = result_df.drop('Invoice Date', 'Calendar Date','distribution_ibp')


#     # Drop duplicates
#     result_df = result_df.dropDuplicates()

#     return result_df


from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime


def rfm_analysis(
    df: DataFrame,
    transaction_date_col: str,
    monetary_col: str,
    dimension_cols: list = None,
    reference_date: str = None,
    n_bins: int = 3
) -> DataFrame:
    """
    Perform RFM (Recency, Frequency, Monetary) analysis on transactional data.
    
    Parameters:
    -----------
    df : DataFrame
        Input PySpark DataFrame with transactional data
    transaction_date_col : str
        Column name for transaction date
    monetary_col : str
        Column name for monetary value (e.g., transaction amount)
    dimension_cols : list, optional
        Additional dimension columns to group by (e.g., ['region', 'product_category'])
        If None, only customer_id will be used
    reference_date : str, optional
        Reference date for recency calculation in 'YYYY-MM-DD' format
        If None, uses the maximum date in the dataset
    n_bins : int, default=3
        Number of bins for scoring (default is 3 for tertiles)
    
    Returns:
    --------
    DataFrame
        DataFrame with RFM scores and combined RFM segment
        
    Example:
    --------
    >>> result_df = rfm_analysis(
    ...     df=transactions_df,
    ...     transaction_date_col='transaction_date',
    ...     monetary_col='amount',
    ...     dimension_cols=['region', 'channel'],
    ...     reference_date='2024-12-31'
    ... )
    """
    
    # Initialize dimension columns
    if dimension_cols is None:
        dimension_cols = []
    
    group_cols = dimension_cols
    
    # Determine reference date for recency calculation
    if reference_date is None:
        ref_date = df.agg(F.max(transaction_date_col)).collect()[0][0]
    else:
        ref_date = datetime.strptime(reference_date, '%Y-%m-%d').date()
    
    # Calculate RFM metrics
    rfm_df = df.groupBy(group_cols).agg(
        # Recency: Days since last transaction (lower is better)
        F.datediff(F.lit(ref_date), F.max(transaction_date_col)).alias('recency'),
        
        # Frequency: Number of transactions (higher is better)
        F.count(transaction_date_col).alias('frequency'),
        
        # Monetary: Total monetary value (higher is better)
        F.sum(monetary_col).alias('monetary')
    )
    
    # Create percentile buckets for scoring
    # For Recency: Lower values (more recent) should get higher scores
    # For Frequency & Monetary: Higher values should get higher scores
    
    # Define quantiles based on n_bins
    quantiles = [i / n_bins for i in range(1, n_bins)]
    
    # Calculate percentile thresholds for each metric across all groups
    recency_percentiles = rfm_df.approxQuantile('recency', quantiles, 0.01)
    frequency_percentiles = rfm_df.approxQuantile('frequency', quantiles, 0.01)
    monetary_percentiles = rfm_df.approxQuantile('monetary', quantiles, 0.01)
    
    # # Function to create scoring conditions
    # def create_score_conditions(col_name, percentiles, reverse=False):
    #     """
    #     Create CASE-WHEN conditions for scoring.
    #     reverse=True for recency (lower values get higher scores)
    #     reverse=False for frequency and monetary (higher values get higher scores)
    #     """
    #     conditions = None
        
    #     if reverse:
    #         # For Recency: Lower values (more recent) = Higher scores
    #         for i, threshold in enumerate(percentiles):
    #             score = n_bins - i
    #             if conditions is None:
    #                 conditions = F.when(F.col(col_name) <= threshold, score)
    #             else:
    #                 conditions = conditions.when(F.col(col_name) <= threshold, score)
    #         conditions = conditions.otherwise(1)
    #     else:
    #         # For Frequency & Monetary: Higher values = Higher scores
    #         for i, threshold in enumerate(percentiles):
    #             score = i + 1
    #             if conditions is None:
    #                 conditions = F.when(F.col(col_name) <= threshold, score)
    #             else:
    #                 conditions = conditions.when(F.col(col_name) <= threshold, score)
    #         conditions = conditions.otherwise(n_bins)
        
    #     return conditions
    
    # # Apply scoring
    # rfm_scored = rfm_df.withColumn(
    #     'R_score',
    #     create_score_conditions('recency', recency_percentiles, reverse=True)
    # ).withColumn(
    #     'F_score',
    #     create_score_conditions('frequency', frequency_percentiles, reverse=False)
    # ).withColumn(
    #     'M_score',
    #     create_score_conditions('monetary', monetary_percentiles, reverse=False)
    # )
    
    # # Create combined RFM score (e.g., "333", "321", etc.)
    # rfm_final = rfm_scored.withColumn(
    #     'RFM_score',
    #     F.concat(
    #         F.col('R_score').cast('string'),
    #         F.col('F_score').cast('string'),
    #         F.col('M_score').cast('string')
    #     )
    # )
    
    # # Add RFM segment labels (optional but useful)
    # rfm_final = rfm_final.withColumn(
    #     'RFM_segment',
    #     F.when((F.col('R_score') == n_bins) & (F.col('F_score') == n_bins) & (F.col('M_score') == n_bins), 'Champions')
    #     .when((F.col('R_score') >= n_bins - 1) & (F.col('F_score') >= n_bins - 1), 'Loyal Customers')
    #     .when((F.col('R_score') >= n_bins - 1) & (F.col('F_score') <= 2), 'Potential Loyalists')
    #     .when((F.col('R_score') == n_bins) & (F.col('F_score') == 1), 'New Customers')
    #     .when((F.col('R_score') >= 2) & (F.col('F_score') >= 2) & (F.col('M_score') >= 2), 'Promising')
    #     .when((F.col('R_score') >= 2) & (F.col('F_score') <= 2), 'Need Attention')
    #     .when((F.col('R_score') <= 2) & (F.col('F_score') >= n_bins - 1), 'At Risk')
    #     .when((F.col('R_score') == 1) & (F.col('F_score') == n_bins), 'Cant Lose Them')
    #     .when((F.col('R_score') <= 2) & (F.col('F_score') <= 2), 'Hibernating')
    #     .otherwise('Lost')
    # )
    
    # # Select final columns in a logical order
    # final_cols = (
    #     group_cols + 
    #     ['recency', 'frequency', 'monetary', 
    #      'R_score', 'F_score', 'M_score', 'RFM_score', 'RFM_segment']
    # )

    
    
    # return rfm_final.select(final_cols)


    # Function to create scoring conditions
    def create_score_conditions_RF(col_name, percentiles, reverse=False):
        """
        Create CASE-WHEN conditions for scoring.
        reverse=True for recency (lower values get higher scores)
        reverse=False for frequency and monetary (higher values get higher scores)
        """
        conditions = None
        
        if reverse:
            # For Recency: Lower values (more recent) = Higher scores
            for i, threshold in enumerate(percentiles):
                score = n_bins - i
                if conditions is None:
                    conditions = F.when(F.col(col_name) <= threshold, score)
                else:
                    conditions = conditions.when(F.col(col_name) <= threshold, score)
            conditions = conditions.otherwise(1)
        else:
            # For Frequency & Monetary: Higher values = Higher scores
            for i, threshold in enumerate(percentiles):
                score = i + 1
                if conditions is None:
                    conditions = F.when(F.col(col_name) <= threshold, score)
                else:
                    conditions = conditions.when(F.col(col_name) <= threshold, score)
            conditions = conditions.otherwise(n_bins)
        
        return conditions
    
    # Apply scoring
    rfm_scored = rfm_df.withColumn(
        'R_score',
        create_score_conditions_RF('recency', recency_percentiles, reverse=True)
    ).withColumn(
        'F_score',
        create_score_conditions_RF('frequency', frequency_percentiles, reverse=False)
    )
    # Create combined RFM score (e.g., "333", "321", etc.)
    rfm_final = rfm_scored.withColumn(
        'RF_score',
        F.concat(
            F.col('R_score').cast('string'),
            F.col('F_score').cast('string')
        )
    )
    
    # Add RFM segment labels (optional but useful)
    rfm_final = rfm_final.withColumn(
        'RF_segment',
        F.when((F.col('R_score') == n_bins) & (F.col('F_score') == n_bins) , 'Champions')
        .when((F.col('R_score') >= n_bins - 1) & (F.col('F_score') >= n_bins - 1), 'Loyal Customers')
        .when((F.col('R_score') >= n_bins - 1) & (F.col('F_score') == n_bins), 'Loyal Customers')
        .when((F.col('R_score') == n_bins) & (F.col('F_score') == n_bins-1), 'Loyal Customers')
        .when((F.col('R_score') == n_bins -1 ) & (F.col('F_score') >= n_bins -2) , 'Potential Loyalist')
        .when((F.col('R_score') == n_bins) & (F.col('F_score') == n_bins -2), 'Potential Loyalist')
        .when((F.col('R_score') == n_bins -2) & (F.col('F_score') == n_bins - 1), 'At Risk')
        .when((F.col('R_score') == n_bins -2) & (F.col('F_score') == n_bins), 'At Risk')
        .when((F.col('R_score') == n_bins -2) & (F.col('F_score') <= n_bins-2), 'Hibernating')
        .otherwise('Hibernating')
    )
    
    # Select final columns in a logical order
    final_cols = (
        group_cols + 
        ['recency', 'frequency', 'monetary', 
         'R_score', 'F_score',  'RF_score', 'RF_segment']
    )

    
    
    return rfm_final.select(final_cols)





def cost_rfm_pipeline(
    df: DataFrame,
    cost_cols: list,
    dimension_cols: list,
    transaction_date_col: str,
    reference_date: str = None,
    n_bins: int = 3
) -> DataFrame:
    """
    Performs RFM scoring for each cost metric column where cost < benchmark.

    Parameters
    ----------
    df : DataFrame
        Input transactional data containing cost and benchmark columns.
    cost_cols : list
        List of cost column names (e.g., ['cost_a', 'cost_b']).
        Each must have a corresponding benchmark column (e.g., 'cost_a_benchmark').
    dimension_cols : list
        Columns used for grouping in RFM analysis (e.g., ['customer_id', 'region']).
    transaction_date_col : str
        Column name representing transaction date.
    reference_date : str, optional
        Reference date for recency calculation ('YYYY-MM-DD'). Defaults to max(transaction_date_col).
    n_bins : int, default=3
        Number of bins for RFM scoring.

    Returns
    -------
    DataFrame
        Original DataFrame with new columns appended:
        - `<metric>_rfm`: RFM score string (e.g., "321"), or "000" if no valid RFM score.
    """

    result_df = df
    

    for cost_col in cost_cols:
        benchmark_col = f"{cost_col}_benchmark"
        filtered_df = df.filter(F.col(cost_col) < F.col(benchmark_col))

        # Run RFM analysis on the filtered subset
        rfm_result = rfm_analysis(
            df=filtered_df,
            transaction_date_col=transaction_date_col,
            monetary_col=cost_col,
            dimension_cols=dimension_cols,
            reference_date=reference_date,
            n_bins=n_bins
        ).select(
            *dimension_cols,
            F.col("RF_score").alias(f"{cost_col}_rf"),
            F.col("RF_segment").alias(f"{cost_col}_segment"),
            F.col("recency").alias(f"{cost_col}_recency"),
            F.col("frequency").alias(f"{cost_col}_frequency")
        )

        # Join RFM results back to original DataFrame
        result_df = (
            result_df.join(rfm_result, on=dimension_cols, how="left")
            .withColumn(
                f"{cost_col}_rf",
                F.coalesce(F.col(f"{cost_col}_rf"), F.lit("00"))
            )
        )

    # Drop all cost and benchmark columns
    cols_to_drop = cost_cols + [f"{c}_benchmark" for c in cost_cols]
    result_df = result_df.drop(*cols_to_drop)
    result_df = result_df.drop('Invoice Date', 'Calendar Date','distribution_ibp',"RF_score")


    # Drop duplicates
    result_df = result_df.dropDuplicates()

    return result_df

def cost_rfm_pipeline_good(
    df: DataFrame,
    cost_cols: list,
    dimension_cols: list,
    transaction_date_col: str,
    reference_date: str = None,
    n_bins: int = 3
) -> DataFrame:
    """
    Performs RFM scoring for each cost metric column where cost < benchmark.

    Parameters
    ----------
    df : DataFrame
        Input transactional data containing cost and benchmark columns.
    cost_cols : list
        List of cost column names (e.g., ['cost_a', 'cost_b']).
        Each must have a corresponding benchmark column (e.g., 'cost_a_benchmark').
    dimension_cols : list
        Columns used for grouping in RFM analysis (e.g., ['customer_id', 'region']).
    transaction_date_col : str
        Column name representing transaction date.
    reference_date : str, optional
        Reference date for recency calculation ('YYYY-MM-DD'). Defaults to max(transaction_date_col).
    n_bins : int, default=3
        Number of bins for RFM scoring.

    Returns
    -------
    DataFrame
        Original DataFrame with new columns appended:
        - `<metric>_rfm`: RFM score string (e.g., "321"), or "000" if no valid RFM score.
    """

    result_df = df
    

    for cost_col in cost_cols:
        benchmark_col = f"{cost_col}_benchmark"
        filtered_df = df.filter(F.col(cost_col) > F.col(benchmark_col))

        # Run RFM analysis on the filtered subset
        rfm_result = rfm_analysis(
            df=filtered_df,
            transaction_date_col=transaction_date_col,
            monetary_col=cost_col,
            dimension_cols=dimension_cols,
            reference_date=reference_date,
            n_bins=n_bins
        ).select(
            *dimension_cols,
            F.col("RF_score").alias(f"{cost_col}_rf"),
            F.col("RF_segment").alias(f"{cost_col}_segment"),
            F.col("recency").alias(f"{cost_col}_recency"),
            F.col("frequency").alias(f"{cost_col}_frequency")
        )

        # Join RFM results back to original DataFrame
        result_df = (
            result_df.join(rfm_result, on=dimension_cols, how="left")
            .withColumn(
                f"{cost_col}_rf",
                F.coalesce(F.col(f"{cost_col}_rf"), F.lit("00"))
            )
        )

    # Drop all cost and benchmark columns
    cols_to_drop = cost_cols + [f"{c}_benchmark" for c in cost_cols]
    result_df = result_df.drop(*cols_to_drop)
    result_df = result_df.drop('Invoice Date', 'Calendar Date','distribution_ibp',"RF_score")


    # Drop duplicates
    result_df = result_df.dropDuplicates()

    return result_df





def cost_rfm_pipeline_recency_frequency(
    df: DataFrame,
    cost_cols: list,
    dimension_cols: list,
    transaction_date_col: str,
    reference_date: str = None,
    n_bins: int = 3
) -> DataFrame:
    """
    Performs RFM analysis for each cost metric column where cost < benchmark,
    returning Recency and Frequency values.

    Parameters
    ----------
    df : DataFrame
        Input transactional data containing cost and benchmark columns.
    cost_cols : list
        List of cost column names (e.g., ['cost_a', 'cost_b']).
        Each must have a corresponding benchmark column (e.g., 'cost_a_benchmark').
    dimension_cols : list
        Columns used for grouping in RFM analysis (e.g., ['customer_id', 'region']).
    transaction_date_col : str
        Column name representing transaction date.
    reference_date : str, optional
        Reference date for recency calculation ('YYYY-MM-DD'). Defaults to max(transaction_date_col).
    n_bins : int, default=3
        Number of bins for RFM scoring.

    Returns
    -------
    DataFrame
        Original DataFrame with new columns appended:
        - `<metric>_recency`: Days since last transaction (null if no valid data).
        - `<metric>_frequency`: Number of transactions (null if no valid data).
    """

    result_df = df
    

    for cost_col in cost_cols:
        benchmark_col = f"{cost_col}_benchmark"
        filtered_df = df.filter(F.col(cost_col) < F.col(benchmark_col))

        # Run RFM analysis on the filtered subset
        rfm_result = rfm_analysis(
            df=filtered_df,
            transaction_date_col=transaction_date_col,
            monetary_col=cost_col,
            dimension_cols=dimension_cols,
            reference_date=reference_date,
            n_bins=n_bins
        ).select(
            *dimension_cols,
            # CHANGE 1: Select 'recency' and 'frequency' instead of 'RFM_score'
            F.col("recency").alias(f"{cost_col}_recency"),
            F.col("frequency").alias(f"{cost_col}_frequency")
        )

        # Join RFM results back to original DataFrame
        result_df = (
            result_df.join(rfm_result, on=dimension_cols, how="left")
            # CHANGE 2: Handle null values for both recency and frequency columns
            .withColumn(
                f"{cost_col}_recency",
                F.coalesce(F.col(f"{cost_col}_recency"), F.lit(0))
            )
            .withColumn(
                f"{cost_col}_frequency",
                F.coalesce(F.col(f"{cost_col}_frequency"), F.lit(0))
            )
        )

    # Drop all cost and benchmark columns
    cols_to_drop = cost_cols + [f"{c}_benchmark" for c in cost_cols]
    result_df = result_df.drop(*cols_to_drop)
    result_df = result_df.drop('Invoice Date', 'Calendar Date','distribution_ibp')


    # Drop duplicates
    result_df = result_df.dropDuplicates()

    return result_df

 
