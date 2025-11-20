from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql import Window

def filter_high_priority_cases_dynamic(df: DataFrame, metric: str) -> (DataFrame, DataFrame, DataFrame, DataFrame):
    """
    Filters high-priority cases based on the given metric (return, tier2costs, costtoserve).

    Parameters:
        df (DataFrame): Input PySpark DataFrame.
        metric (str): Metric to use ('return', 'tier2costs', or 'costtoserve').

    Returns:
        Tuple of DataFrames: (full_df, high_priority_df, result_df)
    """
    # 🚀 **Metric Mappings**
    metric_map = {
        "return": {
            "main_metric": "return_rate_pct",
            "cost_metric": "CostOfReturn",
            "deviation_metric": "avg_deviation_return_rate_pct",
            "transaction_metric": "avg_deviation_return_transaction_pct"
        },
        "tier2costs": {
            "main_metric": "Tier_2_Costs_pct",
            "cost_metric": "Tier_2_Costs",
            "deviation_metric": "avg_deviation_tier_2_costs_pct",
            "transaction_metric": None
        },
        "costtoserve": {
            "main_metric": "Tiers_1_2_Costs_pct",
            "cost_metric": "Cost_to_Serve_Tiers_1_2",
            "deviation_metric": "avg_deviation_tiers_1_2_costs_pct",
            "transaction_metric": None
        }
    }

    if metric not in metric_map:
        raise ValueError("Invalid metric. Choose from 'return', 'tier2costs', or 'costtoserve'.")

    # 🚀 **Map the column names based on the selected metric**
    main_metric = metric_map[metric]["main_metric"] #Percentage Metric
    cost_metric = metric_map[metric]["cost_metric"] 
    deviation_metric = metric_map[metric]["deviation_metric"]
    transaction_metric = metric_map[metric]["transaction_metric"]

    # 🚀 **Filter valid cases**
    if metric == "return":
        df = df.filter(df['HasReturn'] > 0)
    
    df = df.withColumn(main_metric, F.abs(main_metric))

    # 🚀 **Filter for net revenue >= 0**
    df = df.filter(F.col("Sales") >= 1000)

    # 🚀 **Handle missing values**
    if metric == "return":
        df = df.filter((df[main_metric] > 0) & (df[main_metric] <= 100))

    df = df.na.drop(subset=[main_metric, cost_metric])

    # 🚀 **Invert cost metric for positive ranking**
    df = df.withColumn('Cost_Positive', -df[cost_metric])



    # 🚀 **Outlier Removal (1st and 99th Percentile Trimming)**
    percentiles = df.select(
        F.expr(f"percentile_approx({main_metric}, 0.01)").alias("metric_1"),
        F.expr(f"percentile_approx({main_metric}, 0.99)").alias("metric_99"),
        F.expr(f"percentile_approx(Cost_Positive, 0.01)").alias("cost_1"),
        F.expr(f"percentile_approx(Cost_Positive, 0.99)").alias("cost_99")
    ).collect()[0]

    # Filter only the higher bit (greater than 99th percentile)
    higher_df = df.filter(
    (df['Cost_Positive'] > percentiles['cost_99'])   # Greater than 99th percentile for Cost_Positive
    )


    df = df.filter(
        (df[main_metric].between(percentiles['metric_1'], percentiles['metric_99'])) &
        (df['Cost_Positive'].between(percentiles['cost_1'], percentiles['cost_99']))
    )

    # 🚀 **Z-score normalization at ProductType level**
    window_spec = Window.partitionBy("ProductType")

    df = df.withColumn(
        'Z_metric', 
        (F.col(main_metric) - F.mean(main_metric).over(window_spec)) / 
        F.stddev(main_metric).over(window_spec)
    ).withColumn(
        'Z_cost', 
        (F.col('Cost_Positive') - F.mean('Cost_Positive').over(window_spec)) / 
        F.stddev('Cost_Positive').over(window_spec)
    ).fillna(0)

    # 🚀 **Calculate Standard Deviations**
    std_dev_metric = df.select(F.stddev('Z_metric').alias('std_metric')).collect()[0]['std_metric']
    std_dev_cost = df.select(F.stddev('Z_cost').alias('std_cost')).collect()[0]['std_cost']

    # **Calculate Dynamic Weights**
    w1 = std_dev_cost / (std_dev_cost + std_dev_metric)
    w2 = std_dev_metric / (std_dev_cost + std_dev_metric)

    # 🚀 **Composite Score with Dynamic Weights**
    df = df.withColumn(
        'composite_score',
        (1 + w1 * F.col('Z_metric')) * (1 + w2 * F.col('Z_cost'))
    )


    #  **Initial Composite Score**
    # df = df.withColumn(
    #     'composite_score',
    #     (1 + F.col('Z_metric')) * (1 + F.col('Z_cost'))
    # )

    # 🚀 **Pre-calculate dynamic percentiles for initial filtering**
    if metric == "return":
        thresholds = df.groupBy("ProductType").agg(
            F.expr("percentile_approx(composite_score, 0.85)").alias("threshold_85")
        )
    else:
        thresholds = df.groupBy("ProductType").agg(
            F.expr("percentile_approx(composite_score, 0.9)").alias("threshold_85")
        )


    # 🚀 **Broadcast thresholds for efficiency**
    thresholds = F.broadcast(thresholds)

    # 🚀 **Join with the main dataframe**
    df = df.alias("main").join(
        thresholds.alias("thresh"),
        df["ProductType"] == thresholds["ProductType"],
        how="left"
    ).drop(thresholds["ProductType"])

    # ✅ **Filter based on composite score**
    filtered_df = df.filter(F.col('composite_score') >= F.col('thresh.threshold_85'))

    # 🚀 **Apply Frequency & Consistency calculations only on filtered records**
    filtered_window_spec = Window.partitionBy("ProductType")

    filtered_df = filtered_df.withColumn(
        'Z_deviation_metric',
        (F.col(deviation_metric) - F.mean(deviation_metric).over(filtered_window_spec)) / 
        F.stddev(deviation_metric).over(filtered_window_spec)
    ).fillna(0)

    # 🚀 **Add transaction metric only if it exists**
    if transaction_metric:
        filtered_df = filtered_df.withColumn(
            'Z_deviation_transaction',
            (F.col(transaction_metric) - F.mean(transaction_metric).over(filtered_window_spec)) / 
            F.stddev(transaction_metric).over(filtered_window_spec)
        ).fillna(0)

        # 🚀 **Final Multiplicative Score (with transaction metric)**
        filtered_df = filtered_df.withColumn(
            'final_multiplicative_score',
            (1 + F.col('Z_metric')) * 
            (1 + F.col('Z_cost')) * 
            (1 + F.col('Z_deviation_metric')) * 
            (1 + F.col('Z_deviation_transaction'))
        )
    else:
        # 🚀 **Final Multiplicative Score (without transaction metric)**
        filtered_df = filtered_df.withColumn(
            'final_multiplicative_score',
            (1 + F.col('Z_metric')) * 
            (1 + F.col('Z_cost')) * 
            (1 + F.col('Z_deviation_metric'))
        )

    # 🚀 **Calculate 10th percentile threshold**
    threshold_10_percent = filtered_df.select(
        F.expr("percentile_approx(final_multiplicative_score, 0.3)").alias("threshold_10")
    ).collect()[0]["threshold_10"]

    # ✅ **Final High-Priority Filter (keeping top 70%)**
    high_priority_df = filtered_df.filter(F.col('final_multiplicative_score') >= threshold_10_percent)


    from pyspark.sql.types import DoubleType

    # # 🚀 **Cast columns to DoubleType before multiplication**
    # weighted_avg = df.select(
    #     (F.sum(F.col(main_metric).cast(DoubleType()) * F.col("Sales").cast(DoubleType())) / 
    #     F.sum(F.col("Sales").cast(DoubleType()))).alias("weighted_avg_main"),
        
    #     (F.sum(F.col('Cost_Positive').cast(DoubleType()) * F.col("Sales").cast(DoubleType())) / 
    #     F.sum(F.col("Sales").cast(DoubleType()))).alias("weighted_avg_cost")
    # ).collect()[0]



        # 🚀 **Step 1: Calculate Weighted Average by ProductType**
    weighted_avg_by_product = df.groupBy("ProductType").agg(
        (F.sum(F.col(main_metric).cast(DoubleType()) * F.col("Sales").cast(DoubleType())) / 
        F.sum(F.col("Sales").cast(DoubleType()))).alias("weighted_avg_main"),
        
        (F.sum(F.col("Cost_Positive").cast(DoubleType()) * F.col("Sales").cast(DoubleType())) / 
        F.sum(F.col("Sales").cast(DoubleType()))).alias("weighted_avg_cost")
    )



    # 🚀 **Step 2: Join with high_priority_df**
    # This ensures we compare the metrics in `high_priority_df` against the corresponding ProductType averages
    high_priority_df = high_priority_df.join(
        weighted_avg_by_product, on="ProductType", how="left"
    )

    df = df.join(
        weighted_avg_by_product, on="ProductType", how="left"
    )

    # ✅ **Step 3: Filter out rows below 60% of the weighted averages**
    high_priority_df = high_priority_df.filter(
        (F.col(main_metric) >= 0.6 * F.col("weighted_avg_main")) &
        (F.col("Cost_Positive") >= 0.6 * F.col("weighted_avg_cost"))
    )

    # # 🚀 **Calculate Weighted Average at ProductType Level**
    # weighted_avg_by_product = df.groupBy("ProductType").agg(
    # (F.sum(F.col(main_metric).cast(DoubleType()) * F.col("Sales").cast(DoubleType())) / 
    #  F.sum(F.col("Sales").cast(DoubleType()))).alias("weighted_avg_main"),
    
    # (F.sum(F.col("Cost_Positive").cast(DoubleType()) * F.col("Sales").cast(DoubleType())) / 
    #  F.sum(F.col("Sales").cast(DoubleType()))).alias("weighted_avg_cost"))


    # weighted_avg_main = weighted_avg["weighted_avg_main"]
    # weighted_avg_cost = weighted_avg["weighted_avg_cost"]

    # # ✅ **Filter by 50% of weighted averages**
    # high_priority_df = high_priority_df.filter(
    #     (F.col(main_metric) >= 0.5 * weighted_avg_main) &
    #     (F.col('Cost_Positive') >= 0.5 * weighted_avg_cost)
    # )


      # ✅ **Auto-mark categories only for high-priority cases**
    high_priority_ids = high_priority_df.select(
        "BILL TO_CustomerID", "CommodityCode"
    ).distinct()

    
    # df = df.join(
    #     high_priority_ids.withColumnRenamed("BILL TO_CustomerID", "HP_CustomerID")
    #                     .withColumnRenamed("CommodityCode", "HP_CommodityCode"),
    #     (df["BILL TO_CustomerID"] == F.col("HP_CustomerID")) &
    #     (df["CommodityCode"] == F.col("HP_CommodityCode")),
    #     "left"
    # )

    # # ✅ Mark 'High-Priority' only for valid matches
    # df = df.withColumn(
    #     'Category',
    #     F.when(F.col("HP_CustomerID").isNotNull() & F.col("HP_CommodityCode").isNotNull(), "High-Priority")
    #     .otherwise("Regular")
    # ).drop("HP_CustomerID", "HP_CommodityCode")


    # from pyspark.sql.functions import lit

    # # # Step 1: Add 'Category' column to higher_df with value "High-Priority"
    # # higher_df = higher_df.withColumn("Category", lit("High-Priority"))

    # # # Step 2: Align the columns by adding missing columns with null values
    # # for col_name in df.columns:
    # #     if col_name not in higher_df.columns:
    # #         higher_df = higher_df.withColumn(col_name, lit(None))  # Add missing columns with null

    # # for col_name in higher_df.columns:
    # #     if col_name not in df.columns:
    # #         df = df.withColumn(col_name, lit(None))  # Add missing columns with null

    # # # Step 3: Perform the union (append the rows)
    # # df = df.unionByName(higher_df)

    # from pyspark.sql.functions import lit

    # from pyspark.sql.functions import lit, col

    # # Step 1: Add 'Category' column to higher_df with value "High-Priority"
    # higher_df = higher_df.withColumn("Category", lit("High-Priority"))

    # # Step 2: Align columns efficiently
    # # Get all unique column names from both DataFrames
    # all_columns = list(set(df.columns).union(set(higher_df.columns)))

    # # Select all columns, adding missing ones with null values
    # df_aligned = df.select([col(c) if c in df.columns else lit(None).alias(c) for c in all_columns])
    # higher_df_aligned = higher_df.select([col(c) if c in higher_df.columns else lit(None).alias(c) for c in all_columns])

    # # Step 3: Perform the union
    # df = df_aligned.unionByName(higher_df_aligned)



    # # 🚀 **Final result dataframe**
    # result_df = high_priority_df.select(
    #     "BILL TO_CustomerID",
    #     "CommodityCode",
    #     "final_multiplicative_score"
    # ).distinct()

    return high_priority_df, higher_df

def remove_duplicates_keep_max_qty_spark(processed_df):
    """
    Removes duplicate Customer-Product combinations by keeping the row with the higher Totalqty.

    Args:
    - processed_df (DataFrame): The output PySpark DataFrame from the process_grouped_data_spark function.

    Returns:
    - DataFrame: A cleaned PySpark DataFrame without duplicate Customer-Product combinations.
    """
    # Remove rows where CommodityCode is "UNKNOWN"
    cleaned_df = processed_df.filter(processed_df['CommodityCode'] != 'UNKNOWN')

    # Create a window partitioned by Customer ID and CommodityCode, ordered by Totalqty descending
    window_spec = Window.partitionBy('BILL TO_CustomerID', 'CommodityCode','ProductType').orderBy(F.desc('Totalqty'))

    # Add a row number column based on the window specification
    ranked_df = cleaned_df.withColumn('row_num', F.row_number().over(window_spec))

    # Filter to keep only the first row (highest Totalqty) per Customer-Product combination
    unique_df = ranked_df.filter(ranked_df['row_num'] == 1).drop('row_num')

    return unique_df


def calculate_average_deviation(processed_monthly_df: DataFrame, filtered_df: DataFrame) -> DataFrame:
    """
    Filters customer-commodity codes based on filtered_df, calculates the deviation of 
    customer-commodity return rate, return transaction rate, Tier_2_Costs_pct, and Tiers_1_2_Costs_pct 
    from the producttype-level rates, and averages the deviations at the specified granularity before 
    merging with filtered_df.

    Args:
    - processed_monthly_df (DataFrame): Aggregated PySpark DataFrame with computed metrics.
    - filtered_df (DataFrame): DataFrame at the target granularity to merge the final results with.

    Returns:
    - DataFrame: PySpark DataFrame with averaged deviations merged with filtered_df.
    """

    # Step 1: Filter based on customer-commodity combinations in filtered_df
    filtered_combinations = filtered_df.select(
        'BILL TO_CustomerID', 'PriceClass', 'ProductType', 'CustomerType',
        'BILL TO_CustomerName', 'CommodityCode'
    ).distinct()

    filtered_monthly_df = processed_monthly_df.join(
        filtered_combinations,
        ['BILL TO_CustomerID', 'PriceClass', 'ProductType', 'CustomerType',
         'BILL TO_CustomerName', 'CommodityCode'],
        'inner'
    )

    # Step 2: Aggregate at the ProductType level for all metrics
    producttype_level_df = filtered_monthly_df.groupBy(
        'Month', 'ProductType'
    ).agg(
        # Return rates
        F.sum('returnqty').alias('producttype_returnqty'),
        F.sum('Totalqty').alias('producttype_totalqty'),
        F.sum('countofreturntransactions').alias('producttype_return_transaction'),
        F.sum('countoftransactions').alias('producttype_total_transaction'),
        
        # Cost percentages
        F.sum('Cost_to_Serve_Tiers_1_2').alias('producttype_cost_to_serve'),
        F.sum('Sales').alias('producttype_sales'),
        F.sum('Tier_2_Costs').alias('producttype_tier_2_costs')
    ).withColumn(
        'producttype_return_rate_pct', 
        F.when(F.col('producttype_totalqty') != 0, 
               (F.col('producttype_returnqty') / F.col('producttype_totalqty')) * 100)
        .otherwise(0)
    ).withColumn(
        'producttype_return_transaction_pct',
        F.when(F.col('producttype_total_transaction') != 0, 
               (F.col('producttype_return_transaction') / F.col('producttype_total_transaction')) * 100)
        .otherwise(0)
    ).withColumn(
        'producttype_tiers_1_2_costs_pct',
        F.when(F.col('producttype_sales') != 0, 
               (F.col('producttype_cost_to_serve') / F.col('producttype_sales')) * 100)
        .otherwise(0)
    ).withColumn(
        'producttype_tier_2_costs_pct',
        F.when(F.col('producttype_sales') != 0, 
               (F.col('producttype_tier_2_costs') / F.col('producttype_sales')) * 100)
        .otherwise(0)
    )

    # Step 3: Join the producttype-level rates with the customer-commodity data
    comparison_df = filtered_monthly_df.join(
        producttype_level_df,
        ['Month', 'ProductType'],
        'left'
    )

    # Step 4: Calculate the deviations for all metrics
    comparison_df = comparison_df.withColumn(
        'deviation_return_rate_pct', 
        comparison_df['return_rate_pct'] - comparison_df['producttype_return_rate_pct']
    ).withColumn(
        'deviation_return_transaction_pct',
        comparison_df['return_transaction_pct'] - comparison_df['producttype_return_transaction_pct']
    ).withColumn(
        'deviation_tiers_1_2_costs_pct',
        comparison_df['Tiers_1_2_Costs_pct'] - comparison_df['producttype_tiers_1_2_costs_pct']
    ).withColumn(
        'deviation_tier_2_costs_pct',
        comparison_df['Tier_2_Costs_pct'] - comparison_df['producttype_tier_2_costs_pct']
    )

    # Step 5: Aggregate at the required granularity and average all deviations
    avg_deviation_df = comparison_df.groupBy(
        'BILL TO_CustomerID', 'PriceClass', 'ProductType', 'CustomerType',
        'BILL TO_CustomerName', 'CommodityCode'
    ).agg(
        F.avg('deviation_return_rate_pct').alias('avg_deviation_return_rate_pct'),
        F.avg('deviation_return_transaction_pct').alias('avg_deviation_return_transaction_pct'),
        F.avg('deviation_tiers_1_2_costs_pct').alias('avg_deviation_tiers_1_2_costs_pct'),
        F.avg('deviation_tier_2_costs_pct').alias('avg_deviation_tier_2_costs_pct')
    )

    # Step 6: Merge with filtered_df
    final_df = filtered_df.join(
        avg_deviation_df,
        ['BILL TO_CustomerID', 'PriceClass', 'ProductType', 'CustomerType',
         'BILL TO_CustomerName', 'CommodityCode'],
        'left'
    )

    return final_df


def process_monthly_grouped_data_spark(udm: DataFrame) -> DataFrame:
    """
    Groups data by ['Month', 'BILL TO_CustomerID', 'BILL TO_CustomerName', 'CommodityCode']
    and calculates return quantity, total quantity, return rate, cost to serve, and sales metrics.

    Args:
    - udm (DataFrame): Input PySpark DataFrame containing necessary columns.

    Returns:
    - DataFrame: Aggregated PySpark DataFrame with computed metrics.
    """

    # Aggregate all records (return + non-return) by Month and customer-commodity combination
    aggregated_df = udm.groupBy(
        'Month', 
        'BILL TO_CustomerID', 'PriceClass', 'ProductType', 'CustomerType',
        'BILL TO_CustomerName', 'CommodityCode'
    ).agg(
        F.sum('ShipQty').alias('Totalqty'),
        F.sum(F.when(udm['Return?'] == 1, udm['ShipQty']).otherwise(0)).alias('returnqty'),
        F.sum(F.when(udm['Return?'] == 1, udm['Returns']).otherwise(0)).alias('CostOfReturn'),
        F.sum('Cost to Serve').alias('Cost_to_Serve_Tiers_1_2'),
        F.sum('Net Revenue').alias('Sales'),
        F.sum('Variable Transaction Costs').alias('Tier_1_Costs'),
        F.sum('Semi-Variable Transaction Costs').alias('Tier_2_Costs'),
        F.count(F.when(udm['Return?'] == 1, udm['InvoiceIDLine'])).alias('countofreturntransactions'),
        F.count('InvoiceIDLine').alias('countoftransactions')
    )

    # Calculate additional metrics
    aggregated_df = aggregated_df.withColumn(
        'return_rate_pct', F.when(aggregated_df['Totalqty'] != 0, 
                                  (aggregated_df['returnqty'] / aggregated_df['Totalqty']) * 100)
                            .otherwise(0)
    )

    aggregated_df = aggregated_df.withColumn(
        'return_transaction_pct', F.when(aggregated_df['countoftransactions'] != 0, 
                                         (aggregated_df['countofreturntransactions'] / aggregated_df['countoftransactions']) * 100)
                                  .otherwise(0)
    )

    aggregated_df = aggregated_df.withColumn(
        'Tiers_1_2_Costs_pct', F.when(aggregated_df['Sales'] != 0, 
                                      (aggregated_df['Cost_to_Serve_Tiers_1_2'] / aggregated_df['Sales']) * 100)
                               .otherwise(0)
    )

    aggregated_df = aggregated_df.withColumn(
        'Tier_2_Costs1', aggregated_df['Cost_to_Serve_Tiers_1_2'] - aggregated_df['Tier_1_Costs']
    )

    aggregated_df = aggregated_df.withColumn(
        'Tier_2_Costs_pct', F.when(aggregated_df['Sales'] != 0, 
                                   (aggregated_df['Tier_2_Costs'] / aggregated_df['Sales']) * 100)
                            .otherwise(0)
    )

    return aggregated_df


def process_grouped_data_spark(udm: DataFrame) -> DataFrame:
    """
    Groups data by ['BILL TO_CustomerID', 'BILL TO_CustomerName', 'CommodityCode']
    and calculates return quantity, total quantity, return rate, cost to serve, and sales metrics.

    Args:
    - udm (DataFrame): Input PySpark DataFrame containing necessary columns.

    Returns:
    - DataFrame: Aggregated PySpark DataFrame with computed metrics.
    """

    # Aggregate data by grouping for all transactions (return and non-return combined)
    agg_df = udm.groupBy(
        'BILL TO_CustomerID', 'PriceClass', 'ProductType', 'CustomerType',
        'BILL TO_CustomerName', 'CommodityCode'
    ).agg(
        F.sum(F.when(F.col('Return?') == 1, F.col('ShipQty')).otherwise(0)).alias('returnqty'),
        F.sum(F.when(F.col('Return?') != 1, F.col('ShipQty')).otherwise(0)).alias('Totalqty'),
        F.sum(F.when(F.col('Return?') == 1, F.col('Returns')).otherwise(0)).alias('CostOfReturn'),
        F.sum(F.when(F.col('Return?') == 1, 1).otherwise(0)).alias('HasReturn'),
        F.sum('Net Revenue').alias('Sales'),   # Summing for both return & non-return
        F.sum('Variable Transaction Costs').alias('Tier_1_Costs'),  # Summing both
        F.sum('Semi-Variable Transaction Costs').alias('Tier_2_Costs'),  # Summing both
        F.sum('Cost to Serve').alias('Cost_to_Serve_Tiers_1_2')  # Summing both
    )

    # Calculate additional metrics
    agg_df = agg_df.withColumn(
        'return_rate_pct', F.when(agg_df['Totalqty'] != 0, 
                                  (agg_df['returnqty'] / agg_df['Totalqty']) * 100)
                            .otherwise(0)
    )

    agg_df = agg_df.withColumn(
        'Tiers_1_2_Costs_pct', F.when(agg_df['Sales'] != 0, 
                                      (agg_df['Cost_to_Serve_Tiers_1_2'] / agg_df['Sales']) * 100)
                               .otherwise(0)
    )

    agg_df = agg_df.withColumn(
        'Tier_2_Costs1', agg_df['Cost_to_Serve_Tiers_1_2'] - agg_df['Tier_1_Costs']
    )

    agg_df = agg_df.withColumn(
        'Tier_2_Costs_pct', F.when(agg_df['Sales'] != 0, 
                                   (agg_df['Tier_2_Costs'] / agg_df['Sales']) * 100)
                            .otherwise(0)
    )

    return agg_df
