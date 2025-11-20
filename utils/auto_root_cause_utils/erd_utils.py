# Spark Imports
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when
from pyspark.sql.window import Window
from pyspark.sql.functions import col, min, max, sum, mean, count
from pyspark.sql.functions import lit, add_months

def get_6m_change(udm_data, groupby_columns, source_column, output_column):
    """
    Compute Month-over-Month (MoM) Change and 6-Month Trending Performance stored in the output_column defined.
    aggregated at user-defined groupby level based on source_column provided.
    """
    
    # Convert invoice_date to 'YYYY-MM' format for monthly aggregation
    udm_data = udm_data.withColumn("year_month", F.date_format(F.col("invoice_date"), "yyyy-MM"))
    
    # Define window partitioned by defined groupby columns, ordered by month
    window_spec = Window.partitionBy(groupby_columns).orderBy("year_month")
    
    # Compute Previous Month's Cost
    udm_data = udm_data.withColumn("prev_month_cost", F.lag(source_column).over(window_spec))
    
    # Compute MoM Change (%)
    udm_data = udm_data.withColumn("mom_change",
        F.when(F.col("prev_month_cost").isNotNull(),
               ((F.col(source_column) - F.col("prev_month_cost")) / F.col("prev_month_cost")) * 100)
         .otherwise(None)
    )
    
    # Define window for last 6-month average
    rolling_window = Window.partitionBy(groupby_columns).orderBy("year_month").rowsBetween(-5, 0)

    # Compute 6-month Average MoM Trend
    udm_data = udm_data.withColumn(output_column, F.avg("mom_change").over(rolling_window))
    
    return udm_data


#### Functions for Derived Dimension Data
def get_product_derived_dimension_data(filtered_udm_table_df, udm_data_mapping):
    filtered_udm_table_df = filtered_udm_table_df.withColumn("sales_account_type", when(col(udm_data_mapping.get("OutsideSalesID","outside_sales_id")) != None, "Inside Sales").otherwise("Outside Sales"))

    return filtered_udm_table_df

def get_customer_derived_dimension_data(filtered_udm_table_df, udm_data_mapping):
    return filtered_udm_table_df

def get_customer_product_derived_dimension_data(filtered_udm_table_df, udm_data_mapping):
    return filtered_udm_table_df

def get_branch_customer_product_derived_dimension_data(filtered_udm_table_df, udm_data_mapping):
    return filtered_udm_table_df

#### Functions for Derived Fact Data
def get_product_derived_fact_data(filtered_udm_table_df, udm_data_mapping, primary_keys):
    returns_udm_table_df = filtered_udm_table_df.filter(col(udm_data_mapping.get("Return?","is_return")) == 1)

    return_count_df = returns_udm_table_df.groupBy(primary_keys).agg(F.countDistinct(udm_data_mapping.get("LedgerID","ledger_id")).alias("return_order_count"))

    order_count_df = filtered_udm_table_df.groupBy(primary_keys).agg(F.countDistinct(udm_data_mapping.get("LedgerID","ledger_id")).alias("order_count"))

    sku_count_df = filtered_udm_table_df.groupBy(primary_keys).agg(F.countDistinct(udm_data_mapping.get("ProductID","product_id")).alias("sku_count"))

    filtered_udm_table_df = filtered_udm_table_df.join(order_count_df, primary_keys, how="left")
    filtered_udm_table_df = filtered_udm_table_df.join(sku_count_df, primary_keys, how="left")
    filtered_udm_table_df = filtered_udm_table_df.join(return_count_df, primary_keys, how="left")

    return filtered_udm_table_df

def get_customer_derived_fact_data(filtered_udm_table_df, udm_data_mapping, primary_keys):
    returns_udm_table_df = filtered_udm_table_df.filter(col(udm_data_mapping.get("Return?","is_return")) == 1)

    return_count_df = returns_udm_table_df.groupBy(primary_keys).agg(F.countDistinct(udm_data_mapping.get("LedgerID","ledger_id")).alias("return_order_count"))

    order_count_df = filtered_udm_table_df.groupBy(primary_keys).agg(F.countDistinct(udm_data_mapping.get("LedgerID","ledger_id")).alias("order_count"))

    sku_count_df = filtered_udm_table_df.groupBy(primary_keys).agg(F.countDistinct(udm_data_mapping.get("ProductID","product_id")).alias("sku_count"))

    commodity_count_df = filtered_udm_table_df.groupBy(primary_keys).agg(F.countDistinct(udm_data_mapping.get("CommodityCode","commodity_code")).alias("commodity_count"))

    filtered_udm_table_df = filtered_udm_table_df.join(order_count_df, primary_keys, how="left")
    filtered_udm_table_df = filtered_udm_table_df.join(sku_count_df, primary_keys, how="left")
    filtered_udm_table_df = filtered_udm_table_df.join(commodity_count_df, primary_keys, how="left")
    filtered_udm_table_df = filtered_udm_table_df.join(return_count_df, primary_keys, how="left")

    return filtered_udm_table_df

def get_customer_product_derived_fact_data(filtered_udm_table_df, udm_data_mapping, primary_keys=None):
    returns_udm_table_df = filtered_udm_table_df.filter(col(udm_data_mapping.get("Return?","is_return")) == 1)

    return_count_df = returns_udm_table_df.groupBy(primary_keys).agg(F.countDistinct(udm_data_mapping.get("LedgerID","ledger_id")).alias("return_order_count"))

    order_count_df = filtered_udm_table_df.groupBy(primary_keys).agg(F.countDistinct(udm_data_mapping.get("LedgerID","ledger_id")).alias("order_count"))

    filtered_udm_table_df = filtered_udm_table_df.join(order_count_df, primary_keys, how="left")
    filtered_udm_table_df = filtered_udm_table_df.join(return_count_df, primary_keys, how="left")

    return filtered_udm_table_df

def get_branch_customer_product_derived_fact_data(filtered_udm_table_df, udm_data_mapping, primary_keys=None):
    # returns_udm_table_df = filtered_udm_table_df.filter(col(udm_data_mapping.get("Return?","is_return")) == 1)

    # return_count_df = returns_udm_table_df.groupBy(primary_keys).agg(F.countDistinct(udm_data_mapping.get("LedgerID","ledger_id")).alias("return_order_count"))

    # order_count_df = filtered_udm_table_df.groupBy(primary_keys).agg(F.countDistinct(udm_data_mapping.get("LedgerID","ledger_id")).alias("order_count"))

    # filtered_udm_table_df = filtered_udm_table_df.join(order_count_df, primary_keys, how="left")
    # filtered_udm_table_df = filtered_udm_table_df.join(return_count_df, primary_keys, how="left")

    return filtered_udm_table_df


#### Functions for Derived Fact Agg Data
def get_product_derived_fact_agg_data(filtered_udm_table_df, udm_data_mapping):
    return filtered_udm_table_df

def get_customer_derived_fact_agg_data(filtered_udm_table_df, udm_data_mapping):
    return filtered_udm_table_df

def get_customer_product_derived_fact_agg_data(filtered_udm_table_df, udm_data_mapping):
    return filtered_udm_table_df

def get_branch_customer_product_derived_fact_agg_data(filtered_udm_table_df, udm_data_mapping):
    return filtered_udm_table_df


#### Functions for Derived Dimension Agg Data
def get_product_derived_dimension_agg_data(filtered_udm_table_df, udm_data_mapping):
    return filtered_udm_table_df

def get_customer_derived_dimension_agg_data(filtered_udm_table_df, udm_data_mapping):
    filtered_udm_table_df = filtered_udm_table_df.withColumn(
            "net_customer_spend",
            when(col(udm_data_mapping.get("Net Revenue","net_revenue")) <= 500000, "$0-$500k")
            .when((col(udm_data_mapping.get("Net Revenue","net_revenue")) > 500000) & (col(udm_data_mapping.get("Net Revenue","net_revenue")) <= 999999), "$501k-$999k")  # Corrected upper bound
            .when((col(udm_data_mapping.get("Net Revenue","net_revenue")) >= 1000000) & (col(udm_data_mapping.get("Net Revenue","net_revenue")) <= 2500000), "$1M-$2.5M")
            .otherwise(">$2.5M")
        )
    
    return filtered_udm_table_df

def get_customer_product_derived_dimension_agg_data(filtered_udm_table_df, udm_data_mapping):
    return filtered_udm_table_df

def get_branch_customer_product_derived_dimension_agg_data(filtered_udm_table_df, udm_data_mapping):
    return filtered_udm_table_df


#### Functions for Common Fact Data
def get_aggregated_fact_data(filtered_udm_table_df, udm_data_mapping, groupby_cols):
    filtered_udm_table_df = filtered_udm_table_df.groupBy(groupby_cols).agg(
        min(col("pricing_pct")).alias("min_pricing_pct"),
        max(col("pricing_pct")).alias("max_pricing_pct"),
        sum(col("weighted_pricing_pct")).alias("weighted_pricing_pct"),
        min(col("total_order_quantity")).alias("total_order_quantity"),

        sum(col(udm_data_mapping.get("ShipQty","order_quantity"))).alias("order_quantity"),
        sum(col("return_quantity")).alias("return_quantity"),
        mean(col(udm_data_mapping.get("Price","actual_pricing"))).alias("actual_pricing"),
        mean(col(udm_data_mapping.get("PriceOverridden","system_pricing"))).alias("system_pricing"),

        sum(col(udm_data_mapping.get("Returns","returns_cost"))).alias("returns_cost"),

        sum(col(udm_data_mapping.get("COGS","cogs"))).alias("cogs"),
        sum(col("return_sales")).alias("return_sales"),
        sum(col(udm_data_mapping.get("PriceOverrideCost","price_override_cost"))).alias("price_override_cost"),


        sum(col(udm_data_mapping.get("Variable Transaction Costs", "tier_1_cost"))).alias("tier_1_cost"),

        sum(col(udm_data_mapping.get("Semi-Variable Transaction Costs", "tier_2_cost"))).alias("tier_2_cost"),
        sum(col(udm_data_mapping.get("Semi-Fixed Systemic Costs", "tier_3_cost"))).alias("tier_3_cost"),
        sum(col(udm_data_mapping.get("Fixed Systemic Costs", "tier_4_cost"))).alias("tier_4_cost"),
        sum(col(udm_data_mapping.get("Cost to Serve", "cost_to_serve"))).alias("cost_to_serve"),

        sum(col(udm_data_mapping.get("Tier 1 Costs less COGS", "tier_1_cost_less_cogs"))).alias("tier_1_cost_less_cogs"),
        sum(col(udm_data_mapping.get("Return Handling Ops Labor", "return_handling_labor_cost"))).alias("return_handling_labor_cost"),
        sum(col(udm_data_mapping.get("Return Freight", "return_freight_cost"))).alias("return_freight_cost"),
        sum(col(udm_data_mapping.get("Handling Ops Labor", "order_handling_cost"))).alias("order_handling_cost"),

        sum(col(udm_data_mapping.get("Enterprise Profit", "net_enterprise_profit"))).alias("net_enterprise_profit"),
        sum(col(udm_data_mapping.get("Profit Post Tier 2", "enterprise_profit"))).alias("enterprise_profit"),
        sum(col(udm_data_mapping.get("GrossProift", "gross_profit"))).alias("gross_profit"),
        sum(col(udm_data_mapping.get("Net Revenue", "net_revenue"))).alias("net_revenue"),

        mean(col("returns_cost_6m_change")).alias("returns_cost_6m_change"),
        mean(col("tier_2_cost_6m_change")).alias("tier_2_cost_6m_change"),
        mean(col("cost_to_serve_6m_change")).alias("cost_to_serve_6m_change")
    )

    return filtered_udm_table_df


#### Function for Return Reason Code Calculation
def get_return_reason_code(processed_udm_df, udm_data_mapping, primary_keys):
    # Convert DecimalType to FloatType for efficiency
    processed_udm_df = processed_udm_df.withColumn("return_quantity", col("return_quantity").cast("double"))

    # Select relevant columns and group by necessary keys
    return_codes_df = (
        processed_udm_df
        .groupBy(primary_keys + [udm_data_mapping.get("ReturnCodeBroad","return_reason_code")])
        .sum('return_quantity')
        .fillna({udm_data_mapping.get("ReturnCodeBroad","return_reason_code"): "UNKNOWN"})
    )

    # Convert to Pandas after DecimalType Fix
    return_codes_pd = return_codes_df.toPandas()

    # Pivot the DataFrame
    return_codes_pd = return_codes_pd.pivot(index=primary_keys, columns=udm_data_mapping.get("ReturnCodeBroad","return_reason_code"), values='sum(return_quantity)').reset_index()

    return_codes_pd.columns = [c.lower().replace(" ","_").replace("/","_") + "_pct" if c not in primary_keys else c for c in return_codes_pd.columns]

    # Select all numeric columns dynamically (excluding primary_keys)
    numeric_cols = [c for c in return_codes_pd.columns if c not in primary_keys]

    # Replace positive values with 0
    return_codes_pd[numeric_cols] = return_codes_pd[numeric_cols].where(return_codes_pd[numeric_cols] <= 0, 0)

    # Compute total return quantity
    return_codes_pd["total_return_quantity"] = return_codes_pd[numeric_cols].sum(axis=1)

    # Normalize numeric columns by total_quantity
    return_codes_pd[numeric_cols] = return_codes_pd[numeric_cols].astype(float).div(return_codes_pd["total_return_quantity"], axis=0)

    # Ensure all numeric values are floats
    return_codes_pd[numeric_cols] = return_codes_pd[numeric_cols].astype(float)

    return_codes_pd = return_codes_pd[primary_keys+numeric_cols].reset_index(drop=True)
    return return_codes_pd

    