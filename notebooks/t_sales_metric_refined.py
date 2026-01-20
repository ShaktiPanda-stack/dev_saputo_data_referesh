"""
Standalone script version of t_sales_metric notebook
Run with: python t_sales_metric_refined.py --app_env dev --triggered_on "2025-11-17 10:00:00"
"""

import argparse
import os
import sys
import yaml
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark.sql.functions import col, lit, when
from pyspark.sql.functions import col, round as spark_round


# Add project root to path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname('/Workspace/Users/lovepreet.singh@mathco.com/Huron'), '..'))
sys.path.append(PROJECT_ROOT)

# Import utilities
from utils.blob_operations.pyspark_blob import read_parquet_from_blob
from utils.generic_utils.add_sales_metric import add_sales_metrics, add_other_metrics,add_other_metrics_fiscal_year
from utils.generic_utils.group_sales_metric import groupby_statistics,groupby_statistics_fiscal_year
from utils.generic_utils.remove_duplicate_distribution_channel import deduplicate_distribution_channels
from utils.generic_utils.months_calculations import analyze_cost_metrics
from utils.generic_utils.months_calculations import  analyze_best_months
from utils.generic_utils.output_validations import Validations
from utils.postgres_operations.pyspark_postgres import write_to_postgres


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Run t_sales_metric data pipeline')
    parser.add_argument('--app_env', type=str, default='dev', 
                       help='Application environment (dev/prod)')
    parser.add_argument('--triggered_on', type=str, 
                       default=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                       help='Trigger timestamp')
    # return parser.parse_args()
    args, _ = parser.parse_known_args()   # <-- FIX HERE
    return args


def load_configs(pipeline_parent_directory, nb_name):
    """Load all configuration files"""
    pipeline_config_path = os.path.join(pipeline_parent_directory, 
                                       "/Workspace/Users/lovepreet.singh@mathco.com/Huron/configs/pipeline_configs.yaml")
    filepath_config_path = os.path.join(pipeline_parent_directory, 
                                       "/Workspace/Users/lovepreet.singh@mathco.com/Huron/configs/filepath_configs.yaml")
    validation_config_path = os.path.join(pipeline_parent_directory, 
                                         f"/Workspace/Users/lovepreet.singh@mathco.com/Huron/configs/data_validations/{nb_name}.yaml")
    
    with open(pipeline_config_path, "r") as f:
        pipeline_configs = yaml.safe_load(f)
    
    with open(filepath_config_path, "r") as f:
        filepath_configs = yaml.safe_load(f)
    
    with open(validation_config_path, "r") as f:
        validation_configs = yaml.safe_load(f)
    
    return pipeline_configs, filepath_configs, validation_configs


def create_spark_session(app_name="t_sales_metric"):
    """Create and configure Spark session"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.caseSensitive", "true") \
        .getOrCreate()
    
    return spark


def main():
    # Configuration
    nb_name = 't_sales_metric'
    
    # Parse arguments
    args = parse_arguments()
    app_env = args.app_env
    triggered_on = args.triggered_on
    
    print(f"Environment: {app_env}")
    print(f"Triggered On: {triggered_on}")
    
    # Load environment variables
    load_dotenv()
    storage_account = os.getenv("blob_storage_account")
    container_name = os.getenv("blob_container_name")
    sas_token = os.getenv("blob_sas_token")
    
    # Load configurations
    pipeline_parent_directory = PROJECT_ROOT
    pipeline_configs, filepath_configs, validation_configs = load_configs(
        pipeline_parent_directory, nb_name
    )
    
    # Create Spark session
    spark = create_spark_session(nb_name)
    
    try:
        # Load raw data from blob
        print("Loading data from blob storage...")
        udm_file_path = filepath_configs['udm_cheese']
        raw_data = read_parquet_from_blob(
            file_path=udm_file_path,
            spark=spark,
            storage_account=storage_account,
            container_name=container_name,
            sas_token=sas_token
        )
        


        req_cols = pipeline_configs[nb_name]['req_cols']
        df = raw_data.select(req_cols)

        # get max date as "yyyy-MM-dd" string on driver
        max_date_row = df.select(F.date_format(F.max("Invoice Date"), "yyyy-MM-dd").alias("max_date_str")).collect()
        if not max_date_row or max_date_row[0]["max_date_str"] is None:
            raise ValueError("No Invoice Date found in data")

        max_date_str = max_date_row[0]["max_date_str"]  # e.g., "2025-03-29"

        # compute two_years_ago as a Spark Column (date)
        two_years_ago = F.add_months(F.to_date(F.lit(max_date_str), "yyyy-MM-dd"), -24)

        # filter (ensure Invoice Date is of date/timestamp type; if it's a string, cast to date)
        df = df.filter(F.to_date(F.col("Invoice Date")) >= two_years_ago)

        # # Fetch minimum and maximum date
        date_stats = df.agg(
            F.min('Invoice Date').alias("min_date"),
            F.max('Invoice Date').alias("max_date")
        )

        # Show results
        date_stats.show()


        df = df.filter(df["IBP Group 2 Text"] != "Production/WIP")

        
        # Create derived outcome metrics
        print("Creating derived metrics...")
        df = df.withColumn('Tier 2 and 3 Total', col("Tier 2 Total") + col("Tier 3 Total"))
        df = df.withColumn('Interplant Logistics', 
                          col("Interplant Freight | Allocation") + 
                          col("WH Handling - Interplant | Allocation"))
        df = df.withColumn('Plant Ops', 
                          col("Plant Ops - Non-Value | Allocation") + 
                          col("Plant Ops - Conversion - Filling & Packaging | Allocation") + 
                          col("Plant Ops - Conversion - Production & Processing | Allocation") + 
                          col("Plant Ops - Milk Receiving | Allocation") + 
                          col("Plant Ops - Testing | Allocation"))
        
        # Drop intermediate columns
        df = df.drop('Interplant Freight | Allocation', 
                    'WH Handling - Interplant | Allocation',
                    'Plant Ops - Non-Value | Allocation',
                    'Plant Ops - Conversion - Filling & Packaging | Allocation',
                    'Plant Ops - Conversion - Production & Processing | Allocation',
                    'Plant Ops - Testing | Allocation',
                    'Plant Ops - Milk Receiving | Allocation')
        
        # Rename columns
        print("Renaming columns...")
        column_mapping = {
            "L3 Customer Name": "l3_customer",
            "Product Distance Zone": "product_distance",
            "Primary Production Plant Name": "production_plant",
            "Fulfillment Facility Name": "fulfillment_warehouse",
            "IBP Group 2 Text": "product_type",
            "Kosher Indicator": "kosher_indicator",
            "Organic Indicator": "organic_indicator",
            "Non-GMO Indicator": "gmo_indicator",
            "Lactose Free Indicator": "lactose_indicator",
            "Product Brand Type Text": "brand",
            "Asset Type": "packaging",
            "Cost to Serve": "cost_to_serve",
            "Tier 2 Total": "tier_2",
            "Tier 3 Total": "tier_3",
            "Tier 2 and 3 Total": "tier_2_3",
            "Enterprise Profit": "enterprise_profit",
            'Interplant Logistics': "interplant_logistics",
            "Plant Ops": "plant_ops",
            "Net Revenue" : "net_revenue",
            "Order Type - Dairy Cheese" : 'order_type'
        }
        
        for df_col, db_col in column_mapping.items():
            df = df.withColumnRenamed(df_col, db_col)
        
        # Add sales metrics
        print("Adding sales metrics...")
        cost_column = pipeline_configs[nb_name]['cost_col_origin']
        result_df = add_sales_metrics(df, outcome_metric=cost_column, total_sales_column = 'net_revenue')
        
        # Group by statistics
        print("Calculating statistics...")
        group_cols = pipeline_configs[nb_name]['level_of_data']
        metric_cols = pipeline_configs[nb_name]['cost_cols']
        # final_df = groupby_statistics(result_df, group_cols, metric_cols, stats=['median'])
        final_df = groupby_statistics_fiscal_year(result_df, group_cols, metric_cols, 
                        stats=['median'])
        
        # Add other metrics
        print("Adding other metrics...")
        group_cols  = pipeline_configs[nb_name]['level_of_data']
        sum_cols = pipeline_configs[nb_name]['sum_cols']
        # final_df_metrics = add_other_metrics(df, group_cols, sum_cols)
        final_df_metrics = add_other_metrics_fiscal_year(df, group_cols, sum_cols )
        
        # Rename metric columns
        column_mapping_metrics = {'net_revenue_sum' : "net_revenue" ,
            "Order Quantity - Pounds_sum": "order_qty_lbs",
            "total_records": "order_frequency"
        }
        
        # Rename columns
        for df_col, db_col in column_mapping_metrics.items():
           final_df_metrics = final_df_metrics.withColumnRenamed(df_col, db_col)


        group_cols.append('fiscal_year')
        final_df = final_df.fillna('UNKNOWN')
        final_df_metrics = final_df_metrics.fillna('UNKNOWN')
        merged_df = final_df.join(final_df_metrics, on=group_cols, how="left")   

        # def safe_join(left_df, right_df, join_keys):
        #     left = left_df.alias("l")
        #     right = right_df.alias("r")

        #     right_unique = [c for c in right.columns if c not in join_keys and c not in left.columns]

        #     return left.join(right, join_keys, "left") \
        #             .select("l.*", *[F.col(f"r.{c}") for c in right_unique])

        # for df_col, db_col in column_mapping_metrics.items():
        #     final_df_metrics = final_df_metrics.withColumnRenamed(df_col, db_col)
   
        
        # Fill nulls and merge
        # final_df = final_df.fillna('UNKNOWN')
        # final_df_metrics = final_df_metrics.fillna('UNKNOWN')
        # # merged_df = final_df.join(final_df_metrics, on=group_cols, how="left")
        # merged_df = safe_join(final_df, final_df_metrics, group_cols)

        
        # # Analyze cost metrics by month
        # print("Analyzing monthly cost metrics...")
        # df = df.withColumn("Invoice Date", F.to_date(col("Invoice Date")))
        # cost_metrics = pipeline_configs[nb_name]['cost_col_origin']
        # result_month = analyze_cost_metrics(
        #     df=df,
        #     cost_metrics=cost_metrics,
        #     dimensions=group_cols,
        #     date_column="Invoice Date"
        # )
         

        # Best months computation
        # result_best = analyze_best_months(
        #     df=df,
        #     cost_metrics=cost_metrics,
        #     dimensions=group_cols,
        #     date_column="Invoice Date"
        # )

        # # Merge both results, continue using result_month
        # result_month = result_month.join(
        #     result_best,
        #     on=group_cols,
        #     how="outer"
        # ) 




        
        # Merge with monthly results
        # result_month = result_month.fillna('UNKNOWN')


        # merged_df = merged_df.join(result_month, on=group_cols, how="left")replaced join with safe join below to avoid error
        # merged_df = safe_join(merged_df, result_month, group_cols)


        
        # Add load date
        merged_df = merged_df.withColumn("load_date", lit(triggered_on))
        
        # Map indicator columns
        print("Mapping indicator columns...")
        indicator_mappings = {
            "kosher_indicator": {1: "Kosher", 0: "Non - Kosher"},
            "organic_indicator": {1: "Organic", 0: "Non - Organic"},
            "gmo_indicator": {1: "Non - GMO", 0: "GMO"},
            "lactose_indicator": {1: "Lactose - Free", 0: "Contains Lactose"}
        }
        
        for col_name, mapping in indicator_mappings.items():
            if col_name in merged_df.columns:
                merged_df = merged_df.withColumn(
                    col_name,
                    when(col(col_name) == 1, mapping[1])
                    .when(col(col_name) == 0, mapping[0])
                    .otherwise("Other")
                )
        
        # Drop unnecessary columns
        # merged_df = merged_df.drop(
        #     "worst_month_cost_to_serve_value",
        #     "worst_month_tier_2_value",
        #     "worst_month_tier_3_value",
        #     "worst_month_tier_2_3_value",
        #     "worst_month_enterprise_profit_value",
        #     "worst_month_interplant_logistics_value",
        #     "worst_month_plant_ops_value"
        #     "worst_month_net_revenue_value",
        #     "best_month_cost_to_serve_value",
        #     "best_month_tier_2_value",
        #     "best_month_tier_3_value",
        #     "best_month_tier_2_3_value",
        #     "best_month_enterprise_profit_value",
        #     "best_month_interplant_logistics_value",
        #     "best_month_plant_ops_value",
        #     "best_month_net_revenue_value"
        #     )
        
        # Round values
        # print("Rounding values...")
        # cost_cols = pipeline_configs[nb_name]['cost_cols']
        # cost_sum_cols = [str(i) + "_sum" for i in cost_cols 
        #                 if not (i.endswith('_pct_sales') or i.endswith('_per_lb'))]
        # cost_cols = [str(i) + "_median" for i in cost_cols]
        # other_metrics = ['net_revenue', 'order_qty_lbs'] + cost_sum_cols
        
        # for c in cost_cols:
        #     merged_df = merged_df.withColumn(c, F.round(col(c), 3))
        
        # for c in other_metrics:
        #     merged_df = merged_df.withColumn(c, F.round(col(c), 0))

        print("Rounding values...")
        cost_cols = pipeline_configs[nb_name]['cost_cols']
        cost_sum_cols = [str(i)+"_sum" for i in cost_cols if not (i.endswith('_pct_sales') or i.endswith('_per_lb'))]
        cost_cols  = [str(i)+"_median" for i in cost_cols]
        other_metrics = ['net_revenue', 'order_qty_lbs']
        other_metrics = other_metrics + cost_sum_cols


        for c in cost_cols:
            merged_df = merged_df.withColumn(c, F.round(F.col(c), 4))

        for c in other_metrics:
            merged_df = merged_df.withColumn(c, F.round(F.col(c), 0))






        merged_df = merged_df.drop("net_revenue")

     

        # existing_cols = set(merged_df.columns)
        # # Round medians to 3 decimals
        # for base in pipeline_configs[nb_name]['cost_cols']:
        #     median_col = f"{base}_median"
        #     if median_col in existing_cols:
        #         merged_df = merged_df.withColumn(median_col, spark_round(col(median_col), 3))

        # # Round sums + core metrics to 0 decimals
        # targets = ['net_revenue_sum', 'order_qty_lbs'] + pipeline_configs[nb_name]['cost_cols']

        # for base in targets:
        #     # Check naming order preference:
        #     candidates = [
        #         base,
        #         f"{base}_sum",
        #         f"{base}_median"
        #     ]

        #     match = next((c for c in candidates if c in existing_cols), None)

        #     if match:
        #         merged_df = merged_df.withColumn(match, spark_round(col(match), 0))
        #     else:
        #         print(f"⚠ Skipping (not found): {base}")

        
     
        # # Run validations
        # print("Running data validations...")
        # validator = Validations(merged_df, validation_configs)
        # validation_report_df = validator.run()
        # validation_report_df = validation_report_df.withColumn("load_date", lit(triggered_on))
        # validation_report_df = validation_report_df.withColumn(
        #     "notebook",
        #     lit(" ".join([each.title() for each in nb_name.split("_")]))
        # )
        # validation_report_df = validation_report_df.select([
        #     "notebook", "validation_type", "column", "rule", "message", "status", "load_date"
        # ])
        app_env="prod"
        # Connection details
        connection_details = {
            "jdbc_url": os.environ["postgres_url"],
            "user": os.environ["postgres_user"],
            "password": os.environ["postgres_password"]
        }
        
        # # Write validation report
        # print("Writing validation report...")
        # pg_table_name = f"{pipeline_configs[app_env]['pg_schema_name']}.data_validation_report"
        # write_to_postgres(
        #     validation_report_df,
        #     pg_table_name,
        #     mode='append',
        #     connection_details=connection_details,
        #     spark=spark
        # )
        
        # # Check DQM score
        # calculated_dqm_score = validator.calculate_dqm_score()
        # print(f"DQM Score: {calculated_dqm_score:.1f}%")
        
        # if calculated_dqm_score < validation_configs.get("validation_score_required", 75):
        #     raise Exception(
        #         f"Pipeline Stopped - DQM Score {calculated_dqm_score:.1f}% is below "
        #         f"threshold {validation_configs.get('validation_score_required', 75)}%"
        #     )
        
        # print("Data validation passed! Writing data to PostgreSQL...")
        
        # Write final data
        result = write_to_postgres(
            df=merged_df,
            table_name=f"ca_{app_env}.t_sales_metric",
            mode="overwrite",
            connection_details=connection_details,
            spark=spark
        )
        
        print("Pipeline completed successfully!")
        
    except Exception as e:
        print(f"Pipeline failed with error: {str(e)}")
        raise
    
    finally:
        
        df.count()
        print("Last Block Executed")



if __name__ == "__main__":
    main()