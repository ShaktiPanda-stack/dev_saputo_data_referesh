import threading
import argparse
import os
import sys
import yaml
import threading
from pyspark.sql.functions import when
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, concat_ws



# Add project root to path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname('/Workspace/Users/lovepreet.singh@mathco.com/Huron'), '..'))
sys.path.append(PROJECT_ROOT)

from utils.blob_operations.pyspark_blob import read_parquet_from_blob
from utils.generic_utils.add_sales_metric import add_sales_metrics, add_other_metrics
from utils.generic_utils.group_sales_metric import groupby_statistics
from utils.generic_utils.remove_duplicate_distribution_channel import deduplicate_distribution_channels
from utils.rfm_utils.calculate_rfm_score import rfm_analysis, cost_rfm_pipeline
from utils.rfm_utils.add_benchmarks import add_benchmark_columns
from utils.generic_utils.months_calculations import  analyze_cost_metrics
from utils.generic_utils.column_renaming import clean_column_name
# from utils.generic_utils.output_validations import Validations
from utils.postgres_operations.pyspark_postgres import write_to_postgres, read_from_postgres


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Run t_benchmark data pipeline')
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
    # validation_config_path = os.path.join(pipeline_parent_directory, 
    #                                      f"/Workspace/Users/lovepreet.singh@mathco.com/Huron/configs/data_validations/{nb_name}.yaml")
    
    with open(pipeline_config_path, "r") as f:
        pipeline_configs = yaml.safe_load(f)
    
    with open(filepath_config_path, "r") as f:
        filepath_configs = yaml.safe_load(f)
    
    # with open(validation_config_path, "r") as f:
    #     validation_configs = yaml.safe_load(f)
    
    return pipeline_configs, filepath_configs   


def create_spark_session(app_name="t_rfm_outliers_bad"):
    """Create and configure Spark session"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.caseSensitive", "true") \
        .getOrCreate()

    return spark


def main():
    # Configuration
    nb_name = 't_rfm_outliers'

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
    print("Load Env Variables")

    


    



    # Load configurations
    pipeline_parent_directory = PROJECT_ROOT
    pipeline_configs, filepath_configs = load_configs(
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
        
        # Filter required columns
        print("Filtering required columns...")
        req_cols = pipeline_configs[nb_name]['req_cols']
        df = raw_data.select(req_cols)
        
        # Get max date and filter data
        max_date_row = df.select(
            F.date_format(F.max("Invoice Date"), "yyyy-MM-dd").alias("max_date_str")
        ).collect()
        
        if not max_date_row or max_date_row[0]["max_date_str"] is None:
            raise ValueError("No Invoice Date found in data")
        
        max_date_str = max_date_row[0]["max_date_str"]
        two_years_ago = F.add_months(F.to_date(F.lit(max_date_str), "yyyy-MM-dd"), -24)
        
        df = df.filter(F.to_date(F.col("Invoice Date")) >= two_years_ago)
        # df = df.filter(df["IBP Group 2 Text"] != "Production/WIP")
        
        # Show date range
        date_stats = df.agg(
            F.min('Invoice Date').alias("min_date"),
            F.max('Invoice Date').alias("max_date")
        )
        print("Date range:")
        date_stats.show()
        
        # Deduplicate distribution channels
        df = df.filter(df["IBP Group 2 Text"] != "Production/WIP")
        print("Deduplicating distribution channels...")
        df = deduplicate_distribution_channels(df)
        
        # Create outcome metrics
        df = df.withColumn('Tier 2 and 3 Total', col("Tier 2 Total") + col("Tier 3 Total"))
        df = df.withColumn('Interplant Logistics', col("Interplant Freight | Allocation") + col("WH Handling - Interplant | Allocation"))
        df = df.withColumn('Plant Ops', col("Plant Ops - Non-Value | Allocation") + col("Plant Ops - Conversion - Filling & Packaging | Allocation") +  col("Plant Ops - Conversion - Production & Processing | Allocation")  +  col("Plant Ops - Milk Receiving | Allocation") +   col("Plant Ops - Testing | Allocation"))
                
        
        # Create cohort column
        df = df.withColumn("distribution_ibp", concat_ws(" | ", col("Distribution Channel Text"), col("IBP Group 2 Text")))

        print("Mappings")        
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
            "Cost to Serve" : "cost_to_serve",
            "Tier 2 Total" : "tier_2",
            "Tier 3 Total" : "tier_3",
            "Tier 2 and 3 Total" : "tier_2_3",
            "Enterprise Profit" : "enterprise_profit",
            'Interplant Logistics' :    "interplant_logistics",
            "Plant Ops" : "plant_ops",
            "Net Revenue": "net_revenue"

        }

        # Rename columns
        for df_col, db_col in column_mapping.items():
            df = df.withColumnRenamed(df_col, db_col)
        
        cost_column =  pipeline_configs[nb_name]['cost_col_origin']
        result_df = add_sales_metrics(df,outcome_metric = cost_column,total_sales_column = "net_revenue" )


        result_df = result_df.drop('Interplant Freight | Allocation',"Interplant Freight | Allocation",'WH Handling - Interplant | Allocation','Plant Ops - Non-Value | Allocation','Plant Ops - Conversion - Filling & Packaging | Allocation','Plant Ops - Conversion - Production & Processing | Allocation','Plant Ops - Testing | Allocation','Plant Ops - Milk Receiving | Allocation','Distribution Channel Text','Net Revenue','Order Quantity - Pounds', 'Tier 1 Total', 'Tier 4 Total')
        
        app_env="prod"
        # Connection details
        connection_details = {
            "jdbc_url": os.environ["postgres_url"],
            "user": os.environ["postgres_user"],
            "password": os.environ["postgres_password"]
        }
        
        print("Reading from DB")
        benchmarks = read_from_postgres(table_name=f"ca_{app_env}.t_benchmark", query=None,  connection_details=connection_details,spark=spark).cache()

        benchmarks = benchmarks.withColumn("benchmark", col("bad_benchmark"))
        

        # Define cost columns to analyze
        cost_columns = pipeline_configs[nb_name]['cost_cols']
        
        # Apply the function
        result_with_benchmarks = add_benchmark_columns(result_df, benchmarks, cost_columns)


        cost_cols = pipeline_configs[nb_name]['cost_cols']
        # cost_cols = ['cost_to_serve']
        dimension_cols = pipeline_configs[nb_name]["level_of_data"]


        max_date_row = result_with_benchmarks.select(F.date_format(F.max("Invoice Date"), "yyyy-MM-dd").alias("max_date_str")).collect()
        max_date_str = max_date_row[0]["max_date_str"] 
        rfm_results = cost_rfm_pipeline(
                                        result_with_benchmarks,
                                        cost_cols,
                                        dimension_cols,
                                        transaction_date_col = 'Invoice Date',
                                        reference_date = max_date_str,
                                        n_bins = 3
                                    ) 

        # Map indicator columns
        print("Mapping indicators")
        indicator_mappings = {
            "kosher_indicator": {1: "Kosher", 0: "Non-Kosher"},
            "organic_indicator": {1: "Organic", 0: "Non-Organic"},
            "gmo_indicator": {1: "Non-GMO", 0: "GMO"},
            "lactose_indicator": {1: "Lactose-Free", 0: "Contains Lactose"}
        }

        for col_name, mapping in indicator_mappings.items():
            if col_name in rfm_results.columns:
                rfm_results = rfm_results.withColumn(
                    col_name,
                    when(col(col_name) == 1, mapping[1])
                    .when(col(col_name) == 0, mapping[0])
                    .otherwise("Other")
                )


        def fill_nulls_by_pattern(df):
            """
            Fill null values based on column name patterns using fillna
            
            Args:
                df: Input DataFrame
            
            Returns:
                DataFrame with nulls filled based on column patterns
            """
            
            # Get all column names
            all_columns = df.columns
            
            # Identify columns by their suffixes
            recency_frequency_cols = [c for c in all_columns if c.endswith('_recency') or c.endswith('_frequency')]
            segment_cols = [c for c in all_columns if c.endswith('_segment')]
            
            # Create fill dictionaries
            fill_dict_numeric = {col_name: 0 for col_name in recency_frequency_cols}
            fill_dict_string = {col_name: "NA" for col_name in segment_cols}
            
            # Combine dictionaries and fill
            fill_dict = {**fill_dict_numeric, **fill_dict_string}
            df_filled = df.fillna(fill_dict)
            
            return df_filled


        # Usage
        df_filled = fill_nulls_by_pattern(rfm_results)  
        columns_to_drop = [
            "net_revenue_pct_sales",
            "net_revenue_rf",
            "net_revenue_per_lb_rf",
            "cost_to_serve_rf",
            "cost_to_serve_pct_sales_rf",
            "cost_to_serve_per_lb_rf",
            "tier_2_rf",
            "tier_2_pct_sales_rf",
            "tier_2_per_lb_rf",
            "tier_3_rf",
            "tier_3_pct_sales_rf",
            "tier_3_per_lb_rf",
            "tier_2_3_rf",
            "tier_2_3_pct_sales_rf",
            "tier_2_3_per_lb_rf",
            "enterprise_profit_rf",
            "enterprise_profit_pct_sales_rf",
            "enterprise_profit_per_lb_rf",
            "interplant_logistics_rf",
            "interplant_logistics_pct_sales_rf",
            "interplant_logistics_per_lb_rf",
            "plant_ops_rf",
            "plant_ops_pct_sales_rf",
            "plant_ops_per_lb_rf"
        ]


        df_filled = df_filled.drop(*columns_to_drop)

        # Add load date
        df_filled = df_filled.withColumn("load_date",lit(triggered_on))
        # rows = df_filled.count()
        # cols = len(df_filled.columns)

        # print(f"Rows: {rows}, Columns: {cols}")


    
        # Write final data
        print("Writing data to PostgreSQL...")
        result = write_to_postgres(
            df=df_filled,
            table_name=f"ca_{app_env}.t_recency_frequency_bad",
            mode="overwrite",
            connection_details=connection_details,
            spark=spark
        )
        
        print("Pipeline completed successfully!")

        
    except Exception as e:
            print(f"Pipeline failed with error: {str(e)}")
            raise
        

if __name__ == "__main__":
    main()