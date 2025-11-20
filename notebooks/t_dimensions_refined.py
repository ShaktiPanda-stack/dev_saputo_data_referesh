"""
Standalone script version of t_dimensions notebook
Run with: python t_dimensions.py --app_env dev --triggered_on "2025-11-17 10:00:00"
"""

import argparse
import os
import sys
import yaml
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit

# Add project root to path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(PROJECT_ROOT)

# Import utilities
from utils.blob_operations.pyspark_blob import read_parquet_from_blob
from utils.generic_utils.prepare_dimensions_data import prepare_dimension_data_generic
from utils.generic_utils.remove_duplicate_distribution_channel import deduplicate_distribution_channels
from utils.generic_utils.output_validations import Validations
from utils.postgres_operations.pyspark_postgres import write_to_postgres


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Run t_dimensions data pipeline')
    parser.add_argument('--app_env', type=str, default='dev', 
                       help='Application environment (dev/prod)')
    parser.add_argument('--triggered_on', type=str, 
                       default=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                       help='Trigger timestamp')
    return parser.parse_args()


def load_configs(pipeline_parent_directory, nb_name):
    """Load all configuration files"""
    pipeline_config_path = os.path.join(pipeline_parent_directory, 
                                       "configs/pipeline_configs.yaml")
    filepath_config_path = os.path.join(pipeline_parent_directory, 
                                       "configs/filepath_configs.yaml")
    validation_config_path = os.path.join(pipeline_parent_directory, 
                                         f"configs/data_validations/{nb_name}.yaml")
    
    with open(pipeline_config_path, "r") as f:
        pipeline_configs = yaml.safe_load(f)
    
    with open(filepath_config_path, "r") as f:
        filepath_configs = yaml.safe_load(f)
    
    with open(validation_config_path, "r") as f:
        validation_configs = yaml.safe_load(f)
    
    return pipeline_configs, filepath_configs, validation_configs


def create_spark_session(app_name="t_dimensions"):
    """Create and configure Spark session"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.caseSensitive", "true") \
        .getOrCreate()
    
    return spark


def main():
    # Configuration
    nb_name = 't_dimensions'
    
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
        udm_file_path = filepath_configs['udm']
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
        df = df.filter(df["IBP Group 2 Text"] != "Production/WIP")
        
        # Show date range
        date_stats = df.agg(
            F.min('Invoice Date').alias("min_date"),
            F.max('Invoice Date').alias("max_date")
        )
        print("Date range:")
        date_stats.show()
        
        # Deduplicate distribution channels
        print("Deduplicating distribution channels...")
        df = deduplicate_distribution_channels(df)
        
        # Prepare dimension data
        print("Preparing dimension data...")
        dimension_cols = pipeline_configs[nb_name]['derived_dimension_columns']
        dimension_data_mapped = prepare_dimension_data_generic(df, dimension_cols)
        
        # Rename columns
        print("Renaming columns...")
        column_mapping = {
            "L3 Customer Name": "l3_customer",
            "Product Distance Zone": "product_distance",
            "Primary Production Plant Name": "production_plant",
            "Source Plant Name": "fulfillment_warehouse",
            "IBP Group 2 Text": "product_type",
            "Kosher Indicator": "kosher_indicator",
            "Organic Indicator": "organic_indicator",
            "Non-GMO Indicator": "gmo_indicator",
            "Lactose Free Indicator": "lactose_indicator",
            "Product Brand Type Text": "brand",
            "Asset Type": "packaging",
            "Distribution Channel Text": 'distribution_channel'
        }
        
        for df_col, db_col in column_mapping.items():
            dimension_data_mapped = dimension_data_mapped.withColumnRenamed(df_col, db_col)
        
        # Add load date
        dimension_data_mapped = dimension_data_mapped.withColumn("load_date", lit(triggered_on))
        
        # Run validations
        print("Running data validations...")
        validator = Validations(dimension_data_mapped, validation_configs)
        validation_report_df = validator.run()
        validation_report_df = validation_report_df.withColumn("load_date", lit(triggered_on))
        validation_report_df = validation_report_df.withColumn(
            "notebook", 
            lit(" ".join([each.title() for each in nb_name.split("_")]))
        )
        validation_report_df = validation_report_df.select([
            "notebook", "validation_type", "column", "rule", "message", "status", "load_date"
        ])
        
        # Connection details
        connection_details = {
            "jdbc_url": os.environ["postgres_url"],
            "user": os.environ["postgres_user"],
            "password": os.environ["postgres_password"]
        }
        
        # Write validation report
        print("Writing validation report...")
        pg_table_name = f"{pipeline_configs[app_env]['pg_schema_name']}.data_validation_report"
        write_to_postgres(
            validation_report_df,
            pg_table_name,
            mode='append',
            connection_details=connection_details
        )
        
        # Check DQM score
        calculated_dqm_score = validator.calculate_dqm_score()
        print(f"DQM Score: {calculated_dqm_score:.1f}%")
        
        if calculated_dqm_score < validation_configs.get("validation_score_required", 75):
            raise Exception(
                f"Pipeline Stopped - DQM Score {calculated_dqm_score:.1f}% is below "
                f"threshold {validation_configs.get('validation_score_required', 75)}%"
            )
        
        print("Data validation passed! Writing data to PostgreSQL...")
        
        # Write final data
        result = write_to_postgres(
            df=dimension_data_mapped,
            table_name=f"ca_{app_env}.t_dimensions",
            mode="overwrite",
            connection_details=connection_details
        )
        
        print("Pipeline completed successfully!")
        
    except Exception as e:
        print(f"Pipeline failed with error: {str(e)}")
        raise
    
    finally:
        # Stop Spark session
        spark.stop()


if __name__ == "__main__":
    main()