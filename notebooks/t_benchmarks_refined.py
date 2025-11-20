"""
Standalone script version of t_benchmarks notebook
Run with: python t_benchmarks_refined.py --app_env dev --triggered_on "2025-11-17 10:00:00"
"""

import argparse
import os
import sys
import yaml
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, concat_ws

# Add project root to path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(PROJECT_ROOT)

# Import utilities
from utils.blob_operations.pyspark_blob import read_parquet_from_blob
from utils.generic_utils.add_sales_metric import add_sales_metrics
from utils.generic_utils.remove_duplicate_distribution_channel import deduplicate_distribution_channels
from utils.benchmark_utils.remove_outliers_from_data import remove_outliers_by_metric_cohort
from utils.benchmark_utils.recency_weighted_calculations import recency_weights_calculations, weight_duplication
from utils.benchmark_utils.calculate_quantiles import calculate_quantiles_multi_df_metrics, calculate_quantiles_with_cohorts_opt
from utils.benchmark_utils.extract_cohort_metrics import transform_dataframe
from utils.benchmark_utils.filter_columns import filter_dataframe_columns
from utils.benchmark_utils.find_nearest_percentile import find_nearest_percentile_benchmark
from utils.generic_utils.output_validations import Validations
from utils.postgres_operations.pyspark_postgres import write_to_postgres


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Run t_benchmark data pipeline')
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


def create_spark_session(app_name="t_benchmark"):
    """Create and configure Spark session"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.caseSensitive", "true") \
        .getOrCreate()
    
    return spark


def main():
    # Configuration
    nb_name = 't_benchmark'
    
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
        
        # Create outcome metrics
        print("Creating outcome metrics...")
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
        
        # Create cohort column
        print("Creating cohort column...")
        df = df.withColumn("distribution_ibp", 
                          concat_ws(" | ", col("Distribution Channel Text"), col("IBP Group 2 Text")))
        
        # Create Quarter Label
        df = df.withColumn(
            "Quarter_Label",
            F.concat(
                F.lit("Q"),
                F.quarter(F.col("Calendar Date")),
                F.lit("'"),
                F.substring(F.year(F.col("Calendar Date")), 3, 2)
            )
        )
        
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
            "Cost to Serve": "cost_to_serve",
            "Tier 2 Total": "tier_2",
            "Tier 3 Total": "tier_3",
            "Tier 2 and 3 Total": "tier_2_3",
            "Enterprise Profit": "enterprise_profit",
            'Interplant Logistics': "interplant_logistics",
            "Plant Ops": "plant_ops"
        }
        
        for df_col, db_col in column_mapping.items():
            df = df.withColumnRenamed(df_col, db_col)
        
        # Add sales metrics
        print("Adding sales metrics...")
        cost_column = pipeline_configs[nb_name]['cost_col_origin']
        result_df = add_sales_metrics(df, outcome_metric=cost_column)
        
        # Remove outliers
        print("Removing outliers...")
        metrics = pipeline_configs[nb_name]['cost_cols']
        clean_dfs = remove_outliers_by_metric_cohort(
            result_df, metrics, cohort_column='distribution_ibp', method='iqr', factor=1.5
        )
        
        # Filter required columns
        print("Filtering columns for analysis...")
        common_columns = pipeline_configs[nb_name]['common_cols']
        dataframes = {f"{key}_df": value for key, value in clean_dfs.items()}
        clean_dfs = filter_dataframe_columns(dataframes, common_columns)
        
        # Apply recency weighting
        print("Applying recency weighting...")
        df_final = {}
        for df_name, df_ in clean_dfs.items():
            recency_weighted_df = recency_weights_calculations(df_, quarter_col='Quarter_Label')
            weight_duplication_df = weight_duplication(recency_weighted_df)
            df_final[df_name] = weight_duplication_df
        
        # Calculate percentiles
        print("Calculating percentiles...")
        df_metric_map = {df_: key.replace('_df', '') for key, df_ in df_final.items()}
        df_names = list(df_metric_map.values())
        
        quantile_df = calculate_quantiles_multi_df_metrics(df_metric_map, df_names=df_names)
        
        quantile_cohort_df = calculate_quantiles_with_cohorts_opt(
            df_metric_map,
            cohort_column='distribution_ibp',
            df_names=df_names
        )
        
        # Find nearest percentile
        print("Finding nearest percentiles...")
        result_df = find_nearest_percentile_benchmark(
            overall_percentile_df=quantile_df,
            cohort_percentile_df=quantile_cohort_df,
            target_percentile='p10',
            metric_column='dataframe_metric'
        )
        
        # Transform dataframe
        print("Transforming results...")
        trans_df = transform_dataframe(result_df)
        
        # Select and rename final columns
        final_df = trans_df.select("cohort", "cost format", "Metric", "Final Benchmark")
        column_mapping_final = {
            "cost format": "cost_format",
            "Metric": "cost_metric",
            "Final Benchmark": "benchmark"
        }
        
        for df_col, db_col in column_mapping_final.items():
            final_df = final_df.withColumnRenamed(df_col, db_col)
        
        # Round values
        final_df = final_df.withColumn("benchmark", F.round(col('benchmark'), 4))
        
        # Add load date
        final_df = final_df.withColumn("load_date", lit(triggered_on))
        
        # Run validations
        print("Running data validations...")
        validator = Validations(final_df, validation_configs)
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
            df=final_df,
            table_name=f"ca_{app_env}.t_benchmark",
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