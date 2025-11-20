from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, expr, max as ps_max, min as ps_min
from pyspark.sql.types import DecimalType, DoubleType, FloatType, IntegerType, NumericType, StringType

class Validations:
    def __init__(self, df: DataFrame, configs: dict, reference_data: dict = None, spark=None):
        """
        Initializes the validation class.

        Parameters:
            df (DataFrame): The Spark DataFrame to validate.
            configs (dict): A dictionary containing validation rules.
            reference_data (dict, optional): Dictionary of reference DataFrames for foreign key validation.
            spark (SparkSession, optional): Spark session to use. If None, will get or create one.
        """
        self.df = df
        self.configs = configs
        # Get Spark session from the DataFrame or create/get one
        self.spark = spark if spark is not None else df.sparkSession
        self.reference_data = reference_data or {}  # Default empty if no reference tables provided
        self.validation_results = []

    def validate_column_existence(self):
        """Validates if expected columns exist in the DataFrame."""

        expected_columns = self.configs.get('column_existence', [])
        # Check for empty or invalid primary keys configuration
        if expected_columns in [None, "None", []]:
            self.validation_results.append((
                "N/A", "column_existence", 0, 
                "No Column Existence validation configuration provided.", "Skipped"
            ))
            return
        for column in expected_columns:
            if column not in self.df.columns:
                self.validation_results.append((
                    column, "column_existence", "", f"Expected {column}, but not found.", "Failed"
                ))
            else:
                self.validation_results.append((
                    column, "column_existence", "", f"Expected {column} column found.", "Passed"
                ))

    def validate_primary_key(self):
        """Checks for duplicate primary keys with edge case handling."""
        
        # Fetch primary keys from configs
        primary_keys = self.configs.get("primary_key_validation", [])
        
        # Check for empty or invalid primary keys configuration
        if primary_keys in [None, "None", []]:
            self.validation_results.append((
                "N/A", "primary_key_validation", 0, 
                "No primary key validation configuration provided.", "Skipped"
            ))
            return
        
        # Check if all primary key columns exist in the DataFrame
        missing_columns = [key for key in primary_keys if key not in self.df.columns]
        if missing_columns:
            self.validation_results.append((
                ", ".join(missing_columns), "primary_key_validation", 0, 
                f"Missing columns: {', '.join(missing_columns)}.", "Failed"
            ))
            return
        
        # Check if the DataFrame is empty
        if self.df.count() == 0:
            self.validation_results.append((
                ", ".join(primary_keys), "primary_key_validation", 0, 
                "DataFrame is empty. No records to validate.", "Skipped"
            ))
            return
        
        # Check for duplicates
        duplicate_count = self.df.groupBy(primary_keys).count().filter(col("count") > 1).count()
        status = "Failed" if duplicate_count > 0 else "Passed"
        
        # Append the result to validation results
        self.validation_results.append((
            ", ".join(primary_keys), "primary_key_validation", duplicate_count, 
            f"Duplicate records found: {duplicate_count}.", status
        ))

    def validate_null_threshold(self):
        """Validates if null values exceed the allowed threshold with edge case handling."""
        
        # Fetch null threshold validation configurations
        null_thresholds = self.configs.get("null_threshold_validation", {})
        
        # Check if the configuration is empty or invalid
        if null_thresholds in [None, "None", {}, [], ""]:
            self.validation_results.append((
                "N/A", "null_threshold_validation", 0, 
                "No null threshold validation configuration provided.", "Skipped"
            ))
            return
        
        # Check if the DataFrame is empty
        if self.df.count() == 0:
            self.validation_results.append((
                "N/A", "null_threshold_validation", 0, 
                "DataFrame is empty. No records to validate.", "Skipped"
            ))
            return
        
        # Loop through each column and threshold in the configuration
        for column, threshold in null_thresholds.items():
            
            # Check if the column exists in the DataFrame
            if column not in self.df.columns:
                self.validation_results.append((
                    column, "null_threshold_validation", 0, 
                    f"Column '{column}' does not exist in the DataFrame.", "Skipped"
                ))
                continue
            
            # Ensure threshold is a valid number
            if not isinstance(threshold, (int, float)):
                self.validation_results.append((
                    column, "null_threshold_validation", 0, 
                    f"Invalid threshold value: {threshold}. Expected a numeric value.", "Skipped"
                ))
                continue
            
            # Calculate the number of null values in the column
            null_count = self.df.filter(col(column).isNull()).count()
            total_count = self.df.select(col(column)).count()

            null_threshold = (null_count / total_count)*100
            
            # Check if the null count exceeds the threshold
            status = "Failed" if null_threshold > threshold else "Passed"
            
            # Append the validation result
            self.validation_results.append((
                column, "null_threshold_validation", null_threshold, 
                f"Allowed null limit: {threshold}, Found: {null_threshold}.", status
            ))

    def validate_datatype(self):
        """Validates if column datatypes match expected types with edge case handling."""
        
        # Fetch the datatype validation configurations
        datatype_validations = self.configs.get("datatype_validation", {})
        
        # Check if the configuration is empty or invalid
        if datatype_validations in [None, "None", {}, [], ""]:
            self.validation_results.append((
                "N/A", "datatype_validation", 0, 
                "No datatype validation configuration provided.", "Skipped"
            ))
            return
        
        # Check if the DataFrame is empty
        if self.df.count() == 0:
            self.validation_results.append((
                "N/A", "datatype_validation", 0, 
                "DataFrame is empty. No records to validate.", "Skipped"
            ))
            return

        # Loop through each column and expected datatype in the configuration
        for column, expected_dtype in datatype_validations.items():
            
            # Check if the column exists in the DataFrame
            if column not in self.df.columns:
                self.validation_results.append((
                    column, "datatype_validation", 0, 
                    f"Column '{column}' does not exist in the DataFrame.", "Skipped"
                ))
                continue
            
            # Check for invalid or missing expected datatype
            if expected_dtype in [None, "None"]:
                self.validation_results.append((
                    column, "datatype_validation", 0, 
                    "Invalid or missing expected datatype.", "Failed"
                ))
                continue
            
            # Get the inferred datatype from the DataFrame schema
            inferred_dtype = self.df.schema[column].dataType
            
            # Convert inferred dtype to a comparable string format
            if isinstance(inferred_dtype, DecimalType):
                inferred_dtype_str = "DECIMAL"
            elif isinstance(inferred_dtype, StringType):
                inferred_dtype_str = "STRING"
            elif isinstance(inferred_dtype, IntegerType):
                inferred_dtype_str = "INTEGER"
            elif isinstance(inferred_dtype, DoubleType):
                inferred_dtype_str = "DOUBLE"
            elif isinstance(inferred_dtype, FloatType):
                inferred_dtype_str = "FLOAT"
            else:
                inferred_dtype_str = inferred_dtype.simpleString().upper()
            
            # Compare the inferred datatype with the expected datatype
            status = "Passed" if inferred_dtype_str == expected_dtype else "Failed"
            
            # Append the validation result
            self.validation_results.append((
                column, "datatype_validation", inferred_dtype_str, 
                f"Expected: {expected_dtype}, Found: {inferred_dtype_str}.", status
            ))

    def validate_min_range(self):
        """Validates minimum value constraints for numeric columns with edge case handling."""
        
        # Fetch the minimum range validation configurations
        min_range_validations = self.configs.get("min_range_validation", {})
        
        # Check if the configuration is empty or invalid
        if min_range_validations in [None, "None", {}, [], ""]:
            self.validation_results.append((
                "N/A", "min_range_validation", 0, 
                "No minimum range validation configuration provided.", "Skipped"
            ))
            return
        
        # Check if the DataFrame is empty
        if self.df.count() == 0:
            self.validation_results.append((
                "N/A", "min_range_validation", 0, 
                "DataFrame is empty. No records to validate.", "Skipped"
            ))
            return

        # Loop through each column and minimum value in the configuration
        for column, min_value in min_range_validations.items():
            
            # Check if the column exists in the DataFrame
            if column not in self.df.columns:
                self.validation_results.append((
                    column, "min_range_validation", 0, 
                    f"Column '{column}' does not exist in the DataFrame.", "Skipped"
                ))
                continue
            
            # Check for invalid or missing minimum value
            if min_value in [None, "None"] or not isinstance(min_value, (int, float)):
                self.validation_results.append((
                    column, "min_range_validation", 0, 
                    f"Invalid or missing minimum value: {min_value}. Expected a numeric value.", "Skipped"
                ))
                continue
            
            # Check if the column is numeric (only apply min range to numeric columns)
            column_type = self.df.schema[column].dataType
            if not isinstance(column_type, NumericType):
                self.validation_results.append((
                    column, "min_range_validation", 0, 
                    f"Column '{column}' is not numeric. Skipping min range validation.", "Skipped"
                ))
                continue
            
            # Calculate the number of records below the minimum value
            min_violation = self.df.filter(col(column) < min_value).count()
            
            # Check if there are any violations (records below min value)
            status = "Failed" if min_violation > 0 else "Passed"
            
            # Append the validation result
            self.validation_results.append((
                column, "min_range_validation", min_violation, 
                f"{min_violation} records below min value {min_value}.", status
            ))

    def validate_max_range(self):
        """Validates maximum value constraints for numeric columns with edge case handling."""
        
        # Fetch the maximum range validation configurations
        max_range_validations = self.configs.get("max_range_validation", {})
        
        # Check if the configuration is empty or invalid
        if max_range_validations in [None, "None", {}, [], ""]:
            self.validation_results.append((
                "N/A", "max_range_validation", 0, 
                "No maximum range validation configuration provided.", "Skipped"
            ))
            return
        
        # Check if the DataFrame is empty
        if self.df.count() == 0:
            self.validation_results.append((
                "N/A", "max_range_validation", 0, 
                "DataFrame is empty. No records to validate.", "Skipped"
            ))
            return

        # Loop through each column and maximum value in the configuration
        for column, max_value in max_range_validations.items():
            
            # Check if the column exists in the DataFrame
            if column not in self.df.columns:
                self.validation_results.append((
                    column, "max_range_validation", 0, 
                    f"Column '{column}' does not exist in the DataFrame.", "Skipped"
                ))
                continue
            
            # Check for invalid or missing minimum value
            if max_value in [None, "None"] or not isinstance(max_value, (int, float)):
                self.validation_results.append((
                    column, "max_range_validation", 0, 
                    f"Invalid or missing minimum value: {max_value}. Expected a numeric value.", "Skipped"
                ))
                continue
            
            # Check if the column is numeric (only apply min range to numeric columns)
            column_type = self.df.schema[column].dataType
            if not isinstance(column_type, NumericType):
                self.validation_results.append((
                    column, "max_range_validation", 0, 
                    f"Column '{column}' is not numeric. Skipping min range validation.", "Skipped"
                ))
                continue
            
            # Check if the column is numeric (only apply max range to numeric columns)
            column_type = self.df.schema[column].dataType
            if not isinstance(column_type, NumericType):
                self.validation_results.append((
                    column, "max_range_validation", 0, 
                    f"Column '{column}' is not numeric. Skipping max range validation.", "Skipped"
                ))
                continue
            
            # Calculate the number of records above the maximum value
            max_violation = self.df.filter(col(column) > max_value).count()
            
            # Check if there are any violations (records above max value)
            status = "Failed" if max_violation > 0 else "Passed"
            
            # Append the validation result
            self.validation_results.append((
                column, "max_range_validation", max_violation, 
                f"{max_violation} records above max value {max_value}.", status
            ))

    def validate_flag_column(self):
        """Validates if column values are within allowed values, with edge case handling."""
        
        # Fetch the flag column validation configurations
        flag_column_validations = self.configs.get("flag_column_validation", {})
        
        # Check if the configuration is empty or invalid
        if flag_column_validations in [None, "None", {}, [], ""]:
            self.validation_results.append((
                "N/A", "flag_column_validation", 0, 
                "No flag column validation configuration provided.", "Skipped"
            ))
            return
        
        # Check if the DataFrame is empty
        if self.df.count() == 0:
            self.validation_results.append((
                "N/A", "flag_column_validation", 0, 
                "DataFrame is empty. No records to validate.", "Skipped"
            ))
            return
        
        # Loop through each column and allowed values in the configuration
        for column, allowed_values in flag_column_validations.items():
            
            # Check if the column exists in the DataFrame
            if column not in self.df.columns:
                self.validation_results.append((
                    column, "flag_column_validation", 0, 
                    f"Column '{column}' does not exist in the DataFrame.", "Skipped"
                ))
                continue
            
            # Check for invalid or missing allowed values
            if allowed_values in [None, "None", []]:
                self.validation_results.append((
                    column, "flag_column_validation", 0, 
                    f"Invalid or empty allowed values: {allowed_values}.", "Skipped"
                ))
                continue
            
            # Check if the column is of a suitable type (e.g., StringType or IntegerType)
            column_type = self.df.schema[column].dataType
            if not isinstance(column_type, (StringType, IntegerType)):
                self.validation_results.append((
                    column, "flag_column_validation", 0, 
                    f"Column '{column}' is not a categorical type (String or Integer). Skipping validation.", "Skipped"
                ))
                continue
            
            # Calculate the number of invalid values (values not in allowed values)
            invalid_count = self.df.filter(~col(column).isin(allowed_values)).count()
            
            # Check if there are any invalid values
            status = "Failed" if invalid_count > 0 else "Passed"
            
            # Append the validation result
            self.validation_results.append((
                column, "flag_column_validation", invalid_count, 
                f"Invalid values found: {invalid_count}, Allowed: {allowed_values}.", status
            ))

    def validate_format(self):
        """Validates if column values match regex patterns, with edge case handling."""
        
        # Fetch the format validation configurations
        format_validations = self.configs.get("format_validation", {})
        
        # Check if the configuration is empty or invalid
        if format_validations in [None, "None", {}, [], ""]:
            self.validation_results.append((
                "N/A", "format_validation", 0, 
                "No format validation configuration provided.", "Skipped"
            ))
            return
        
        # Check if the DataFrame is empty
        if self.df.count() == 0:
            self.validation_results.append((
                "N/A", "format_validation", 0, 
                "DataFrame is empty. No records to validate.", "Skipped"
            ))
            return
        
        # Loop through each column and regex pattern in the configuration
        for column, pattern in format_validations.items():
            
            # Check if the column exists in the DataFrame
            if column not in self.df.columns:
                self.validation_results.append((
                    column, "format_validation", 0, 
                    f"Column '{column}' does not exist in the DataFrame.", "Skipped"
                ))
                continue
            
            # Check if the regex pattern is valid (not None or empty)
            if pattern in [None, "None", ""]:
                self.validation_results.append((
                    column, "format_validation", 0, 
                    f"Invalid or empty regex pattern: '{pattern}'.", "Skipped"
                ))
                continue
            
            # Remove the "regex(" and ")" if present, for proper regex formatting
            try:
                regex_pattern = pattern.replace("regex(", "").replace(")", "")
            except Exception as e:
                self.validation_results.append((
                    column, "format_validation", 0, 
                    f"Error processing regex pattern: '{pattern}'. Error: {str(e)}", "Failed"
                ))
                continue
            
            # Check if the column is of a suitable type (e.g., StringType)
            column_type = self.df.schema[column].dataType
            if not isinstance(column_type, StringType):
                self.validation_results.append((
                    column, "format_validation", 0, 
                    f"Column '{column}' is not of String type. Skipping format validation.", "Skipped"
                ))
                continue
            
            # Calculate the number of invalid records based on the regex pattern
            invalid_count = self.df.filter(~col(column).rlike(regex_pattern)).count()
            
            # Determine the validation status
            status = "Failed" if invalid_count > 0 else "Passed"
            
            # Append the validation result
            self.validation_results.append((
                column, "format_validation", invalid_count, 
                f"Invalid format records: {invalid_count}.", status
            ))
    
    def validate_business_rules(self):
        """Validates business rules defined in the configuration, with error handling for edge cases."""
        
        # Fetch the business rule validation configurations
        business_rules = self.configs.get("business_rule_validation", [])
        
        # Check if the business rule configuration is empty or invalid
        if business_rules in ["None", None, []]:
            self.validation_results.append((
                "N/A", "business_rule_validation", 0, 
                "No business rule validation configuration provided.", "Skipped"
            ))
            return
        
        # Check if the DataFrame is empty
        if self.df.count() == 0:
            self.validation_results.append((
                "N/A", "business_rule_validation", 0, 
                "DataFrame is empty. No records to validate.", "Skipped"
            ))
            return
        
        # Loop through each business rule in the configuration
        for rule in business_rules:
            try:
                # Replace '=' with '==' for comparison in expressions
                condition = rule.replace("=", "==")
                
                # Check if the rule contains invalid syntax after replacement
                if not condition or "==" not in condition:
                    raise ValueError(f"Invalid business rule condition: '{rule}'.")
                
                # Apply the condition and count failed records
                failed_count = self.df.filter(~expr(condition)).count()
                status = "Failed" if failed_count > 0 else "Passed"
                
                # Append the result to the validation results
                self.validation_results.append((
                    "BusinessRule", "business_rule_validation", failed_count, 
                    f"Records violating rule: {failed_count}. Rule: {rule}.", status
                ))
            
            except Exception as e:
                # Handle any error in processing the business rule
                self.validation_results.append((
                    "BusinessRule", "business_rule_validation", 0,
                    f"Error processing rule '{rule}': {str(e)}", "Failed"
                ))

    def validate_data_freshness(self):
        """Checks if data is older than 7 days, and handles edge cases."""
        
        # Check if configuration for freshness validation exists
        freshness_config = self.configs.get("data_freshness_validation", {})
        
        if freshness_config in [None, "None", {}, [], ""]:
            self.validation_results.append((
                "N/A", "data_freshness_validation", 0, 
                "No data freshness validation configuration provided.", "Skipped"
            ))
            return

        # Check if the DataFrame is empty
        if self.df.count() == 0:
            self.validation_results.append((
                "N/A", "data_freshness_validation", 0, 
                "DataFrame is empty. No records to validate.", "Skipped"
            ))
            return
        
        # Loop through each column and freshness configuration
        for column, start_end_dates in freshness_config.items():
            try:
                if start_end_dates in [None, "None"]:
                    continue  # Skip if the start_end_dates are invalid

                # Check if the column exists in the DataFrame
                if column not in self.df.columns:
                    self.validation_results.append((
                        column, "data_freshness_validation", 0, 
                        f"Column '{column}' does not exist in DataFrame.", "Skipped"
                    ))
                    continue

                # Ensure that the column contains date values (handle nulls)
                temp_df = self.df.filter(col(column).isNotNull())

                # Calculate the earliest and latest dates from the column
                earliest_date_detected = temp_df.agg(ps_min(col(column))).collect()[0][0]
                latest_date_detected = temp_df.agg(ps_max(col(column))).collect()[0][0]

                if not earliest_date_detected or not latest_date_detected:
                    self.validation_results.append((
                        column, "data_freshness_validation", 0, 
                        f"Invalid or missing date values in column '{column}'.", "Skipped"
                    ))
                    continue

                # Handle start date check
                if "start_date" in start_end_dates:
                    stale_count = self.df.filter(col(column) < earliest_date_detected).count()
                    status = "Failed" if stale_count > 0 else "Passed"
                    self.validation_results.append((
                        column, "data_freshness_validation", stale_count, 
                        f"Old records: {stale_count}, Data Starts from: {earliest_date_detected}.", status
                    ))

                # Handle end date check
                elif "end_date" in start_end_dates:
                    if isinstance(start_end_dates["end_date"], datetime):
                        stale_count = self.df.filter(col(column) >= start_end_dates["end_date"]).count()
                        status = "Failed" if stale_count > 0 else "Passed"
                        self.validation_results.append((
                            column, "data_freshness_validation", stale_count, 
                            f"New records: {stale_count}, Data Found till: {latest_date_detected}.", status
                        ))
                    else:
                        self.validation_results.append((
                            column, "data_freshness_validation", 0, 
                            f"Invalid end_date value for column '{column}'.", "Failed"
                        ))

            except Exception as e:
                self.validation_results.append((
                    column, "data_freshness_validation", 0,
                    f"Error processing data freshness for column '{column}': {str(e)}", "Failed"
                ))

    def validate_foreign_keys(self):
        """Checks if foreign key values exist in reference tables."""
        for column, ref_table in self.configs.get("foreign_key_validation", {}).items():
            if column in self.reference_data:
                ref_df = self.reference_data[column]
                missing_fk_count = self.df.join(ref_df, column, "left_anti").count()
                status = "Failed" if missing_fk_count > 0 else "Passed"
                self.validation_results.append((
                    column, "foreign_key_validation", missing_fk_count, 
                    f"Missing FK records: {missing_fk_count} in {ref_table}.", status
                ))

    def run(self):
        """Runs all validation checks and returns results as a DataFrame."""
        self.validate_column_existence()
        self.validate_null_threshold()
        self.validate_datatype()
        self.validate_min_range()
        self.validate_max_range()
        self.validate_flag_column()
        self.validate_primary_key()
        # self.validate_foreign_keys()
        self.validate_format()
        self.validate_business_rules()
        self.validate_data_freshness()

        return self.spark.createDataFrame(self.validation_results, ["column", "validation_type", "rule", "message", "status"])
    
    def calculate_dqm_score(self):
        """
        Calculates the Data Quality Management (DQM) score.

        Returns:
            float: DQM score as a percentage.
        """
        if not self.validation_results:
            return 0.0  # No validations performed, return 0%

        # Convert the validation results into a DataFrame
        result_df = self.spark.createDataFrame(self.validation_results, ["column", "validation_type", "rule", "message", "status"])

        result_df = result_df.filter(col("status") != "Skipped")

        total_checks = result_df.count()
        passed_checks = result_df.filter(col("status") == "Passed").count()

        if total_checks == 0:
            return 0.0  # Avoid division by zero
        
        dqm_score = (passed_checks / total_checks) * 100
        return round(dqm_score, 2)