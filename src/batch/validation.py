"""
Data validation module for batch processing pipeline.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, isnan, isnull, when, length
import yaml
import os
from typing import Dict, List, Tuple, Optional, Union
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataValidator:
    """
    Data validation for the LOTUS-PRISM batch processing pipeline.
    Checks for data quality issues and validates data against expectations.
    """
    
    def __init__(self, spark: SparkSession, config_path: str = "../config/batch_config.yaml"):
        """
        Initialize the DataValidator with Spark session and configuration.
        
        Args:
            spark: Spark session
            config_path: Path to the batch configuration file
        """
        self.spark = spark
        self.config = self._load_config(config_path)
        self.validation_config = self.config.get("validation", {})
        
    def _load_config(self, config_path: str) -> Dict:
        """
        Load configuration from YAML file.
        
        Args:
            config_path: Path to the configuration file
            
        Returns:
            Dict containing configuration
        """
        try:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
            return config
        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            # Return default config if file can't be loaded
            return {
                "validation": {
                    "min_records_threshold": 1000,
                    "max_null_percentage": 0.05,
                    "price_min_threshold": 1000,
                    "price_max_threshold": 100000000,
                    "enable_schema_validation": True,
                    "enable_data_quality_checks": True,
                    "notify_on_failure": True
                }
            }
    
    def validate_row_count(self, df: DataFrame) -> Tuple[bool, str]:
        """
        Validates if the dataframe has a minimum number of records.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, message)
        """
        min_threshold = self.validation_config.get("min_records_threshold", 1000)
        row_count = df.count()
        
        if row_count < min_threshold:
            message = f"Row count validation failed. Expected at least {min_threshold} records, got {row_count}."
            logger.warning(message)
            return False, message
        
        return True, f"Row count validation passed: {row_count} records."
    
    def validate_null_values(self, df: DataFrame, required_columns: List[str] = None) -> Tuple[bool, Dict[str, float]]:
        """
        Validates the percentage of null values in each column.
        
        Args:
            df: DataFrame to validate
            required_columns: List of columns that should not contain nulls
            
        Returns:
            Tuple of (is_valid, null_percentage_by_column)
        """
        max_null_pct = self.validation_config.get("max_null_percentage", 0.05)
        if required_columns is None:
            required_columns = df.columns
        
        # Count total rows
        total_rows = df.count()
        if total_rows == 0:
            return False, {"error": "DataFrame is empty"}
        
        # Calculate null percentages for each column
        null_counts = {}
        for column in df.columns:
            null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
            null_counts[column] = null_count / total_rows
        
        # Check if required columns have acceptable null percentages
        all_valid = True
        for column in required_columns:
            if column in null_counts and null_counts[column] > max_null_pct:
                all_valid = False
                logger.warning(f"Column '{column}' has {null_counts[column]:.2%} null values, exceeding threshold of {max_null_pct:.2%}")
        
        return all_valid, null_counts
    
    def validate_price_range(self, df: DataFrame, price_column: str = "price") -> Tuple[bool, str]:
        """
        Validates if prices are within acceptable ranges.
        
        Args:
            df: DataFrame to validate
            price_column: Name of the price column
            
        Returns:
            Tuple of (is_valid, message)
        """
        if price_column not in df.columns:
            return False, f"Price column '{price_column}' not found in DataFrame."
        
        min_price = self.validation_config.get("price_min_threshold", 1000)
        max_price = self.validation_config.get("price_max_threshold", 100000000)
        
        # Count records with prices outside acceptable range
        out_of_range_count = df.filter(
            (col(price_column) < min_price) | 
            (col(price_column) > max_price) | 
            isnan(col(price_column)) | 
            isnull(col(price_column))
        ).count()
        
        total_count = df.count()
        if total_count == 0:
            return False, "DataFrame is empty, cannot validate price range."
        
        out_of_range_pct = out_of_range_count / total_count
        
        if out_of_range_pct > self.validation_config.get("max_null_percentage", 0.05):
            message = f"Price range validation failed. {out_of_range_pct:.2%} of prices are outside the valid range."
            logger.warning(message)
            return False, message
        
        return True, "Price range validation passed."
    
    def validate_schema(self, df: DataFrame, expected_schema: Dict[str, str]) -> Tuple[bool, str]:
        """
        Validates if DataFrame has the expected schema.
        
        Args:
            df: DataFrame to validate
            expected_schema: Dictionary mapping column names to data types
            
        Returns:
            Tuple of (is_valid, message)
        """
        if not self.validation_config.get("enable_schema_validation", True):
            return True, "Schema validation skipped as per configuration."
        
        actual_schema = {field.name: field.dataType.simpleString() for field in df.schema.fields}
        
        # Check for missing columns
        missing_columns = [col for col in expected_schema.keys() if col not in actual_schema]
        if missing_columns:
            return False, f"Missing columns in schema: {', '.join(missing_columns)}"
        
        # Check for type mismatches
        type_mismatches = []
        for col_name, expected_type in expected_schema.items():
            if col_name in actual_schema and expected_type != actual_schema[col_name]:
                type_mismatches.append(f"{col_name} (expected: {expected_type}, actual: {actual_schema[col_name]})")
        
        if type_mismatches:
            return False, f"Schema type mismatches: {', '.join(type_mismatches)}"
        
        return True, "Schema validation passed."
    
    def validate_uniqueness(self, df: DataFrame, key_columns: List[str]) -> Tuple[bool, str]:
        """
        Validates if the specified columns form a unique key.
        
        Args:
            df: DataFrame to validate
            key_columns: List of columns that should form a unique key
            
        Returns:
            Tuple of (is_valid, message)
        """
        # Check if all key columns exist
        missing_columns = [col for col in key_columns if col not in df.columns]
        if missing_columns:
            return False, f"Key columns not in DataFrame: {', '.join(missing_columns)}"
        
        # Count distinct values and total rows
        distinct_count = df.select(key_columns).distinct().count()
        total_count = df.count()
        
        if distinct_count < total_count:
            duplicate_pct = 1 - (distinct_count / total_count)
            message = f"Uniqueness validation failed. {duplicate_pct:.2%} of rows have duplicate keys."
            logger.warning(message)
            return False, message
        
        return True, "Uniqueness validation passed."
    
    def validate_string_lengths(self, df: DataFrame, string_columns: Dict[str, int]) -> Tuple[bool, Dict[str, int]]:
        """
        Validates if string columns have acceptable lengths.
        
        Args:
            df: DataFrame to validate
            string_columns: Dictionary mapping column names to maximum allowed lengths
            
        Returns:
            Tuple of (is_valid, violations_by_column)
        """
        violations = {}
        
        for column, max_length in string_columns.items():
            if column not in df.columns:
                violations[column] = f"Column not found"
                continue
                
            violations_count = df.filter(length(col(column)) > max_length).count()
            
            if violations_count > 0:
                violations[column] = violations_count
        
        is_valid = len(violations) == 0
        
        return is_valid, violations
    
    def run_all_validations(
        self, 
        df: DataFrame, 
        required_columns: List[str] = None,
        price_column: str = "price",
        expected_schema: Dict[str, str] = None,
        key_columns: List[str] = None,
        string_columns: Dict[str, int] = None
    ) -> Dict[str, Union[bool, str, Dict]]:
        """
        Run all validation checks on the DataFrame.
        
        Args:
            df: DataFrame to validate
            required_columns: List of columns that should not contain nulls
            price_column: Name of the price column
            expected_schema: Dictionary mapping column names to data types
            key_columns: List of columns that should form a unique key
            string_columns: Dictionary mapping column names to maximum allowed lengths
            
        Returns:
            Dictionary with validation results
        """
        results = {}
        overall_valid = True
        
        # Validate row count
        valid, message = self.validate_row_count(df)
        results["row_count"] = {"valid": valid, "message": message}
        overall_valid = overall_valid and valid
        
        # Validate null values
        if required_columns:
            valid, null_pcts = self.validate_null_values(df, required_columns)
            results["null_values"] = {"valid": valid, "null_percentages": null_pcts}
            overall_valid = overall_valid and valid
        
        # Validate price range
        if price_column in df.columns:
            valid, message = self.validate_price_range(df, price_column)
            results["price_range"] = {"valid": valid, "message": message}
            overall_valid = overall_valid and valid
        
        # Validate schema
        if expected_schema:
            valid, message = self.validate_schema(df, expected_schema)
            results["schema"] = {"valid": valid, "message": message}
            overall_valid = overall_valid and valid
        
        # Validate uniqueness
        if key_columns:
            valid, message = self.validate_uniqueness(df, key_columns)
            results["uniqueness"] = {"valid": valid, "message": message}
            overall_valid = overall_valid and valid
        
        # Validate string lengths
        if string_columns:
            valid, violations = self.validate_string_lengths(df, string_columns)
            results["string_lengths"] = {"valid": valid, "violations": violations}
            overall_valid = overall_valid and valid
        
        results["overall_valid"] = overall_valid
        
        # Log validation results
        if overall_valid:
            logger.info("All data validations passed.")
        else:
            logger.warning("One or more data validations failed.")
            if self.validation_config.get("notify_on_failure", True):
                self._send_notification(results)
        
        return results
    
    def _send_notification(self, validation_results: Dict) -> None:
        """
        Send notification about validation failures.
        
        Args:
            validation_results: Results of validation checks
        """
        # This would be implemented to send email/Slack/other notifications
        # For now, just log the validation failures
        logger.error("Data validation failed with the following results:")
        for check_name, result in validation_results.items():
            if check_name != "overall_valid" and isinstance(result, dict) and not result.get("valid", True):
                logger.error(f"- {check_name}: {result.get('message', str(result))}")


# Example usage (for documentation)
if __name__ == "__main__":
    # This would be used in a Spark job
    spark = SparkSession.builder.appName("DataValidation").getOrCreate()
    
    # Create validator
    validator = DataValidator(spark)
    
    # Assuming df is a DataFrame loaded from a data source
    # results = validator.run_all_validations(
    #     df, 
    #     required_columns=["product_id", "name", "price"], 
    #     key_columns=["product_id"],
    #     expected_schema={"product_id": "string", "name": "string", "price": "double"}
    # )
