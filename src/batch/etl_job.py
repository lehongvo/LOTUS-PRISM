"""
Main ETL job for LOTUS-PRISM batch processing pipeline.

This module implements the primary ETL job that processes data from bronze
to silver and gold layers, applying data transformations, validations, and
business logic.
"""
import os
import sys
import yaml
import logging
from datetime import datetime
from typing import Dict, List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit

# Add the src directory to the path to import local modules
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from batch.validation import DataValidator
from batch.product_categorization import ProductCategorizer
from batch.price_normalization import PriceNormalizer
from batch.rfm_analysis import RFMAnalyzer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BatchETLJob:
    """
    Main ETL job for batch processing in LOTUS-PRISM.
    """
    
    def __init__(self, config_path: str = "../config/batch_config.yaml"):
        """
        Initialize the BatchETLJob with configuration.
        
        Args:
            config_path: Path to the batch configuration file
        """
        self.config_path = config_path
        self.config = self._load_config()
        
        # Initialize Spark session
        self.spark = SparkSession.builder \
            .appName("LOTUS-PRISM-BatchETL") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        # Set log level from config
        self.spark.sparkContext.setLogLevel(self.config.get("spark", {}).get("log_level", "WARN"))
        
        # Initialize components
        self.validator = DataValidator(self.spark, self.config_path)
        self.categorizer = ProductCategorizer(self.spark, self.config_path)
        self.normalizer = PriceNormalizer(self.spark, self.config_path)
        self.rfm_analyzer = RFMAnalyzer(self.spark, self.config_path)
        
        # Record job start time
        self.job_start_time = datetime.now()
        logger.info(f"BatchETLJob initialized at {self.job_start_time}")
        
    def _load_config(self) -> Dict:
        """
        Load configuration from YAML file.
        
        Returns:
            Dict containing configuration
        """
        try:
            with open(self.config_path, "r") as f:
                config = yaml.safe_load(f)
            return config
        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            # Return default config if file can't be loaded
            return {
                "delta": {
                    "bronze_layer_path": "abfss://data@lotusprismstorage.dfs.core.windows.net/bronze",
                    "silver_layer_path": "abfss://data@lotusprismstorage.dfs.core.windows.net/silver",
                    "gold_layer_path": "abfss://data@lotusprismstorage.dfs.core.windows.net/gold"
                },
                "processing": {
                    "write_mode": "merge",
                    "partition_columns": ["category", "date"]
                },
                "spark": {
                    "log_level": "WARN"
                }
            }
    
    def load_raw_data(self, source_name: str) -> DataFrame:
        """
        Load raw data from a specified source.
        
        Args:
            source_name: Name of the data source (aeon, lotte, etc.)
            
        Returns:
            DataFrame with raw data
        """
        source_path = self.config.get("data_sources", {}).get(f"{source_name}_data")
        if not source_path:
            error_msg = f"Data source path for '{source_name}' not found in configuration."
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f"Loading raw data from {source_name} at {source_path}")
        
        try:
            raw_df = self.spark.read.format("delta").load(source_path)
            logger.info(f"Loaded {raw_df.count()} records from {source_name}")
            
            # Add source and processing_time columns
            raw_df = raw_df.withColumn("source", lit(source_name)) \
                          .withColumn("processing_time", current_timestamp())
            
            return raw_df
        except Exception as e:
            logger.error(f"Error loading data from {source_name}: {str(e)}")
            raise
    
    def process_to_bronze(self, df: DataFrame, table_name: str) -> DataFrame:
        """
        Process raw data to bronze layer (minimal transformations).
        
        Args:
            df: Raw DataFrame
            table_name: Name of the bronze table
            
        Returns:
            DataFrame with bronze layer data
        """
        logger.info(f"Processing data to bronze layer for {table_name}")
        
        # Basic data validation before writing to bronze
        validation_results = self.validator.run_all_validations(
            df,
            required_columns=["product_id", "price"]
        )
        
        if not validation_results.get("overall_valid", False):
            logger.warning(f"Data validation for bronze layer failed. See logs for details.")
        
        # Write to bronze layer
        bronze_path = f"{self.config.get('delta', {}).get('bronze_layer_path')}/{table_name}"
        write_mode = self.config.get("processing", {}).get("write_mode", "merge")
        partition_columns = self.config.get("processing", {}).get("partition_columns", [])
        
        logger.info(f"Writing data to bronze layer at {bronze_path}")
        
        try:
            df.write.format("delta") \
                .mode("overwrite" if write_mode == "overwrite" else "append") \
                .partitionBy(*[col for col in partition_columns if col in df.columns]) \
                .save(bronze_path)
            
            logger.info(f"Successfully wrote {df.count()} records to bronze layer.")
            
            # Read back to ensure consistent schema
            bronze_df = self.spark.read.format("delta").load(bronze_path)
            return bronze_df
        except Exception as e:
            logger.error(f"Error writing to bronze layer: {str(e)}")
            raise
    
    def process_to_silver(self, bronze_df: DataFrame, table_name: str) -> DataFrame:
        """
        Process bronze data to silver layer (cleansing, normalization, enrichment).
        
        Args:
            bronze_df: Bronze layer DataFrame
            table_name: Name of the silver table
            
        Returns:
            DataFrame with silver layer data
        """
        logger.info(f"Processing data to silver layer for {table_name}")
        
        # Apply product categorization if it's a product table
        if "product" in table_name.lower():
            silver_df = self.categorizer.categorize_products(
                bronze_df,
                product_name_column="product_name",
                description_column="description" if "description" in bronze_df.columns else None,
                category_path_column="category_path" if "category_path" in bronze_df.columns else None
            )
            logger.info(f"Applied product categorization to {silver_df.count()} records")
        else:
            silver_df = bronze_df
        
        # Apply price normalization if price column exists
        if "price" in silver_df.columns:
            silver_df = self.normalizer.normalize_prices(
                silver_df,
                price_column="price",
                unit_column="unit" if "unit" in silver_df.columns else None,
                quantity_column="quantity" if "quantity" in silver_df.columns else None,
                product_name_column="product_name" if "product_name" in silver_df.columns else None
            )
            logger.info(f"Applied price normalization")
        
        # Add processing_time and layer columns
        silver_df = silver_df.withColumn("processing_time", current_timestamp()) \
                           .withColumn("layer", lit("silver"))
        
        # Write to silver layer
        silver_path = f"{self.config.get('delta', {}).get('silver_layer_path')}/{table_name}"
        write_mode = self.config.get("processing", {}).get("write_mode", "merge")
        partition_columns = self.config.get("processing", {}).get("partition_columns", [])
        
        logger.info(f"Writing data to silver layer at {silver_path}")
        
        try:
            silver_df.write.format("delta") \
                .mode("overwrite" if write_mode == "overwrite" else "append") \
                .partitionBy(*[col for col in partition_columns if col in silver_df.columns]) \
                .save(silver_path)
            
            logger.info(f"Successfully wrote {silver_df.count()} records to silver layer.")
            
            # Read back to ensure consistent schema
            silver_df = self.spark.read.format("delta").load(silver_path)
            return silver_df
        except Exception as e:
            logger.error(f"Error writing to silver layer: {str(e)}")
            raise
    
    def process_to_gold(self, silver_df: DataFrame, table_name: str) -> DataFrame:
        """
        Process silver data to gold layer (business logic, aggregations, analytics-ready).
        
        Args:
            silver_df: Silver layer DataFrame
            table_name: Name of the gold table
            
        Returns:
            DataFrame with gold layer data
        """
        logger.info(f"Processing data to gold layer for {table_name}")
        
        # Apply different transformations based on table type
        if "transaction" in table_name.lower():
            # For transaction data, apply RFM analysis
            gold_df, segment_analysis, _ = self.rfm_analyzer.perform_rfm_analysis(
                silver_df,
                customer_id_column="customer_id",
                purchase_date_column="purchase_date" if "purchase_date" in silver_df.columns else "date",
                purchase_amount_column="purchase_amount" if "purchase_amount" in silver_df.columns else "amount"
            )
            
            # Also write segment analysis to gold layer
            if segment_analysis is not None:
                segment_path = f"{self.config.get('delta', {}).get('gold_layer_path')}/customer_segments"
                segment_analysis.write.format("delta").mode("overwrite").save(segment_path)
                logger.info(f"Wrote customer segment analysis to {segment_path}")
            
        elif "product" in table_name.lower():
            # For product data, add price comparison and analytics
            competitors = ["aeon", "lotte", "winmart", "mm_mega"]
            
            # Example: Calculate price index compared to competitors
            # In a real implementation, this would involve joining with competitor data
            # and performing more complex calculations
            gold_df = silver_df.withColumn("is_processed_to_gold", lit(True))
            
        elif "competitor" in table_name.lower():
            # For competitor data, add comparative analysis
            gold_df = silver_df.withColumn("is_processed_to_gold", lit(True))
            
        else:
            # Default processing
            gold_df = silver_df
        
        # Add processing_time and layer columns
        gold_df = gold_df.withColumn("processing_time", current_timestamp()) \
                        .withColumn("layer", lit("gold"))
        
        # Write to gold layer
        gold_path = f"{self.config.get('delta', {}).get('gold_layer_path')}/{table_name}"
        write_mode = self.config.get("processing", {}).get("write_mode", "merge")
        partition_columns = self.config.get("processing", {}).get("partition_columns", [])
        
        logger.info(f"Writing data to gold layer at {gold_path}")
        
        try:
            gold_df.write.format("delta") \
                .mode("overwrite" if write_mode == "overwrite" else "append") \
                .partitionBy(*[col for col in partition_columns if col in gold_df.columns]) \
                .save(gold_path)
            
            logger.info(f"Successfully wrote {gold_df.count()} records to gold layer.")
            
            # Read back to ensure consistent schema
            gold_df = self.spark.read.format("delta").load(gold_path)
            return gold_df
        except Exception as e:
            logger.error(f"Error writing to gold layer: {str(e)}")
            raise
    
    def process_competitor_data(self, source_name: str) -> None:
        """
        Process competitor data through the entire pipeline.
        
        Args:
            source_name: Name of the competitor data source
        """
        logger.info(f"Processing full pipeline for competitor data: {source_name}")
        
        try:
            # Load raw data
            raw_df = self.load_raw_data(source_name)
            
            # Process to bronze
            bronze_df = self.process_to_bronze(raw_df, f"{source_name}_products")
            
            # Process to silver
            silver_df = self.process_to_silver(bronze_df, f"{source_name}_products")
            
            # Process to gold
            gold_df = self.process_to_gold(silver_df, f"{source_name}_products")
            
            logger.info(f"Successfully processed {source_name} data through entire pipeline.")
        except Exception as e:
            logger.error(f"Error processing {source_name} data: {str(e)}")
            raise
    
    def process_internal_data(self) -> None:
        """
        Process Lotus's internal data through the entire pipeline.
        """
        logger.info("Processing full pipeline for Lotus internal data")
        
        try:
            # Load raw data
            raw_df = self.load_raw_data("lotus_internal")
            
            # Process to bronze
            bronze_df = self.process_to_bronze(raw_df, "lotus_products")
            
            # Process to silver
            silver_df = self.process_to_silver(bronze_df, "lotus_products")
            
            # Process to gold
            gold_df = self.process_to_gold(silver_df, "lotus_products")
            
            logger.info("Successfully processed Lotus internal data through entire pipeline.")
        except Exception as e:
            logger.error(f"Error processing Lotus internal data: {str(e)}")
            raise
    
    def generate_price_comparison(self) -> None:
        """
        Generate price comparison analysis across all competitors.
        """
        logger.info("Generating price comparison analysis")
        
        try:
            # Define paths to gold layer product data
            gold_path = self.config.get("delta", {}).get("gold_layer_path")
            lotus_path = f"{gold_path}/lotus_products"
            aeon_path = f"{gold_path}/aeon_products"
            lotte_path = f"{gold_path}/lotte_products"
            winmart_path = f"{gold_path}/winmart_products"
            
            # Load gold data
            lotus_df = self.spark.read.format("delta").load(lotus_path)
            aeon_df = self.spark.read.format("delta").load(aeon_path)
            lotte_df = self.spark.read.format("delta").load(lotte_path)
            winmart_df = self.spark.read.format("delta").load(winmart_path)
            
            # In a real implementation, we would match products across retailers
            # and perform detailed price comparison analysis
            # For this example, we'll just create a simple report
            
            # Create comparison output path
            comparison_path = f"{gold_path}/price_comparison"
            
            # Example implementation would be more sophisticated
            # This is just a placeholder for demonstration
            lotus_df.createOrReplaceTempView("lotus_products")
            aeon_df.createOrReplaceTempView("aeon_products")
            lotte_df.createOrReplaceTempView("lotte_products")
            winmart_df.createOrReplaceTempView("winmart_products")
            
            comparison_df = self.spark.sql("""
                SELECT 
                    'Price Comparison Report' as report_name,
                    current_timestamp() as generation_time,
                    (SELECT COUNT(*) FROM lotus_products) as lotus_product_count,
                    (SELECT COUNT(*) FROM aeon_products) as aeon_product_count,
                    (SELECT COUNT(*) FROM lotte_products) as lotte_product_count,
                    (SELECT COUNT(*) FROM winmart_products) as winmart_product_count
            """)
            
            # Write comparison report
            comparison_df.write.format("delta").mode("overwrite").save(comparison_path)
            
            logger.info(f"Successfully generated price comparison analysis at {comparison_path}")
        except Exception as e:
            logger.error(f"Error generating price comparison: {str(e)}")
            raise
    
    def run_full_pipeline(self) -> None:
        """
        Run the complete ETL pipeline for all data sources.
        """
        logger.info("Starting full ETL pipeline for all data sources")
        
        try:
            # Process competitor data
            self.process_competitor_data("aeon")
            self.process_competitor_data("lotte")
            self.process_competitor_data("winmart")
            self.process_competitor_data("mm_mega")
            
            # Process internal data
            self.process_internal_data()
            
            # Generate price comparison
            self.generate_price_comparison()
            
            # Calculate job duration
            job_end_time = datetime.now()
            duration = (job_end_time - self.job_start_time).total_seconds() / 60
            
            logger.info(f"Full ETL pipeline completed successfully in {duration:.2f} minutes")
        except Exception as e:
            logger.error(f"Error in ETL pipeline: {str(e)}")
            raise
        finally:
            # Optimize Delta tables if needed
            optimize_interval = self.config.get("processing", {}).get("optimize_interval", 10)
            # In a production environment, we would check when the last optimization was done
            # and only optimize if enough runs have passed
            logger.info("ETL job completed.")
            
def main():
    """
    Main entry point for the batch ETL job.
    """
    try:
        # Get config path from arguments or use default
        config_path = sys.argv[1] if len(sys.argv) > 1 else "../config/batch_config.yaml"
        
        # Initialize and run the ETL job
        etl_job = BatchETLJob(config_path)
        etl_job.run_full_pipeline()
        
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
