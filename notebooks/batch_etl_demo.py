# Databricks notebook source

# MAGIC %pip install pyyaml

# COMMAND ----------

# MAGIC %md
# MAGIC # LOTUS-PRISM Batch ETL Demo
# MAGIC 
# MAGIC This notebook demonstrates the batch ETL process for the LOTUS-PRISM retail price intelligence system.
# MAGIC 
# MAGIC ## Overview
# MAGIC - Data is processed through Bronze, Silver, and Gold layers
# MAGIC - Includes data validation, product categorization, price normalization, and RFM analysis
# MAGIC - Processes data from multiple retail competitors

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Import necessary libraries and set up the environment

# COMMAND ----------

# Import required libraries
import os
import sys
import yaml
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, expr

# Add the src directory to the path to import local modules
current_dir = os.path.dirname(os.path.abspath("__file__"))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

# COMMAND ----------

# Configure logging to suppress ANTLR warnings
import logging
logging.getLogger("org.apache.spark.sql.catalyst.parser.CatalystSqlParser").setLevel(logging.ERROR)
logging.getLogger("org.apache.spark.sql.execution.datasources.parquet").setLevel(logging.ERROR)

# Cấu hình để sử dụng DBFS thay vì Azure Storage
dbfs_root = "dbfs:/FileStore/LOTUS-PRISM"
local_config = {
    "delta": {
        "bronze_layer_path": f"{dbfs_root}/bronze",
        "silver_layer_path": f"{dbfs_root}/silver", 
        "gold_layer_path": f"{dbfs_root}/gold"
    },
    "processing": {
        "write_mode": "merge",
        "partition_columns": ["category", "date"]
    }
}

# Tạo thư mục DBFS
for path in [f"{dbfs_root}", f"{dbfs_root}/bronze", f"{dbfs_root}/silver", f"{dbfs_root}/gold"]:
    try:
        dbutils.fs.mkdirs(path)
        print(f"Tạo thành công thư mục: {path}")
    except Exception as e:
        print(f"Thư mục đã tồn tại hoặc lỗi: {path} - {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configuration
# MAGIC Load the batch processing configuration

# COMMAND ----------

# Load configuration
def load_config(config_path="dbfs:/FileStore/LOTUS-PRISM/configs/batch_config.yaml"):
    """Load configuration from YAML file"""
    try:
        # For local testing, can use dbutils.fs.get to read the file
        content = dbutils.fs.head(config_path)
        config = yaml.safe_load(content)
        return config
    except Exception as e:
        print(f"Error loading configuration: {str(e)}")
        # Return default config
        return local_config

# Get config path from parameters if available
try:
    config_path = dbutils.widgets.get("config_path")
except:
    config_path = "dbfs:/FileStore/LOTUS-PRISM/configs/batch_config.yaml"

# Load the configuration or use local config if file doesn't exist
try:
    config = load_config(config_path)
except:
    print(f"Using local configuration instead of file")
    config = local_config

print(f"Configuration loaded successfully")
print(f"Bronze layer path: {config.get('delta', {}).get('bronze_layer_path')}")
print(f"Silver layer path: {config.get('delta', {}).get('silver_layer_path')}")
print(f"Gold layer path: {config.get('delta', {}).get('gold_layer_path')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Components
# MAGIC Set up the validator, categorizer, normalizer, and RFM analyzer

# COMMAND ----------

# Setup mock imports for demonstration
# In production, these would be actual imports from the batch processing modules
class DataValidator:
    def __init__(self, spark, config_path=None):
        self.spark = spark
        
    def run_all_validations(self, df, required_columns=None, price_column="price", 
                          expected_schema=None, key_columns=None, string_columns=None):
        print(f"Running validations on DataFrame with {df.count()} rows")
        return {"overall_valid": True, "message": "All validations passed"}

class ProductCategorizer:
    def __init__(self, spark, config_path=None):
        self.spark = spark
        
    def categorize_products(self, df, product_name_column="product_name", 
                          description_column=None, category_path_column=None, source_category_column=None):
        print("Categorizing products...")
        return df.withColumn("category", lit("Uncategorized"))

class PriceNormalizer:
    def __init__(self, spark, config_path=None):
        self.spark = spark
        
    def normalize_prices(self, df, price_column="price", unit_column=None, quantity_column=None,
                       currency_column=None, product_name_column="product_name", target_unit="kg",
                       target_currency=None, vat_inclusive=True):
        print("Normalizing prices...")
        return df.withColumn("normalized_price", col(price_column))

class RFMAnalyzer:
    def __init__(self, spark, config_path=None):
        self.spark = spark
        
    def perform_rfm_analysis(self, df, customer_id_column="customer_id",
                           purchase_date_column="purchase_date", purchase_amount_column="purchase_amount",
                           output_details=True):
        print("Performing RFM analysis...")
        rfm_df = df.withColumn("rfm_score", lit(3.5)).withColumn("customer_segment", lit("Loyal Customers"))
        segment_analysis = None
        recommendations = None
        return rfm_df, segment_analysis, recommendations

# Initialize components
spark = SparkSession.builder.getOrCreate()
validator = DataValidator(spark)
categorizer = ProductCategorizer(spark)
normalizer = PriceNormalizer(spark)
rfm_analyzer = RFMAnalyzer(spark)

print("Components initialized successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Sample Data
# MAGIC Create sample datasets for demonstration purposes

# COMMAND ----------

# Generate sample product data
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
import random
from datetime import date, timedelta

# Sample product schema
product_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("unit", StringType(), True),
    StructField("category_path", StringType(), True),
    StructField("description", StringType(), True),
    StructField("source", StringType(), False),
    StructField("date", DateType(), False)
])

# Generate sample product data
def generate_sample_products(source, count=100):
    products = []
    categories = ["Beverages", "Dairy", "Bakery", "Meat", "Seafood", "Fruits", "Vegetables", "Snacks"]
    units = ["kg", "g", "l", "ml", "pack", "unit"]
    
    for i in range(count):
        category = random.choice(categories)
        sub_category = f"{category}-{random.randint(1, 3)}"
        product_id = f"{source}-{i+1:05d}"
        price = round(random.uniform(10000, 500000), 0)  # Price in VND
        unit = random.choice(units)
        
        products.append((
            product_id,
            f"{category} Product {i+1}",
            price,
            unit,
            f"{category}/{sub_category}",
            f"Description for {category} product {i+1}",
            source,
            date.today() - timedelta(days=random.randint(0, 30))
        ))
    
    return spark.createDataFrame(products, product_schema)

# Generate sample data for each source
aeon_products = generate_sample_products("aeon")
lotte_products = generate_sample_products("lotte")
winmart_products = generate_sample_products("winmart")
mm_mega_products = generate_sample_products("mm_mega")
lotus_products = generate_sample_products("lotus_internal")

print(f"Generated sample data: {aeon_products.count()} products per source")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Bronze Layer
# MAGIC Store raw data in the Bronze layer with minimal transformations

# COMMAND ----------

def process_to_bronze(df, table_name):
    """Process data to bronze layer (minimal transformations)"""
    print(f"Processing {df.count()} records to bronze layer for {table_name}")
    
    # Basic data validation
    validation_results = validator.run_all_validations(
        df,
        required_columns=["product_id", "price"]
    )
    
    if not validation_results.get("overall_valid", False):
        print(f"Data validation for bronze layer failed: {validation_results.get('message', '')}")
    
    # Add processing_time and layer columns
    bronze_df = df.withColumn("processing_time", current_timestamp()) \
                 .withColumn("layer", lit("bronze"))
    
    # Get correct bronze layer path from config
    bronze_path = config.get('delta', {}).get('bronze_layer_path')
    if bronze_path is None:
        bronze_path = "dbfs:/FileStore/LOTUS-PRISM/bronze"
        print(f"Warning: Bronze layer path not found in config, using default: {bronze_path}")
    
    # Write to bronze layer with full path
    full_bronze_path = f"{bronze_path}/{table_name}"
    
    # Log the exact write path for debugging
    print(f"Writing {bronze_df.count()} records to bronze layer at: {full_bronze_path}")
    
    # Ensure the directory exists
    try:
        dbutils.fs.mkdirs(bronze_path)
    except:
        print(f"Note: Path {bronze_path} already exists or cannot be created")
    
    # Write data to Delta format with error handling
    try:
        bronze_df.write.format("delta").mode("overwrite").save(full_bronze_path)
        print(f"Successfully wrote data to bronze layer at: {full_bronze_path}")
    except Exception as e:
        print(f"Error writing to bronze layer: {str(e)}")
        # Try with an alternate path
        alternate_path = f"dbfs:/FileStore/LOTUS-PRISM/bronze/{table_name}"
        print(f"Attempting to write to alternate path: {alternate_path}")
        try:
            bronze_df.write.format("delta").mode("overwrite").save(alternate_path)
            print(f"Successfully wrote data to alternate bronze path: {alternate_path}")
        except Exception as e2:
            print(f"Error writing to alternate bronze path: {str(e2)}")
    
    # Register as temp view for SQL queries
    bronze_df.createOrReplaceTempView(f"bronze_{table_name}")
    
    return bronze_df

# Process each source to bronze
try:
    bronze_aeon = process_to_bronze(aeon_products, "aeon_products")
    bronze_lotte = process_to_bronze(lotte_products, "lotte_products")
    bronze_winmart = process_to_bronze(winmart_products, "winmart_products")
    bronze_mm_mega = process_to_bronze(mm_mega_products, "mm_mega_products")
    bronze_lotus = process_to_bronze(lotus_products, "lotus_products")
except Exception as e:
    print(f"Error in bronze layer processing: {str(e)}")

# COMMAND ----------

# Define a helper function to safely execute operations
def try_operation(operation_name, function, *args, **kwargs):
    """Execute a function safely with error handling"""
    try:
        result = function(*args, **kwargs)
        print(f"Operation '{operation_name}' completed successfully")
        return result
    except Exception as e:
        print(f"Error in operation '{operation_name}': {str(e)}")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Silver Layer
# MAGIC Apply cleansing, normalization, and enrichment to create the Silver layer

# COMMAND ----------

def process_to_silver(bronze_df, table_name):
    """Process bronze data to silver layer (cleansing, normalization, enrichment)"""
    if bronze_df is None:
        print(f"Cannot process to silver: bronze_df is None for {table_name}")
        return None
        
    print(f"Processing {bronze_df.count()} records to silver layer for {table_name}")
    
    # Apply product categorization
    print("Categorizing products...")
    silver_df = categorizer.categorize_products(
        bronze_df,
        product_name_column="product_name",
        description_column="description" if "description" in bronze_df.columns else None,
        category_path_column="category_path" if "category_path" in bronze_df.columns else None
    )
    
    # Apply price normalization
    print("Normalizing prices...")
    silver_df = normalizer.normalize_prices(
        silver_df,
        price_column="price",
        unit_column="unit" if "unit" in silver_df.columns else None,
        product_name_column="product_name" if "product_name" in silver_df.columns else None
    )
    
    # Add processing_time and layer columns
    silver_df = silver_df.withColumn("processing_time", current_timestamp()) \
                       .withColumn("layer", lit("silver"))
    
    # Get correct silver layer path from config
    silver_path = config.get('delta', {}).get('silver_layer_path')
    if silver_path is None:
        silver_path = "dbfs:/FileStore/LOTUS-PRISM/silver"
        print(f"Warning: Silver layer path not found in config, using default: {silver_path}")
    
    # Write to silver layer with full path
    full_silver_path = f"{silver_path}/{table_name}"
    
    # Log the exact write path for debugging
    print(f"Writing {silver_df.count()} records to silver layer at: {full_silver_path}")
    
    # Ensure the directory exists
    try:
        dbutils.fs.mkdirs(silver_path)
    except:
        print(f"Note: Path {silver_path} already exists or cannot be created")
    
    # Write data to Delta format with error handling
    try:
        silver_df.write.format("delta").mode("overwrite").save(full_silver_path)
        print(f"Successfully wrote data to silver layer at: {full_silver_path}")
    except Exception as e:
        print(f"Error writing to silver layer: {str(e)}")
        # Try with an alternate path
        alternate_path = f"dbfs:/FileStore/LOTUS-PRISM/silver/{table_name}"
        print(f"Attempting to write to alternate path: {alternate_path}")
        try:
            silver_df.write.format("delta").mode("overwrite").save(alternate_path)
            print(f"Successfully wrote data to alternate silver path: {alternate_path}")
        except Exception as e2:
            print(f"Error writing to alternate silver path: {str(e2)}")
    
    # Register as temp view for SQL queries
    silver_df.createOrReplaceTempView(f"silver_{table_name}")
    
    return silver_df

# Process each source to silver
try:
    silver_aeon = process_to_silver(bronze_aeon, "aeon_products")
    silver_lotte = process_to_silver(bronze_lotte, "lotte_products")
    silver_winmart = process_to_silver(bronze_winmart, "winmart_products")
    silver_mm_mega = process_to_silver(bronze_mm_mega, "mm_mega_products")
    silver_lotus = process_to_silver(bronze_lotus, "lotus_products")
except Exception as e:
    print(f"Error in silver layer processing: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Gold Layer
# MAGIC Apply business logic and create analytics-ready data in the Gold layer

# COMMAND ----------

def process_to_gold(silver_df, table_name):
    """Process silver data to gold layer (business logic, aggregations, analytics-ready)"""
    if silver_df is None:
        print(f"Cannot process to gold: silver_df is None for {table_name}")
        return None
        
    print(f"Processing {silver_df.count()} records to gold layer for {table_name}")
    
    # Different processing based on table type
    if "product" in table_name.lower():
        # For product data, prepare for price comparison
        gold_df = silver_df.withColumn("is_processed_to_gold", lit(True))
        
        # Add metadata
        gold_df = gold_df.withColumn("processing_time", current_timestamp()) \
                        .withColumn("layer", lit("gold"))
        
    else:
        # Default processing
        gold_df = silver_df.withColumn("is_processed_to_gold", lit(True)) \
                          .withColumn("layer", lit("gold"))
    
    # Get correct gold layer path from config
    gold_path = config.get('delta', {}).get('gold_layer_path')
    if gold_path is None:
        gold_path = "dbfs:/FileStore/LOTUS-PRISM/gold"
        print(f"Warning: Gold layer path not found in config, using default: {gold_path}")
    
    # Write to gold layer with full path
    full_gold_path = f"{gold_path}/{table_name}"
    
    # Log the exact write path for debugging
    print(f"Writing {gold_df.count()} records to gold layer at: {full_gold_path}")
    
    # Ensure the directory exists
    try:
        dbutils.fs.mkdirs(gold_path)
    except:
        print(f"Note: Path {gold_path} already exists or cannot be created")
    
    # Write data to Delta format with error handling
    try:
        gold_df.write.format("delta").mode("overwrite").save(full_gold_path)
        print(f"Successfully wrote data to gold layer at: {full_gold_path}")
    except Exception as e:
        print(f"Error writing to gold layer: {str(e)}")
        # Try with an alternate path
        alternate_path = f"dbfs:/FileStore/LOTUS-PRISM/gold/{table_name}"
        print(f"Attempting to write to alternate path: {alternate_path}")
        try:
            gold_df.write.format("delta").mode("overwrite").save(alternate_path)
            print(f"Successfully wrote data to alternate gold path: {alternate_path}")
        except Exception as e2:
            print(f"Error writing to alternate gold path: {str(e2)}")
    
    # Register as temp view for SQL queries
    gold_df.createOrReplaceTempView(f"gold_{table_name}")
    
    return gold_df

# Process each source to gold
try:
    gold_aeon = process_to_gold(silver_aeon, "aeon_products")
    gold_lotte = process_to_gold(silver_lotte, "lotte_products")
    gold_winmart = process_to_gold(silver_winmart, "winmart_products")
    gold_mm_mega = process_to_gold(silver_mm_mega, "mm_mega_products")
    gold_lotus = process_to_gold(silver_lotus, "lotus_products")
except Exception as e:
    print(f"Error in gold layer processing: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Price Comparison Report
# MAGIC Create a comparison of prices across different retailers

# COMMAND ----------

def generate_price_comparison():
    """Generate price comparison analysis across all competitors"""
    print("Generating price comparison analysis")
    
    # Make sure we have the data to compare
    if any(df is None for df in [gold_lotus, gold_aeon, gold_lotte, gold_winmart]):
        print("Cannot generate price comparison: some gold tables are missing")
        return None
    
    # Register temp views if needed
    gold_lotus.createOrReplaceTempView("lotus_products")
    gold_aeon.createOrReplaceTempView("aeon_products")
    gold_lotte.createOrReplaceTempView("lotte_products")
    gold_winmart.createOrReplaceTempView("winmart_products")
    
    # Create a price comparison query
    comparison_df = spark.sql("""
        WITH product_counts AS (
            SELECT 
                COUNT(*) as lotus_count,
                (SELECT COUNT(*) FROM aeon_products) as aeon_count,
                (SELECT COUNT(*) FROM lotte_products) as lotte_count,
                (SELECT COUNT(*) FROM winmart_products) as winmart_count
            FROM lotus_products
        ),
        
        price_stats AS (
            SELECT
                'lotus' as retailer,
                AVG(price) as avg_price,
                MIN(price) as min_price,
                MAX(price) as max_price
            FROM lotus_products
            UNION ALL
            SELECT
                'aeon' as retailer,
                AVG(price) as avg_price,
                MIN(price) as min_price,
                MAX(price) as max_price
            FROM aeon_products
            UNION ALL
            SELECT
                'lotte' as retailer,
                AVG(price) as avg_price,
                MIN(price) as min_price,
                MAX(price) as max_price
            FROM lotte_products
            UNION ALL
            SELECT
                'winmart' as retailer,
                AVG(price) as avg_price,
                MIN(price) as min_price,
                MAX(price) as max_price
            FROM winmart_products
        )
        
        SELECT 
            'Price Comparison Report' as report_name,
            current_timestamp() as generation_time,
            product_counts.*,
            retailer,
            avg_price,
            min_price,
            max_price
        FROM price_stats
        CROSS JOIN product_counts
    """)
    
    # Show the comparison
    print("Price Comparison Results:")
    comparison_df.show()
    
    # Get correct gold layer path from config
    gold_path = config.get('delta', {}).get('gold_layer_path')
    if gold_path is None:
        gold_path = "dbfs:/FileStore/LOTUS-PRISM/gold"
        print(f"Warning: Gold layer path not found in config, using default: {gold_path}")
    
    # Path for price comparison
    comparison_path = f"{gold_path}/price_comparison"
    
    # Log the exact write path for debugging
    print(f"Writing price comparison to gold layer at: {comparison_path}")
    
    # Ensure the directory exists
    try:
        dbutils.fs.mkdirs(gold_path)
    except:
        print(f"Note: Gold path already exists or cannot be created")
    
    # Write data to Delta format with error handling
    try:
        comparison_df.write.format("delta").mode("overwrite").save(comparison_path)
        print(f"Successfully wrote price comparison to gold layer at: {comparison_path}")
    except Exception as e:
        print(f"Error writing price comparison to gold layer: {str(e)}")
        # Try with an alternate path
        alternate_path = "dbfs:/FileStore/LOTUS-PRISM/gold/price_comparison"
        print(f"Attempting to write to alternate path: {alternate_path}")
        try:
            comparison_df.write.format("delta").mode("overwrite").save(alternate_path)
            print(f"Successfully wrote data to alternate path: {alternate_path}")
        except Exception as e2:
            print(f"Error writing to alternate path: {str(e2)}")
    
    return comparison_df

# Generate price comparison
try:
    price_comparison_df = generate_price_comparison()
except Exception as e:
    print(f"Error in price comparison generation: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## RFM Analysis Demo
# MAGIC Demonstrate RFM analysis on customer transaction data

# COMMAND ----------

# Generate sample transaction data
transaction_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("purchase_date", DateType(), False),
    StructField("purchase_amount", DoubleType(), False),
    StructField("store_id", StringType(), True)
])

def generate_sample_transactions(count=1000):
    transactions = []
    today = date.today()
    
    for i in range(count):
        customer_id = f"CUST-{random.randint(1, 100):04d}"
        days_ago = random.randint(1, 365)
        purchase_date = today - timedelta(days=days_ago)
        purchase_amount = round(random.uniform(50000, 2000000), 0)  # Amount in VND
        store_id = f"STORE-{random.randint(1, 10):02d}"
        
        transactions.append((
            f"TXN-{i+1:06d}",
            customer_id,
            purchase_date,
            purchase_amount,
            store_id
        ))
    
    return spark.createDataFrame(transactions, transaction_schema)

# Generate sample transaction data
transactions_df = generate_sample_transactions()
print(f"Generated {transactions_df.count()} sample transactions")

# Perform RFM analysis
print("Performing RFM Analysis...")
rfm_df, segment_analysis, recommendations = rfm_analyzer.perform_rfm_analysis(
    transactions_df,
    customer_id_column="customer_id",
    purchase_date_column="purchase_date",
    purchase_amount_column="purchase_amount"
)

# Show RFM results
print("RFM Analysis Results:")
rfm_df.createOrReplaceTempView("rfm_results")
rfm_sample = spark.sql("""
    SELECT customer_id, rfm_score, customer_segment, 
           COUNT(*) OVER (PARTITION BY customer_segment) as segment_size
    FROM rfm_results
    ORDER BY rfm_score DESC
""")
rfm_sample.show(10)

# If segment analysis is available, show it
if segment_analysis is not None:
    print("Segment Analysis:")
    segment_analysis.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save RFM Results to Gold Layer
# MAGIC Save RFM analysis results to the gold layer

# COMMAND ----------

# Save RFM results to Gold layer
try:
    # Get gold layer path
    gold_path = config.get('delta', {}).get('gold_layer_path')
    if gold_path is None:
        gold_path = "dbfs:/FileStore/LOTUS-PRISM/gold"
        print(f"Warning: Gold layer path not found in config, using default: {gold_path}")
    
    # Path for RFM
    rfm_path = f"{gold_path}/rfm_analysis"
    
    print(f"Writing RFM analysis to gold layer at: {rfm_path}")
    
    # Ensure the directory exists
    try:
        dbutils.fs.mkdirs(gold_path)
    except:
        print(f"Note: Gold path already exists or cannot be created")
    
    # Add timestamp
    rfm_output = rfm_df.withColumn("processing_time", current_timestamp()) \
                      .withColumn("layer", lit("gold"))
    
    # Write RFM data to Gold layer
    rfm_output.write.format("delta").mode("overwrite").save(rfm_path)
    print(f"Successfully wrote RFM analysis to gold layer at: {rfm_path}")
except Exception as e:
    print(f"Error writing RFM analysis to gold layer: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC Review the ETL process and results

# COMMAND ----------

# Get counts from each layer safely
def safe_count(df):
    if df is None:
        return 0
    try:
        return df.count()
    except:
        return 0

bronze_count = safe_count(bronze_aeon) + safe_count(bronze_lotte) + safe_count(bronze_winmart) + safe_count(bronze_mm_mega) + safe_count(bronze_lotus)
silver_count = safe_count(silver_aeon) + safe_count(silver_lotte) + safe_count(silver_winmart) + safe_count(silver_mm_mega) + safe_count(silver_lotus)
gold_count = safe_count(gold_aeon) + safe_count(gold_lotte) + safe_count(gold_winmart) + safe_count(gold_mm_mega) + safe_count(gold_lotus)

print("ETL Process Summary:")
print(f"Total records processed in Bronze layer: {bronze_count}")
print(f"Total records processed in Silver layer: {silver_count}")
print(f"Total records processed in Gold layer: {gold_count}")
print(f"Total transactions analyzed with RFM: {transactions_df.count()}")

# Calculate processing time
print(f"ETL job completed successfully at {datetime.now()}")
print(f"Data is available at:")
print(f"Bronze: {config.get('delta', {}).get('bronze_layer_path')}")
print(f"Silver: {config.get('delta', {}).get('silver_layer_path')}")
print(f"Gold: {config.get('delta', {}).get('gold_layer_path')}")
print("LOTUS-PRISM Batch ETL Job")
