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
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, expr

# Add the src directory to the path to import local modules
current_dir = os.path.dirname(os.path.abspath("__file__"))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configuration
# MAGIC Load the batch processing configuration

# COMMAND ----------

# Load configuration
def load_config(config_path="dbfs:/FileStore/configs/batch_config.yaml"):
    """Load configuration from YAML file"""
    try:
        # For local testing, can use dbutils.fs.get to read the file
        content = dbutils.fs.head(config_path)
        config = yaml.safe_load(content)
        return config
    except Exception as e:
        print(f"Error loading configuration: {str(e)}")
        # Return default config
        return {
            "delta": {
                "bronze_layer_path": "dbfs:/mnt/data/bronze",
                "silver_layer_path": "dbfs:/mnt/data/silver",
                "gold_layer_path": "dbfs:/mnt/data/gold"
            },
            "processing": {
                "write_mode": "merge",
                "partition_columns": ["category", "date"]
            }
        }

# Load the configuration
config = load_config()
print("Configuration loaded successfully")

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
    
    # Write to bronze layer
    bronze_path = f"{config.get('delta', {}).get('bronze_layer_path')}/{table_name}"
    
    # For demonstration, just show the data
    print(f"Would write {bronze_df.count()} records to {bronze_path}")
    bronze_df.createOrReplaceTempView(f"bronze_{table_name}")
    
    return bronze_df

# Process each source to bronze
bronze_aeon = process_to_bronze(aeon_products, "aeon_products")
bronze_lotte = process_to_bronze(lotte_products, "lotte_products")
bronze_winmart = process_to_bronze(winmart_products, "winmart_products")
bronze_mm_mega = process_to_bronze(mm_mega_products, "mm_mega_products")
bronze_lotus = process_to_bronze(lotus_products, "lotus_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Silver Layer
# MAGIC Apply cleansing, normalization, and enrichment to create the Silver layer

# COMMAND ----------

def process_to_silver(bronze_df, table_name):
    """Process bronze data to silver layer (cleansing, normalization, enrichment)"""
    print(f"Processing {bronze_df.count()} records to silver layer for {table_name}")
    
    # Apply product categorization
    silver_df = categorizer.categorize_products(
        bronze_df,
        product_name_column="product_name",
        description_column="description" if "description" in bronze_df.columns else None,
        category_path_column="category_path" if "category_path" in bronze_df.columns else None
    )
    
    # Apply price normalization
    silver_df = normalizer.normalize_prices(
        silver_df,
        price_column="price",
        unit_column="unit" if "unit" in silver_df.columns else None,
        product_name_column="product_name" if "product_name" in silver_df.columns else None
    )
    
    # Add processing_time and layer columns
    silver_df = silver_df.withColumn("processing_time", current_timestamp()) \
                       .withColumn("layer", lit("silver"))
    
    # Write to silver layer
    silver_path = f"{config.get('delta', {}).get('silver_layer_path')}/{table_name}"
    
    # For demonstration, just show the data
    print(f"Would write {silver_df.count()} records to {silver_path}")
    silver_df.createOrReplaceTempView(f"silver_{table_name}")
    
    return silver_df

# Process each source to silver
silver_aeon = process_to_silver(bronze_aeon, "aeon_products")
silver_lotte = process_to_silver(bronze_lotte, "lotte_products")
silver_winmart = process_to_silver(bronze_winmart, "winmart_products")
silver_mm_mega = process_to_silver(bronze_mm_mega, "mm_mega_products")
silver_lotus = process_to_silver(bronze_lotus, "lotus_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Gold Layer
# MAGIC Apply business logic and create analytics-ready data in the Gold layer

# COMMAND ----------

def process_to_gold(silver_df, table_name):
    """Process silver data to gold layer (business logic, aggregations, analytics-ready)"""
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
    
    # Write to gold layer
    gold_path = f"{config.get('delta', {}).get('gold_layer_path')}/{table_name}"
    
    # For demonstration, just show the data
    print(f"Would write {gold_df.count()} records to {gold_path}")
    gold_df.createOrReplaceTempView(f"gold_{table_name}")
    
    return gold_df

# Process each source to gold
gold_aeon = process_to_gold(silver_aeon, "aeon_products")
gold_lotte = process_to_gold(silver_lotte, "lotte_products")
gold_winmart = process_to_gold(silver_winmart, "winmart_products")
gold_mm_mega = process_to_gold(silver_mm_mega, "mm_mega_products")
gold_lotus = process_to_gold(silver_lotus, "lotus_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Price Comparison Report
# MAGIC Create a comparison of prices across different retailers

# COMMAND ----------

def generate_price_comparison():
    """Generate price comparison analysis across all competitors"""
    print("Generating price comparison analysis")
    
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
    
    # Write to gold layer
    comparison_path = f"{config.get('delta', {}).get('gold_layer_path')}/price_comparison"
    print(f"Would write price comparison to {comparison_path}")
    
    return comparison_df

# Generate price comparison
price_comparison_df = generate_price_comparison()

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
# MAGIC ## Summary
# MAGIC Review the ETL process and results

# COMMAND ----------

# Get counts from each layer
bronze_count = bronze_aeon.count() + bronze_lotte.count() + bronze_winmart.count() + bronze_mm_mega.count() + bronze_lotus.count()
silver_count = silver_aeon.count() + silver_lotte.count() + silver_winmart.count() + silver_mm_mega.count() + silver_lotus.count()
gold_count = gold_aeon.count() + gold_lotte.count() + gold_winmart.count() + gold_mm_mega.count() + gold_lotus.count()

print("ETL Process Summary:")
print(f"Total records processed in Bronze layer: {bronze_count}")
print(f"Total records processed in Silver layer: {silver_count}")
print(f"Total records processed in Gold layer: {gold_count}")
print(f"Total transactions analyzed with RFM: {transactions_df.count()}")

# Calculate processing time
print(f"ETL job completed successfully at {datetime.now()}")
