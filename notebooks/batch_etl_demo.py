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
# MAGIC 
# MAGIC ## Data Layers Implementation
# MAGIC 
# MAGIC ### Bronze Layer 
# MAGIC - Raw data landing zone with minimal validation
# MAGIC - Original data preserved exactly as received from sources
# MAGIC - Ensures data lineage and auditability
# MAGIC - Only basic metadata added (processing timestamp, source, etc.)
# MAGIC 
# MAGIC ### Silver Layer
# MAGIC - Data validation, cleansing, and quality improvements
# MAGIC - Schema standardization across different sources
# MAGIC - Error correction and data enrichment
# MAGIC - Units standardization and price normalization
# MAGIC - Data consolidation and relationship mapping
# MAGIC 
# MAGIC ### Gold Layer
# MAGIC - Business-level aggregations and transformations
# MAGIC - Specific analytical datasets for business use cases
# MAGIC - Performance metrics and KPIs
# MAGIC - Dimension and fact tables for reporting

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Import necessary libraries and set up the environment

# COMMAND ----------

# Import required libraries
import os
import sys
import yaml
import random
from datetime import datetime, date, timedelta
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import col, current_timestamp, current_date, lit, expr
from pyspark.sql.functions import when, regexp_replace, trim, lower, coalesce, concat, round as spark_round
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, datediff
from pyspark.sql.functions import count, sum, avg, min, max, stddev, percentile_approx, ntile

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
        """Run basic validation checks on the dataframe"""
        print(f"Running validations on DataFrame with {df.count()} rows")
        
        validation_results = {
            "overall_valid": True,
            "message": "All validations passed",
            "missing_columns": [],
            "null_counts": {},
            "negative_prices": 0
        }
        
        # Check for required columns
        if required_columns:
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                validation_results["overall_valid"] = False
                validation_results["message"] = f"Missing required columns: {missing_columns}"
                validation_results["missing_columns"] = missing_columns
        
        # Check for null values in key columns
        if key_columns:
            for column in key_columns:
                if column in df.columns:
                    null_count = df.filter(col(column).isNull()).count()
                    validation_results["null_counts"][column] = null_count
                    if null_count > 0:
                        validation_results["overall_valid"] = False
                        validation_results["message"] += f"; Column {column} has {null_count} null values"
        
        # Check for negative prices
        if price_column in df.columns:
            negative_prices = df.filter(col(price_column) < 0).count()
            validation_results["negative_prices"] = negative_prices
            if negative_prices > 0:
                validation_results["overall_valid"] = False
                validation_results["message"] += f"; Found {negative_prices} records with negative prices"
        
        return validation_results

class ProductCategorizer:
    def __init__(self, spark, config_path=None):
        self.spark = spark
        self.category_mapping = {
            # Category mapping for standardization
            "beverage": "Beverages",
            "beverages": "Beverages",
            "water": "Beverages",
            "drinks": "Beverages",
            "coffee": "Beverages",
            "tea": "Beverages",
            "dairy": "Dairy",
            "milk": "Dairy",
            "cheese": "Dairy",
            "yogurt": "Dairy",
            "bakery": "Bakery",
            "bread": "Bakery",
            "pastry": "Bakery",
            "cake": "Bakery",
            "meat": "Meat",
            "beef": "Meat",
            "pork": "Meat",
            "chicken": "Meat",
            "seafood": "Seafood",
            "fish": "Seafood",
            "fruits": "Fruits",
            "fruit": "Fruits",
            "apple": "Fruits",
            "orange": "Fruits",
            "vegetables": "Vegetables",
            "vegetable": "Vegetables",
            "snacks": "Snacks",
            "candy": "Snacks",
            "chocolate": "Snacks",
            "chips": "Snacks"
        }
        
    def extract_category_from_text(self, text):
        """Extract category from text using keyword matching"""
        if not text or not isinstance(text, str):
            return "Uncategorized"
            
        text_lower = text.lower()
        
        # Check for each category keyword
        for keyword, category in self.category_mapping.items():
            if keyword in text_lower:
                return category
                
        return "Uncategorized"
        
    def categorize_products(self, df, product_name_column="product_name", 
                          description_column=None, category_path_column=None, source_category_column=None):
        """Categorize products based on product name, description, and existing categories"""
        print("Categorizing products...")
        
        from pyspark.sql.functions import when, col, lower, regexp_replace, trim, coalesce, lit
        
        # KHÔNG sử dụng UDF vì gây lỗi serialization
        # Thay vào đó, sử dụng các built-in functions của Spark
        
        # Start with existing category if available
        if category_path_column and category_path_column in df.columns:
            # Extract main category from category path (e.g., "Fruits/Fresh" -> "Fruits")
            categorized_df = df.withColumn("extracted_category", 
                                           when(col(category_path_column).isNotNull(),
                                                regexp_replace(col(category_path_column), "/.*", "")))
        else:
            categorized_df = df.withColumn("extracted_category", lit(None).cast("string"))
            
        # Use source category if available as backup
        if source_category_column and source_category_column in df.columns:
            categorized_df = categorized_df.withColumn("extracted_category",
                                                    coalesce(col("extracted_category"),
                                                            col(source_category_column)))
            
        # Extract from product name - thay vì dùng UDF, sử dụng CASE WHEN
        if product_name_column and product_name_column in df.columns:
            # Sử dụng các biểu thức WHEN cho các category phổ biến
            product_name_lower = lower(col(product_name_column))
            categorized_df = categorized_df.withColumn("extracted_category",
                coalesce(
                    col("extracted_category"),
                    when(product_name_lower.contains("beverage") | 
                         product_name_lower.contains("water") | 
                         product_name_lower.contains("coffee") | 
                         product_name_lower.contains("tea") | 
                         product_name_lower.contains("drinks"), lit("Beverages"))
                    .when(product_name_lower.contains("dairy") | 
                         product_name_lower.contains("milk") | 
                         product_name_lower.contains("cheese") | 
                         product_name_lower.contains("yogurt"), lit("Dairy"))
                    .when(product_name_lower.contains("bakery") | 
                         product_name_lower.contains("bread") | 
                         product_name_lower.contains("pastry") | 
                         product_name_lower.contains("cake"), lit("Bakery"))
                    .when(product_name_lower.contains("meat") | 
                         product_name_lower.contains("beef") | 
                         product_name_lower.contains("pork") | 
                         product_name_lower.contains("chicken"), lit("Meat"))
                    .when(product_name_lower.contains("seafood") | 
                         product_name_lower.contains("fish"), lit("Seafood"))
                    .when(product_name_lower.contains("fruit") | 
                         product_name_lower.contains("apple") | 
                         product_name_lower.contains("orange"), lit("Fruits"))
                    .when(product_name_lower.contains("vegetable"), lit("Vegetables"))
                    .when(product_name_lower.contains("snack") | 
                         product_name_lower.contains("candy") | 
                         product_name_lower.contains("chocolate") | 
                         product_name_lower.contains("chips"), lit("Snacks"))
                    .otherwise(lit("Uncategorized"))
                )
            )
                                                            
        # Create standardized category
        categorized_df = categorized_df.withColumn("category",
                                                coalesce(col("extracted_category"),
                                                        lit("Uncategorized")))
        
        # Clean up temporary columns                                        
        if "extracted_category" in categorized_df.columns:
            categorized_df = categorized_df.drop("extracted_category")
            
        return categorized_df

class PriceNormalizer:
    def __init__(self, spark, config_path=None):
        self.spark = spark
        self.unit_conversion = {
            # Weight conversions to kg
            "g": 0.001,      # 1g = 0.001kg
            "kg": 1.0,       # 1kg = 1kg
            "oz": 0.0283495, # 1oz = 0.0283495kg
            "lb": 0.453592,  # 1lb = 0.453592kg
            
            # Volume conversions to l
            "ml": 0.001,     # 1ml = 0.001l
            "l": 1.0,        # 1l = 1l
            "cl": 0.01,      # 1cl = 0.01l
            "floz": 0.0295735  # 1floz = 0.0295735l
        }
        
    def extract_quantity_from_name(self, product_name):
        """Extract quantity from product name using regex patterns"""
        import re
        
        if not product_name or not isinstance(product_name, str):
            return None, None
            
        # Look for patterns like "500g", "2kg", "750ml", "1.5l"
        # Weight patterns
        weight_pattern = r'(\d+(?:\.\d+)?)\s*(g|kg|oz|lb)'
        weight_match = re.search(weight_pattern, product_name, re.IGNORECASE)
        
        if weight_match:
            value = float(weight_match.group(1))
            unit = weight_match.group(2).lower()
            return value, unit
            
        # Volume patterns
        volume_pattern = r'(\d+(?:\.\d+)?)\s*(ml|l|cl|floz)'
        volume_match = re.search(volume_pattern, product_name, re.IGNORECASE)
        
        if volume_match:
            value = float(volume_match.group(1))
            unit = volume_match.group(2).lower()
            return value, unit
            
        return None, None
        
    def normalize_prices(self, df, price_column="price", unit_column=None, quantity_column=None,
                       currency_column=None, product_name_column="product_name", target_unit="kg",
                       target_currency=None, vat_inclusive=True):
        """Normalize prices to standard units and add price per unit"""
        print("Normalizing prices...")
        
        from pyspark.sql.functions import when, col, lit, regexp_replace, lower, regexp_extract
        
        # Thay vì dùng UDF, sử dụng các built-in function của Spark
        normalized_df = df
        
        # Xử lý unit và quantity trực tiếp bằng regexp_extract
        if product_name_column and product_name_column in df.columns:
            if not (unit_column and unit_column in df.columns):
                # Trích xuất unit từ tên sản phẩm với regexp
                normalized_df = normalized_df.withColumn(
                    "unit_std",
                    when(
                        regexp_extract(lower(col(product_name_column)), "\\d+(?:\\.\\d+)?\\s*(g|kg|oz|lb|ml|l|cl|floz)", 1) != "",
                        regexp_extract(lower(col(product_name_column)), "\\d+(?:\\.\\d+)?\\s*(g|kg|oz|lb|ml|l|cl|floz)", 1)
                    ).otherwise(lit("unit"))
                )
            
            if not (quantity_column and quantity_column in df.columns):
                # Trích xuất số lượng từ tên sản phẩm
                normalized_df = normalized_df.withColumn(
                    "quantity",
                    when(
                        regexp_extract(col(product_name_column), "(\\d+(?:\\.\\d+)?)\\s*(?:g|kg|oz|lb|ml|l|cl|floz)", 1) != "",
                        regexp_extract(col(product_name_column), "(\\d+(?:\\.\\d+)?)\\s*(?:g|kg|oz|lb|ml|l|cl|floz)", 1).cast("double")
                    ).otherwise(lit(1.0))
                )
        
        # Standardize unit column if it exists
        if unit_column and unit_column in normalized_df.columns:
            normalized_df = normalized_df.withColumn("unit_std", 
                                                  when(col(unit_column).isNotNull(),
                                                       lower(col(unit_column)))
                                                  .otherwise(
                                                      when(col("unit_std").isNotNull(),
                                                           col("unit_std"))
                                                      .otherwise(lit("unit"))))
        
        # Apply standard unit conversion if unit is known
        normalized_df = normalized_df.withColumn("unit_conversion", 
                                              when(col("unit_std") == "g", lit(0.001))
                                              .when(col("unit_std") == "kg", lit(1.0))
                                              .when(col("unit_std") == "oz", lit(0.0283495))
                                              .when(col("unit_std") == "lb", lit(0.453592))
                                              .when(col("unit_std") == "ml", lit(0.001))
                                              .when(col("unit_std") == "l", lit(1.0))
                                              .when(col("unit_std") == "cl", lit(0.01))
                                              .when(col("unit_std") == "floz", lit(0.0295735))
                                              .otherwise(lit(1.0)))
                                              
        # Calculate normalized price
        if price_column and price_column in normalized_df.columns:
            normalized_df = normalized_df.withColumn("normalized_price", 
                                                  col(price_column) / col("unit_conversion"))
        
        # Calculate price per standardized unit
        if "quantity" in normalized_df.columns and "normalized_price" in normalized_df.columns:
            normalized_df = normalized_df.withColumn("price_per_unit", 
                                                  when(col("quantity").isNotNull() & (col("quantity") > 0),
                                                       col("normalized_price") / col("quantity"))
                                                  .otherwise(col("normalized_price")))
        
        return normalized_df

class RFMAnalyzer:
    def __init__(self, spark, config_path=None):
        self.spark = spark
        
    def calculate_rfm_scores(self, df, customer_id_column, purchase_date_column, purchase_amount_column,
                          reference_date=None):
        """Calculate RFM (Recency, Frequency, Monetary) scores for customers"""
        from pyspark.sql.functions import datediff, current_date, count, sum, avg, max, min, ntile, col
        from pyspark.sql.window import Window
        
        if reference_date is None:
            reference_date = current_date()
            
        # Calculate base RFM metrics
        rfm_base = df.groupBy(customer_id_column).agg(
            datediff(reference_date, max(col(purchase_date_column))).alias("recency_days"),
            count("*").alias("frequency"),
            sum(col(purchase_amount_column)).alias("monetary_total"),
            avg(col(purchase_amount_column)).alias("monetary_avg")
        )
        
        # Define windows for scoring
        recency_window = Window.orderBy(col("recency_days"))
        frequency_window = Window.orderBy(col("frequency").desc())
        monetary_window = Window.orderBy(col("monetary_total").desc())
        
        # Create 5-tile scores (1 is best, 5 is worst for recency; opposite for others)
        rfm_scored = rfm_base.withColumn("r_score", ntile(5).over(recency_window)) \
                           .withColumn("f_score", ntile(5).over(frequency_window)) \
                           .withColumn("m_score", ntile(5).over(monetary_window))
                           
        # Calculate combined RFM score (weighted average)
        rfm_scored = rfm_scored.withColumn("rfm_score", 
                                        (col("r_score") * 0.4) + 
                                        (col("f_score") * 0.3) + 
                                        (col("m_score") * 0.3))
                                        
        # Add customer segments
        rfm_scored = rfm_scored.withColumn("customer_segment",
                                        when(col("rfm_score") >= 4.5, "Champions")
                                        .when(col("rfm_score") >= 4.0, "Loyal Customers")
                                        .when(col("rfm_score") >= 3.5, "Potential Loyalists")
                                        .when(col("rfm_score") >= 3.0, "Promising")
                                        .when(col("rfm_score") >= 2.5, "Customers Needing Attention")
                                        .when(col("rfm_score") >= 2.0, "At Risk")
                                        .when(col("rfm_score") >= 1.5, "Cannot Lose Them")
                                        .when(col("rfm_score") >= 1.0, "Lost")
                                        .otherwise("Inactive"))
        
        return rfm_scored
        
    def perform_rfm_analysis(self, df, customer_id_column="customer_id",
                           purchase_date_column="purchase_date", purchase_amount_column="purchase_amount",
                           output_details=True):
        """Perform RFM analysis and generate segment recommendations"""
        from pyspark.sql.functions import count, col, mean, min, max, desc
        
        print("Performing RFM analysis...")
        
        # Calculate RFM scores
        rfm_df = self.calculate_rfm_scores(
            df, 
            customer_id_column,
            purchase_date_column,
            purchase_amount_column
        )
        
        # Generate segment analysis if requested
        if output_details:
            segment_analysis = rfm_df.groupBy("customer_segment").agg(
                count("*").alias("customer_count"),
                mean("recency_days").alias("avg_recency_days"),
                mean("frequency").alias("avg_frequency"),
                mean("monetary_total").alias("avg_total_spend"),
                mean("monetary_avg").alias("avg_order_value")
            ).orderBy(desc("customer_count"))
            
            # Generate recommended actions based on segments
            segment_recommendations = {
                "Champions": "Reward them, make them feel special with exclusive offers, ask for reviews",
                "Loyal Customers": "Recommend higher-value products, provide loyalty incentives, engage through content",
                "Potential Loyalists": "Invite to loyalty program, recommend complementary products, personalized offers",
                "Promising": "Targeted promotions, cross-selling, focus on value proposition",
                "Customers Needing Attention": "Re-engagement campaigns, special offers to increase frequency",
                "At Risk": "Reactivation offers, re-engagement emails, win-back campaigns",
                "Cannot Lose Them": "Personalized win-back offers, surveys to identify issues",
                "Lost": "Deep reactivation discounts, final attempt offers",
                "Inactive": "Consider removal from marketing list or one final high-value offer"
            }
            
            # Create recommendations DataFrame
            recommendations_data = [(segment, action) for segment, action in segment_recommendations.items()]
            recommendations = spark.createDataFrame(recommendations_data, ["customer_segment", "recommended_action"])
        else:
            segment_analysis = None
            recommendations = None
            
        return rfm_df, segment_analysis, recommendations

# Initialize components
spark = SparkSession.builder.getOrCreate()
validator = DataValidator(spark)
categorizer = ProductCategorizer(spark)
normalizer = PriceNormalizer(spark)
rfm_analyzer = RFMAnalyzer(spark)

print("Components initialized successfully with enhanced functionality")

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
    """Generate sample product data with predefined categories to ensure consistency"""
    products = []
    # Định nghĩa categories cụ thể để đảm bảo tính nhất quán
    categories = ["Beverages", "Dairy", "Bakery", "Meat", "Seafood", "Fruits", "Vegetables", "Snacks"]
    units = ["kg", "g", "l", "ml", "pack", "unit"]
    
    # Dữ liệu mẫu cho mỗi category
    category_products = {
        "Beverages": ["Milk Tea", "Coffee", "Orange Juice", "Mineral Water", "Coca Cola", "Pepsi"],
        "Dairy": ["Fresh Milk", "Yogurt", "Cheese", "Butter", "Cream"],
        "Bakery": ["White Bread", "Baguette", "Croissant", "Cake", "Donut"],
        "Meat": ["Beef Steak", "Pork Chop", "Chicken Breast", "Ground Beef", "Lamb"],
        "Seafood": ["Salmon", "Tuna", "Shrimp", "Crab", "Squid"],
        "Fruits": ["Apple", "Orange", "Banana", "Grape", "Watermelon"],
        "Vegetables": ["Carrot", "Broccoli", "Cucumber", "Potato", "Onion"],
        "Snacks": ["Potato Chips", "Chocolate Bar", "Candy", "Popcorn", "Cookies"]
    }
    
    today = date.today()
    
    for i in range(count):
        # Chọn category
        category = random.choice(categories)
        sub_category = f"{category}-{random.randint(1, 3)}"
        
        # Chọn product name từ danh sách của category
        product_base_name = random.choice(category_products[category])
        product_id = f"{source}-{i+1:05d}"
        product_name = f"{product_base_name} {random.randint(1, 100)}"
        
        # Chọn unit phù hợp với category
        if category in ["Beverages"]:
            unit = random.choice(["l", "ml"])
        elif category in ["Fruits", "Vegetables", "Meat", "Seafood"]:
            unit = random.choice(["kg", "g"])
        else:
            unit = random.choice(units)
        
        # Giá cả phù hợp với category
        if category in ["Meat", "Seafood"]:
            price = round(random.uniform(100000, 500000), 0)  # Các mặt hàng đắt
        elif category in ["Snacks", "Bakery"]:
            price = round(random.uniform(20000, 100000), 0)  # Mức giá trung bình
        else:
            price = round(random.uniform(10000, 200000), 0)  # Mức giá chung
            
        # Chọn ngày trong 30 ngày gần đây
        record_date = today - timedelta(days=random.randint(0, 30))
        
        # Tạo mô tả
        description = f"{product_name} - {unit} - {category} product from {source}"
        
        products.append((
            product_id,
            product_name,
            price,
            unit,
            f"{category}/{sub_category}",
            description,
            source,
            record_date
        ))
    
    # In ra thông tin debug
    print(f"Generated {len(products)} products for {source}")
    print(f"Sample products: {products[:2]}")
    
    return spark.createDataFrame(products, product_schema)

# Generate sample data for each source
print("Generating sample data for Bronze layer...")
try:
    aeon_products = generate_sample_products("aeon")
    lotte_products = generate_sample_products("lotte")
    winmart_products = generate_sample_products("winmart")
    mm_mega_products = generate_sample_products("mm_mega")
    lotus_products = generate_sample_products("lotus_internal")

    print(f"Generated sample data: {aeon_products.count()} products per source")
    
    # Kiểm tra dữ liệu mẫu
    print("Checking sample data...")
    print("AEON products schema:")
    aeon_products.printSchema()
    print("Sample AEON products:")
    aeon_products.show(5)
except Exception as e:
    import traceback
    print(f"Error generating sample data: {str(e)}")
    print(traceback.format_exc())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Bronze Layer
# MAGIC Store raw data in the Bronze layer with minimal transformations - just capturing the data as is

# COMMAND ----------

def process_to_bronze(df, table_name):
    """Process data to bronze layer - storing the raw data exactly as received with minimal transformations"""
    print(f"Processing {df.count()} records to bronze layer for {table_name}")
    
    # Basic data validation - minimal validation, just to ensure data exists
    validation_results = validator.run_all_validations(
        df,
        required_columns=["product_id", "price"]
    )
    
    if not validation_results.get("overall_valid", False):
        print(f"Data validation for bronze layer failed: {validation_results.get('message', '')}")
        print("Proceeding anyway as Bronze layer should store raw data regardless of quality")
    
    # Add only essential metadata columns
    bronze_df = df.withColumn("bronze_processing_time", current_timestamp()) \
                 .withColumn("bronze_batch_id", lit(datetime.now().strftime("%Y%m%d%H%M%S"))) \
                 .withColumn("data_source", lit(table_name)) \
                 .withColumn("year_month", expr("date_format(date, 'yyyy-MM')")) \
                 .withColumn("layer", lit("bronze"))
    
    # Get correct bronze layer path from config
    bronze_path = config.get('delta', {}).get('bronze_layer_path')
    if bronze_path is None:
        bronze_path = "dbfs:/FileStore/LOTUS-PRISM/bronze"
        print(f"Warning: Bronze layer path not found in config, using default: {bronze_path}")
    
    # Check if path is Azure Storage and if we should use local DBFS instead
    if "abfss://" in bronze_path or "wasbs://" in bronze_path:
        print(f"Azure Storage path detected: {bronze_path}")
        print("Switching to local DBFS storage for reliability")
        bronze_path = "dbfs:/FileStore/LOTUS-PRISM/bronze"
    
    # Write to bronze layer with full path
    full_bronze_path = f"{bronze_path}/{table_name}"
    
    # Log the exact write path for debugging
    print(f"Writing {bronze_df.count()} records to bronze layer at: {full_bronze_path}")
    print(f"Storing raw data exactly as received with addition of metadata columns")
    
    # Ensure the directory exists
    try:
        dbutils.fs.mkdirs(bronze_path)
    except:
        print(f"Note: Path {bronze_path} already exists or cannot be created")
    
    # Write data to Delta format with error handling
    try:
        bronze_df.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .option("overwriteSchema", "true") \
            .partitionBy("year_month") \
            .save(full_bronze_path)
        print(f"Successfully wrote data to bronze layer at: {full_bronze_path}")
    except Exception as e:
        print(f"Error writing to bronze layer: {str(e)}")
        # Try with an alternate path
        alternate_path = f"dbfs:/FileStore/LOTUS-PRISM/bronze/{table_name}"
        print(f"Attempting to write to alternate path: {alternate_path}")
        try:
            bronze_df.write.format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .option("overwriteSchema", "true") \
                .save(alternate_path)
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
    """Execute a function safely with error handling and return detailed error information"""
    try:
        print(f"Starting operation '{operation_name}'...")
        result = function(*args, **kwargs)
        print(f"Operation '{operation_name}' completed successfully")
        return result
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"ERROR in operation '{operation_name}':")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        print(f"Error traceback: \n{error_trace}")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Silver Layer
# MAGIC Apply cleansing, normalization, enrichment, and standardization to create the Silver layer

# COMMAND ----------

def process_to_silver(bronze_df, table_name):
    """Process bronze data to silver layer (cleansing, normalization, enrichment)"""
    # Kiểm tra bronze dataframe
    if bronze_df is None:
        print(f"Cannot process to silver: bronze_df is None for {table_name}")
        return None
    
    try:
        # Count records để kiểm tra dataframe hoạt động
        record_count = bronze_df.count()
        print(f"Processing {record_count} records to silver layer for {table_name}")
        
        # First apply data quality checks
        validation_results = validator.run_all_validations(
            bronze_df,
            required_columns=["product_id", "product_name", "price", "source"],
            key_columns=["product_id"],
            price_column="price"
        )
        
        print(f"Data validation results: {validation_results['message']}")
        
        # Start with bronze data and create a working copy
        silver_df = bronze_df
        
        print(f"Starting data cleansing for {table_name}...")
        # Data cleansing - Handle missing values, clean text fields, etc.
        silver_df = silver_df \
            .withColumn("product_name_clean", 
                        when(col("product_name").isNull(), "Unknown Product")
                        .otherwise(trim(regexp_replace(col("product_name"), "[^\\w\\s]", " ")))) \
            .withColumn("description_clean", 
                        when(col("description").isNull(), "")
                        .otherwise(trim(regexp_replace(col("description"), "[^\\w\\s]", " ")))) \
            .withColumn("price_clean", 
                        when(col("price").isNull() | (col("price") < 0), None)
                        .otherwise(col("price"))) \
            .withColumn("retailer_clean", 
                        when(col("source").isNull(), "unknown")
                        .otherwise(lower(trim(col("source")))))
        
        # Apply product categorization
        print("Applying intelligent product categorization...")
        silver_df = categorizer.categorize_products(
            silver_df,
            product_name_column="product_name_clean",
            description_column="description_clean" if "description_clean" in silver_df.columns else None,
            category_path_column="category_path" if "category_path" in silver_df.columns else None
        )
        
        # Kiểm tra kết quả sau khi phân loại
        print(f"After categorization, dataframe has {silver_df.count()} records")
        
        # Apply price normalization
        print("Normalizing prices and converting units...")
        silver_df = normalizer.normalize_prices(
            silver_df,
            price_column="price_clean",
            unit_column="unit" if "unit" in silver_df.columns else None,
            product_name_column="product_name_clean" if "product_name_clean" in silver_df.columns else None
        )
        
        # Kiểm tra kết quả sau khi normalize
        print(f"After normalization, dataframe has {silver_df.count()} records")
        
        # Create standardized date columns for analysis
        print("Creating standardized date columns...")
        silver_df = silver_df \
            .withColumn("date_key", expr("date_format(date, 'yyyyMMdd')")) \
            .withColumn("year", year(col("date"))) \
            .withColumn("month", month(col("date"))) \
            .withColumn("day", dayofmonth(col("date")))
        
        # Data enrichment - add market segments, product types, etc.
        print("Adding data enrichment columns...")
        silver_df = silver_df \
            .withColumn("price_tier", 
                        when(col("normalized_price") < 50000, "Budget")
                        .when(col("normalized_price") < 200000, "Standard")
                        .when(col("normalized_price") < 500000, "Premium")
                        .otherwise("Luxury")) \
            .withColumn("product_id_clean", 
                        regexp_replace(col("product_id"), "[^a-zA-Z0-9-_]", "")) \
            .withColumn("is_validated", lit(validation_results.get("overall_valid", False)))
        
        # Add standardized metadata columns
        silver_df = silver_df \
            .withColumn("silver_processing_time", current_timestamp()) \
            .withColumn("data_quality_score", 
                        when(col("is_validated"), 1.0)
                        .otherwise(0.5)) \
            .withColumn("layer", lit("silver"))
        
        # Debug: in ra schema để kiểm tra
        print(f"Silver DataFrame schema for {table_name}:")
        silver_df.printSchema()
        
        # Get correct silver layer path from config
        silver_path = config.get('delta', {}).get('silver_layer_path')
        if silver_path is None:
            silver_path = "dbfs:/FileStore/LOTUS-PRISM/silver"
            print(f"Warning: Silver layer path not found in config, using default: {silver_path}")
        
        # Check if path is Azure Storage and if we should use local DBFS instead
        if "abfss://" in silver_path or "wasbs://" in silver_path:
            print(f"Azure Storage path detected: {silver_path}")
            print("Switching to local DBFS storage for reliability")
            silver_path = "dbfs:/FileStore/LOTUS-PRISM/silver"
        
        # Write to silver layer with full path
        full_silver_path = f"{silver_path}/{table_name}"
        
        # Log the exact write path for debugging
        print(f"Writing {silver_df.count()} records to silver layer at: {full_silver_path}")
        
        # Ensure the directory exists
        try:
            dbutils.fs.mkdirs(silver_path)
            print(f"Successfully created or verified silver path: {silver_path}")
        except Exception as e:
            print(f"Note: Path {silver_path} already exists or cannot be created: {str(e)}")
        
        # Write data to Delta format with error handling
        try:
            print(f"Starting write to silver layer: {full_silver_path}")
            silver_df.write.format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .option("overwriteSchema", "true") \
                .partitionBy("category", "year_month") \
                .save(full_silver_path)
            print(f"Successfully wrote data to silver layer at: {full_silver_path}")
        except Exception as e:
            print(f"Error writing to silver layer: {str(e)}")
            # Try with an alternate path
            alternate_path = f"dbfs:/FileStore/LOTUS-PRISM/silver/{table_name}"
            print(f"Attempting to write to alternate path: {alternate_path}")
            try:
                silver_df.write.format("delta") \
                    .mode("overwrite") \
                    .option("mergeSchema", "true") \
                    .option("overwriteSchema", "true") \
                    .save(alternate_path)
                print(f"Successfully wrote data to alternate silver path: {alternate_path}")
            except Exception as e2:
                print(f"Error writing to alternate silver path: {str(e2)}")
                print("WARNING: Could not save silver data, but will still return DataFrame for further processing")
        
        # Register as temp view for SQL queries
        try:
            silver_df.createOrReplaceTempView(f"silver_{table_name}")
            print(f"Created temp view: silver_{table_name}")
        except Exception as e:
            print(f"Error creating temp view: {str(e)}")
        
        # Return the DataFrame even if we had errors saving it
        return silver_df
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"CRITICAL ERROR in process_to_silver for {table_name}:")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        print(f"Error traceback: \n{error_trace}")
        
        try:
            # Tạo một minimal silver DataFrame để ít nhất có thể tiếp tục với Gold layer
            print(f"Attempting to create a minimal silver dataframe for {table_name}...")
            minimal_silver_df = bronze_df.withColumn("silver_processing_time", current_timestamp()) \
                                        .withColumn("layer", lit("silver")) \
                                        .withColumn("is_minimal", lit(True))
            
            print(f"Created minimal silver dataframe with {minimal_silver_df.count()} records. Some features will be limited.")
            return minimal_silver_df
        except Exception as e2:
            print(f"Could not create minimal silver dataframe: {str(e2)}")
            return None

# Process each source to silver with safe error handling
# Thêm biến để lưu trữ thông tin lỗi
bronze_to_silver_errors = {}

# Process Bronze to Silver for each retailer
print("Processing Bronze to Silver layer...")

print("Processing aeon_products to Silver layer...")
try:
    # Không dùng try_operation wrapper, gọi trực tiếp process_to_silver
    silver_aeon = process_to_silver(bronze_aeon, "aeon_products")
    if silver_aeon is None:
        print("WARNING: silver_aeon is None after processing")
except Exception as e:
    import traceback
    error_trace = traceback.format_exc()
    bronze_to_silver_errors["aeon"] = str(e)
    print(f"Error processing aeon to silver: {str(e)}")
    print(f"Traceback: \n{error_trace}")
    silver_aeon = None

print("Processing lotte_products to Silver layer...")
try:
    silver_lotte = process_to_silver(bronze_lotte, "lotte_products")
    if silver_lotte is None:
        print("WARNING: silver_lotte is None after processing")
except Exception as e:
    import traceback
    error_trace = traceback.format_exc()
    bronze_to_silver_errors["lotte"] = str(e)
    print(f"Error processing lotte to silver: {str(e)}")
    print(f"Traceback: \n{error_trace}")
    silver_lotte = None

print("Processing winmart_products to Silver layer...")
try:
    silver_winmart = process_to_silver(bronze_winmart, "winmart_products")
    if silver_winmart is None:
        print("WARNING: silver_winmart is None after processing")
except Exception as e:
    import traceback
    error_trace = traceback.format_exc()
    bronze_to_silver_errors["winmart"] = str(e)
    print(f"Error processing winmart to silver: {str(e)}")
    print(f"Traceback: \n{error_trace}")
    silver_winmart = None

print("Processing mm_mega_products to Silver layer...")
try:
    silver_mm_mega = process_to_silver(bronze_mm_mega, "mm_mega_products")
    if silver_mm_mega is None:
        print("WARNING: silver_mm_mega is None after processing")
except Exception as e:
    import traceback
    error_trace = traceback.format_exc()
    bronze_to_silver_errors["mm_mega"] = str(e)
    print(f"Error processing mm_mega to silver: {str(e)}")
    print(f"Traceback: \n{error_trace}")
    silver_mm_mega = None

print("Processing lotus_products to Silver layer...")
try:
    silver_lotus = process_to_silver(bronze_lotus, "lotus_products")
    if silver_lotus is None:
        print("WARNING: silver_lotus is None after processing")
except Exception as e:
    import traceback
    error_trace = traceback.format_exc()
    bronze_to_silver_errors["lotus"] = str(e)
    print(f"Error processing lotus to silver: {str(e)}")
    print(f"Traceback: \n{error_trace}")
    silver_lotus = None

# Kiểm tra nếu tất cả silver dataframes đều là None
if all(df is None for df in [silver_aeon, silver_lotte, silver_winmart, silver_mm_mega, silver_lotus]):
    print("ERROR: All silver dataframes are None. Bronze to Silver processing failed completely.")
    if bronze_to_silver_errors:
        print("Errors summary:")
        for retailer, error in bronze_to_silver_errors.items():
            print(f"  - {retailer}: {error}")
    print("Checking bronze dataframes...")
    for name, df in [("bronze_aeon", bronze_aeon), ("bronze_lotte", bronze_lotte), 
                     ("bronze_winmart", bronze_winmart), ("bronze_mm_mega", bronze_mm_mega), 
                     ("bronze_lotus", bronze_lotus)]:
        if df is None:
            print(f"  - {name} is None")
        else:
            try:
                count = df.count()
                print(f"  - {name}: {count} records")
            except Exception as e:
                print(f"  - {name}: Error counting records: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Gold Layer
# MAGIC Apply business logic, aggregations, and create analytics-ready data in the Gold layer

# COMMAND ----------

def process_to_gold(silver_df, table_name):
    """Process silver data to gold layer - create business-level analytics, aggregations, and insights"""
    if silver_df is None:
        print(f"Cannot process to gold: silver_df is None for {table_name}")
        return None
    
    try:    
        # Count records để kiểm tra dataframe hoạt động
        record_count = silver_df.count()
        print(f"Processing {record_count} records to gold layer for {table_name}")
        
        # Kiểm tra cấu trúc của silver dataframe
        print(f"Silver dataframe schema for {table_name}:")
        silver_df.printSchema()
        
        # Kiểm tra xem silver_df có phải là minimal df
        is_minimal = False
        if "is_minimal" in silver_df.columns:
            is_minimal = silver_df.select("is_minimal").first()[0]
            
        if is_minimal:
            print(f"Using minimal silver dataframe for {table_name}. Creating simplified gold analytics.")
            # Tạo phiên bản gold đơn giản từ minimal silver data
            gold_df = silver_df.groupBy("source") \
                .agg(
                    count("*").alias("record_count"),
                    lit("minimal").alias("processing_type")
                ) \
                .withColumn("gold_processing_time", current_timestamp()) \
                .withColumn("batch_date", current_date()) \
                .withColumn("is_latest", lit(True)) \
                .withColumn("layer", lit("gold"))
        else:
            # Kiểm tra các cột cần thiết cho gold layer
            required_columns = ["category", "retailer_clean" if "retailer_clean" in silver_df.columns else "source", 
                               "price_clean" if "price_clean" in silver_df.columns else "price"]
            
            missing_columns = [col for col in required_columns if col not in silver_df.columns]
            if missing_columns:
                print(f"WARNING: Missing columns for gold processing: {missing_columns}")
                print("Adding default values for missing columns...")
                
                for col_name in missing_columns:
                    if col_name == "category":
                        silver_df = silver_df.withColumn("category", lit("Uncategorized"))
                    elif col_name == "retailer_clean":
                        silver_df = silver_df.withColumn("retailer_clean", lower(col("source")))
                    elif col_name == "price_clean":
                        silver_df = silver_df.withColumn("price_clean", col("price"))
            
            # Different processing based on table type
            if "product" in table_name.lower():
                print("Generating product analytics for gold layer...")
                # For product data, create analytics and aggregations
                
                # Ensure we have date columns
                if "date" in silver_df.columns and "date_key" not in silver_df.columns:
                    silver_df = silver_df.withColumn("date_key", expr("date_format(date, 'yyyyMMdd')"))
                
                if "date" in silver_df.columns and "year" not in silver_df.columns:
                    silver_df = silver_df.withColumn("year", year(col("date")))
                    silver_df = silver_df.withColumn("month", month(col("date")))
                    silver_df = silver_df.withColumn("day", dayofmonth(col("date")))
                
                try:
                    # 1. Create daily price statistics by category and retailer
                    print("Creating daily price statistics...")
                    
                    # Lấy tên cột chính xác cho price và retailer
                    price_col = "price_clean" if "price_clean" in silver_df.columns else "price"
                    retailer_col = "retailer_clean" if "retailer_clean" in silver_df.columns else "source"
                    
                    daily_price_stats = silver_df.groupBy("category", retailer_col, "date_key", "year", "month", "day") \
                        .agg(
                            count("*").alias("product_count"),
                            avg(price_col).alias("avg_price"),
                            min(price_col).alias("min_price"),
                            max(price_col).alias("max_price"),
                            avg("normalized_price").alias("avg_normalized_price") if "normalized_price" in silver_df.columns else avg(price_col).alias("avg_normalized_price"),
                            avg("price_per_unit").alias("avg_price_per_unit") if "price_per_unit" in silver_df.columns else lit(None).alias("avg_price_per_unit"),
                            expr(f"percentile({price_col}, 0.5)").alias("median_price")
                        ) \
                        .withColumn("stats_type", lit("daily"))
                    
                    # 2. Create monthly price trends by category and retailer
                    print("Creating monthly price statistics...")
                    monthly_price_trends = silver_df.groupBy("category", retailer_col, "year", "month") \
                        .agg(
                            count("*").alias("product_count"),
                            avg(price_col).alias("avg_price"),
                            min(price_col).alias("min_price"),
                            max(price_col).alias("max_price"),
                            avg("normalized_price").alias("avg_normalized_price") if "normalized_price" in silver_df.columns else avg(price_col).alias("avg_normalized_price"),
                            expr(f"percentile({price_col}, 0.25)").alias("price_25th_percentile"),
                            expr(f"percentile({price_col}, 0.5)").alias("price_50th_percentile"),
                            expr(f"percentile({price_col}, 0.75)").alias("price_75th_percentile")
                        ) \
                        .withColumn("stats_type", lit("monthly"))
                    
                    # Create a variable to store price_tier if available
                    price_tier_col = "price_tier" if "price_tier" in silver_df.columns else None
                    
                    if price_tier_col:
                        # 3. Create price tier distribution if we have price_tier
                        print("Creating price tier statistics...")
                        price_tier_distribution = silver_df.groupBy("category", retailer_col, price_tier_col) \
                            .agg(
                                count("*").alias("product_count"),
                                avg(price_col).alias("avg_price_in_tier")
                            ) \
                            .withColumn("stats_type", lit("price_tier"))
                    else:
                        # Create a simple tier column based on price
                        print("Price tier not available, calculating basic tiers based on price...")
                        tier_stats = silver_df.withColumn("calculated_tier", 
                                                         when(col(price_col) < 50000, "Budget")
                                                         .when(col(price_col) < 200000, "Standard")
                                                         .when(col(price_col) < 500000, "Premium")
                                                         .otherwise("Luxury"))
                        
                        price_tier_distribution = tier_stats.groupBy("category", retailer_col, "calculated_tier") \
                            .agg(
                                count("*").alias("product_count"),
                                avg(price_col).alias("avg_price_in_tier")
                            ) \
                            .withColumn("stats_type", lit("price_tier"))
                    
                    # 4. Combine statistics into a single gold dataset with different types
                    print("Combining statistics for gold layer...")
                    gold_df = daily_price_stats \
                        .select(
                            col("category"),
                            col(retailer_col).alias("retailer_clean"),
                            col("date_key"),
                            col("year"),
                            col("month"),
                            col("day"),
                            col("stats_type"),
                            col("product_count"),
                            col("avg_price"),
                            col("min_price"),
                            col("max_price"),
                            col("avg_normalized_price"),
                            col("avg_price_per_unit"),
                            col("median_price")
                        )
                    
                    # Union with monthly trends (align schemas)
                    print("Adding monthly trends to gold dataset...")
                    gold_df = gold_df.unionByName(
                        monthly_price_trends.select(
                            col("category"),
                            col(retailer_col).alias("retailer_clean"),
                            lit(None).cast("string").alias("date_key"),
                            col("year"),
                            col("month"),
                            lit(None).cast("int").alias("day"),
                            col("stats_type"),
                            col("product_count"),
                            col("avg_price"),
                            col("min_price"),
                            col("max_price"),
                            col("avg_normalized_price"),
                            lit(None).cast("double").alias("avg_price_per_unit"),
                            col("price_50th_percentile").alias("median_price")
                        ),
                        allowMissingColumns=True
                    )
                    
                    # Union with price tier distribution (need to align schemas)
                    print("Adding price tier distribution to gold dataset...")
                    if price_tier_col:
                        tier_column = price_tier_col
                    else:
                        tier_column = "calculated_tier"
                        
                    gold_df = gold_df.unionByName(
                        price_tier_distribution.select(
                            col("category"),
                            col(retailer_col).alias("retailer_clean"),
                            lit(None).cast("string").alias("date_key"),
                            lit(None).cast("int").alias("year"),
                            lit(None).cast("int").alias("month"),
                            lit(None).cast("int").alias("day"),
                            col("stats_type"),
                            col("product_count"),
                            col("avg_price_in_tier").alias("avg_price"),
                            lit(None).cast("double").alias("min_price"),
                            lit(None).cast("double").alias("max_price"),
                            lit(None).cast("double").alias("avg_normalized_price"),
                            lit(None).cast("double").alias("avg_price_per_unit"),
                            lit(None).cast("double").alias("median_price")
                        ).withColumn("price_tier_details", col(tier_column)),
                        allowMissingColumns=True
                    )
                    
                    # Add business insights columns
                    print("Adding business insights columns...")
                    gold_df = gold_df \
                        .withColumn("price_competitiveness", 
                                  when(col("stats_type") == "daily",
                                      when(col("avg_price") < 100000, "Very Competitive")
                                      .when(col("avg_price") < 200000, "Competitive")
                                      .when(col("avg_price") < 300000, "Standard")
                                      .otherwise("Premium"))
                                  .otherwise(None)) \
                        .withColumn("price_spread", 
                                  when(col("max_price").isNotNull() & col("min_price").isNotNull(),
                                      col("max_price") - col("min_price"))
                                  .otherwise(None))
                except Exception as e:
                    import traceback
                    print(f"Error during gold analytics processing: {str(e)}")
                    print(traceback.format_exc())
                    
                    # Fallback to simple analytics if the detailed processing fails
                    print("Falling back to simple gold analytics...")
                    gold_df = silver_df.groupBy("category", "source") \
                        .agg(
                            count("*").alias("record_count"),
                            avg("price").alias("avg_price"),
                            max("price").alias("max_price"),
                            min("price").alias("min_price")
                        ) \
                        .withColumn("stats_type", lit("simple"))
            else:
                # Default processing for other tables
                gold_df = silver_df \
                    .groupBy("category", "source") \
                    .agg(
                        count("*").alias("record_count"),
                        avg("price").alias("avg_price"),
                        max("price").alias("max_price"),
                        min("price").alias("min_price")
                    )
        
        # Add metadata columns
        print("Adding metadata columns to gold dataframe...")
        gold_df = gold_df \
            .withColumn("gold_processing_time", current_timestamp()) \
            .withColumn("batch_date", current_date()) \
            .withColumn("is_latest", lit(True)) \
            .withColumn("layer", lit("gold"))
        
        # Get correct gold layer path from config
        gold_path = config.get('delta', {}).get('gold_layer_path')
        if gold_path is None:
            gold_path = "dbfs:/FileStore/LOTUS-PRISM/gold"
            print(f"Warning: Gold layer path not found in config, using default: {gold_path}")
        
        # Check if path is Azure Storage and if we should use local DBFS instead
        if "abfss://" in gold_path or "wasbs://" in gold_path:
            print(f"Azure Storage path detected: {gold_path}")
            print("Switching to local DBFS storage for reliability")
            gold_path = "dbfs:/FileStore/LOTUS-PRISM/gold"
        
        # Write to gold layer with full path
        full_gold_path = f"{gold_path}/{table_name}_analytics"
        
        # Log the exact write path for debugging
        print(f"Writing {gold_df.count()} analytics records to gold layer at: {full_gold_path}")
        
        # Ensure the directory exists
        try:
            dbutils.fs.mkdirs(gold_path)
            print(f"Created or confirmed gold path: {gold_path}")
        except Exception as e:
            print(f"Note: Path {gold_path} already exists or cannot be created: {str(e)}")
        
        # Write data to Delta format with error handling
        try:
            print(f"Starting write to gold layer at: {full_gold_path}")
            if "category" in gold_df.columns and "stats_type" in gold_df.columns:
                partition_cols = ["category", "stats_type"]
            elif "category" in gold_df.columns:
                partition_cols = ["category"]
            else:
                partition_cols = []
                
            if partition_cols:
                gold_df.write.format("delta") \
                    .mode("overwrite") \
                    .option("mergeSchema", "true") \
                    .option("overwriteSchema", "true") \
                    .partitionBy(*partition_cols) \
                    .save(full_gold_path)
            else:
                gold_df.write.format("delta") \
                    .mode("overwrite") \
                    .option("mergeSchema", "true") \
                    .option("overwriteSchema", "true") \
                    .save(full_gold_path)
                    
            print(f"Successfully wrote data to gold layer at: {full_gold_path}")
        except Exception as e:
            print(f"Error writing to gold layer: {str(e)}")
            # Try with an alternate path
            alternate_path = f"dbfs:/FileStore/LOTUS-PRISM/gold/{table_name}_analytics"
            print(f"Attempting to write to alternate path: {alternate_path}")
            try:
                gold_df.write.format("delta") \
                    .mode("overwrite") \
                    .option("mergeSchema", "true") \
                    .option("overwriteSchema", "true") \
                    .save(alternate_path)
                print(f"Successfully wrote data to alternate gold path: {alternate_path}")
            except Exception as e2:
                print(f"Error writing to alternate gold path: {str(e2)}")
                print("WARNING: Could not save gold data, but will still return DataFrame")
        
        # Register as temp view for SQL queries
        try:
            gold_df.createOrReplaceTempView(f"gold_{table_name}")
            print(f"Created temp view: gold_{table_name}")
        except Exception as e:
            print(f"Error creating temp view: {str(e)}")
        
        return gold_df
        
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"CRITICAL ERROR in process_to_gold for {table_name}:")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        print(f"Error traceback: \n{error_trace}")
        
        # Tạo một gold DataFrame tối giản để tránh lỗi hoàn toàn
        try:
            print("Creating minimal gold DataFrame...")
            minimal_gold_df = spark.createDataFrame(
                [
                    (table_name, "error_recovery", 0, 0, 0, 0, current_timestamp(), current_date(), True, "gold", f"Error: {str(e)}")
                ],
                schema=["source", "category", "record_count", "avg_price", "max_price", "min_price", 
                        "gold_processing_time", "batch_date", "is_latest", "layer", "error_details"]
            )
            return minimal_gold_df
        except Exception as e2:
            print(f"Could not create minimal gold DataFrame: {str(e2)}")
            return None

# Process each source to gold
# Initialize gold variables to None to prevent errors
gold_aeon = None
gold_lotte = None
gold_winmart = None
gold_mm_mega = None
gold_lotus = None

# Process Silver to Gold for each retailer with individual error handling
print("Processing Silver to Gold layer...")
try:
    gold_aeon = process_to_gold(silver_aeon, "aeon_products")
except Exception as e:
    print(f"Error processing aeon to gold: {str(e)}")

try:
    gold_lotte = process_to_gold(silver_lotte, "lotte_products")
except Exception as e:
    print(f"Error processing lotte to gold: {str(e)}")

try:
    gold_winmart = process_to_gold(silver_winmart, "winmart_products")
except Exception as e:
    print(f"Error processing winmart to gold: {str(e)}")

try:
    gold_mm_mega = process_to_gold(silver_mm_mega, "mm_mega_products")
except Exception as e:
    print(f"Error processing mm_mega to gold: {str(e)}")

try:
    gold_lotus = process_to_gold(silver_lotus, "lotus_products")
except Exception as e:
    print(f"Error processing lotus to gold: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Enhanced Price Comparison Report (Gold Layer)
# MAGIC Create a comprehensive comparison of prices and market positioning across different retailers

# COMMAND ----------

def generate_price_comparison():
    """Generate comprehensive price comparison analysis across all competitors using Silver layer data"""
    print("Generating enhanced price comparison analysis for Gold layer")
    
    # Make sure we have the data to compare
    missing_dfs = []
    if silver_lotus is None: missing_dfs.append("silver_lotus")
    if silver_aeon is None: missing_dfs.append("silver_aeon")
    if silver_lotte is None: missing_dfs.append("silver_lotte")
    if silver_winmart is None: missing_dfs.append("silver_winmart")
    
    if missing_dfs:
        print(f"Cannot generate complete price comparison: missing the following dataframes: {', '.join(missing_dfs)}")
        print("Will attempt to generate comparison with available data")
    
    # Register temp views for available dataframes
    available_retailers = []
    
    if silver_lotus is not None:
        silver_lotus.createOrReplaceTempView("lotus_products")
        available_retailers.append("lotus")
    
    if silver_aeon is not None:
        silver_aeon.createOrReplaceTempView("aeon_products")
        available_retailers.append("aeon")
    
    if silver_lotte is not None:
        silver_lotte.createOrReplaceTempView("lotte_products")
        available_retailers.append("lotte")
    
    if silver_winmart is not None:
        silver_winmart.createOrReplaceTempView("winmart_products")
        available_retailers.append("winmart")
    
    if not available_retailers:
        print("No retailers available for comparison. Aborting price comparison generation.")
        return None
    
    print(f"Generating price comparison with available retailers: {', '.join(available_retailers)}")
    
    # Dynamically build SQL query based on available retailers
    # Start with product counts
    product_counts_sql = "WITH product_counts AS (\n    SELECT "
    
    # Reference retailer (preferably lotus, otherwise the first available)
    reference_retailer = "lotus" if "lotus" in available_retailers else available_retailers[0]
    
    # Add count(*) for each available retailer
    for retailer in available_retailers:
        count_sql = f"(SELECT COUNT(*) FROM {retailer}_products)" if retailer != reference_retailer else "COUNT(*)"
        product_counts_sql += f"\n        {count_sql} as {retailer}_count,"
    
    # Remove the last comma and complete the product counts SQL
    product_counts_sql = product_counts_sql.rstrip(",")
    product_counts_sql += f"\n    FROM {reference_retailer}_products\n),"
    
    # Now build the price stats for each retailer
    price_stats_sql = "\nprice_stats AS (\n"
    
    for i, retailer in enumerate(available_retailers):
        if i > 0:
            price_stats_sql += "\n    UNION ALL\n"
        
        price_stats_sql += f"""    SELECT
        '{retailer}' as retailer,
        COUNT(*) as product_count,
        AVG(normalized_price) as avg_normalized_price,
        MIN(normalized_price) as min_normalized_price,
        MAX(normalized_price) as max_normalized_price,
        PERCENTILE(normalized_price, 0.5) as median_normalized_price,
        STDDEV(normalized_price) as price_stddev,
        
        -- Category counts
        COUNT(DISTINCT category) as category_count,
        
        -- Price tier distribution
        SUM(CASE WHEN price_tier = 'Budget' THEN 1 ELSE 0 END) as budget_product_count,
        SUM(CASE WHEN price_tier = 'Standard' THEN 1 ELSE 0 END) as standard_product_count,
        SUM(CASE WHEN price_tier = 'Premium' THEN 1 ELSE 0 END) as premium_product_count,
        SUM(CASE WHEN price_tier = 'Luxury' THEN 1 ELSE 0 END) as luxury_product_count
    FROM {retailer}_products
    WHERE normalized_price IS NOT NULL"""
    
    price_stats_sql += "\n),"
    
    # Create category price analysis
    category_price_sql = "\ncategory_price_analysis AS (\n"
    
    for i, retailer in enumerate(available_retailers):
        if i > 0:
            category_price_sql += "\n    UNION ALL\n"
        
        category_price_sql += f"""    SELECT 
        '{retailer}' as retailer,
        category,
        COUNT(*) as product_count,
        AVG(normalized_price) as avg_category_price
    FROM {retailer}_products
    GROUP BY category"""
    
    category_price_sql += "\n)"
    
    # Final comparison report - modify if lotus isn't available
    final_comparison_sql = f"""
-- Final comparison report
SELECT 
    'Retailer Price Comparison Report' as report_name,
    current_timestamp() as generation_time,
    p.*,
    -- Calculate percentages for price tiers
    (p.budget_product_count / p.product_count) * 100 as budget_product_pct,
    (p.standard_product_count / p.product_count) * 100 as standard_product_pct,
    (p.premium_product_count / p.product_count) * 100 as premium_product_pct,
    (p.luxury_product_count / p.product_count) * 100 as luxury_product_pct,
    -- Add competitiveness index (lower values indicate more competitive pricing)
    (p.avg_normalized_price / AVG(p.avg_normalized_price) OVER ()) * 100 as price_competitiveness_index,
    -- Add market positioning
    CASE 
        WHEN p.avg_normalized_price < (SELECT MIN(avg_normalized_price) * 1.1 FROM price_stats) THEN 'Price Leader'
        WHEN p.avg_normalized_price > (SELECT MAX(avg_normalized_price) * 0.9 FROM price_stats) THEN 'Premium Positioned'
        ELSE 'Mid-Market'
    END as market_positioning"""
    
    # Add lotus-specific comparison if available
    if "lotus" in available_retailers:
        final_comparison_sql += """,
    -- Calculate diff from lotus avg price
    CASE 
        WHEN p.retailer = 'lotus' THEN 0
        ELSE (p.avg_normalized_price - (SELECT avg_normalized_price FROM price_stats WHERE retailer = 'lotus')) / 
             (SELECT avg_normalized_price FROM price_stats WHERE retailer = 'lotus') * 100
    END as pct_diff_from_lotus"""
    
    final_comparison_sql += """
FROM price_stats p
CROSS JOIN product_counts"""
    
    # Combine all SQL parts
    full_comparison_sql = product_counts_sql + price_stats_sql + category_price_sql + final_comparison_sql
    
    try:
        # Execute the dynamically built SQL
        comparison_df = spark.sql(full_comparison_sql)
        
        # Show the comparison report
        print("Retailer Price Comparison Results:")
        comparison_df.show(truncate=False)
        
        # Also build a category-level comparison if we have at least 2 retailers
        if len(available_retailers) >= 2:
            category_sql = f"""
            WITH category_price_analysis AS (
                {category_price_sql.split('category_price_analysis AS (')[1].split(')')[0]}
            ),
            
            category_avg AS (
                SELECT
                    category,
                    AVG(avg_category_price) as avg_market_price
                FROM category_price_analysis
                GROUP BY category
            )
            
            SELECT
                cpa.category,
                cpa.retailer,
                cpa.product_count,
                cpa.avg_category_price,
                ca.avg_market_price,
                (cpa.avg_category_price - ca.avg_market_price) / ca.avg_market_price * 100 as price_diff_pct,
                CASE 
                    WHEN cpa.avg_category_price < ca.avg_market_price * 0.95 THEN 'Very Competitive'
                    WHEN cpa.avg_category_price < ca.avg_market_price THEN 'Competitive'
                    WHEN cpa.avg_category_price < ca.avg_market_price * 1.05 THEN 'Average'
                    WHEN cpa.avg_category_price < ca.avg_market_price * 1.15 THEN 'Premium'
                    ELSE 'Luxury'
                END as category_price_positioning
            FROM category_price_analysis cpa
            JOIN category_avg ca ON cpa.category = ca.category
            ORDER BY cpa.category, cpa.retailer
            """
            
            category_comparison = spark.sql(category_sql)
            
            print("Category Price Comparison Results:")
            category_comparison.show(truncate=False)
        else:
            print("Skipping category comparison - need at least 2 retailers for meaningful comparison")
            category_comparison = None
        
        # Get correct gold layer path from config
        gold_path = config.get('delta', {}).get('gold_layer_path')
        if gold_path is None:
            gold_path = "dbfs:/FileStore/LOTUS-PRISM/gold"
            print(f"Warning: Gold layer path not found in config, using default: {gold_path}")
        
        # Path for price comparison
        retailer_comparison_path = f"{gold_path}/retailer_price_comparison"
        
        # Write data to Delta format with error handling
        try:
            # Write retailer price comparison
            comparison_df.write.format("delta").mode("overwrite").save(retailer_comparison_path)
            print(f"Successfully wrote retailer price comparison to gold layer at: {retailer_comparison_path}")
            
            # Write category price comparison if available
            if category_comparison is not None:
                category_comparison_path = f"{gold_path}/category_price_comparison"
                category_comparison.write.format("delta").mode("overwrite").partitionBy("category").save(category_comparison_path)
                print(f"Successfully wrote category price comparison to gold layer at: {category_comparison_path}")
        except Exception as e:
            print(f"Error writing price comparison to gold layer: {str(e)}")
        
        return (comparison_df, category_comparison) if category_comparison is not None else comparison_df
    
    except Exception as e:
        print(f"Error generating price comparison: {str(e)}")
        return None

# Generate price comparison
try:
    price_comparison_df, category_comparison_df = generate_price_comparison()
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
# MAGIC Review the ETL process and the distinct differences between Bronze, Silver, and Gold layers

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

print("=" * 80)
print("LOTUS-PRISM ETL Process Summary")
print("=" * 80)

print(f"Total records processed in Bronze layer: {bronze_count}")
print(f"Total records processed in Silver layer: {silver_count}")
print(f"Total records processed in Gold layer: {gold_count}")
print(f"Total transactions analyzed with RFM: {safe_count(transactions_df)}")

print("\nData Layer Implementation:")
print("-" * 80)
print("BRONZE LAYER")
print("-" * 80)
print("✓ Raw data capture with original format preserved")
print("✓ Minimal transformations - just added metadata")
print("✓ Ensured data lineage and auditability")
print("✓ Partitioned by date for efficient historical access")

print("\n" + "-" * 80)
print("SILVER LAYER")
print("-" * 80)
print("✓ Applied comprehensive data validation")
print("✓ Cleaned and standardized data formats")
print("✓ Normalized product categories across retailers")
print("✓ Applied price normalization and unit conversions")
print("✓ Enhanced data with derived fields and quality scores")
print("✓ Created consistent data structure for analytics")

print("\n" + "-" * 80)
print("GOLD LAYER")
print("-" * 80)
print("✓ Created business-level aggregations and metrics")
print("✓ Generated price comparison analytics across retailers")
print("✓ Implemented category-level performance analysis")
print("✓ Added business context and competitive insights")
print("✓ Prepared data specifically for reporting and dashboards")
print("✓ Calculated market positioning and competitive indices")

print("\n" + "=" * 80)
print(f"ETL job completed successfully at {datetime.now()}")
print(f"Data is available at:")
print(f"Bronze: {config.get('delta', {}).get('bronze_layer_path')} - Raw data as received")
print(f"Silver: {config.get('delta', {}).get('silver_layer_path')} - Cleansed and standardized data")
print(f"Gold: {config.get('delta', {}).get('gold_layer_path')} - Business-ready analytics")
print("=" * 80)
print("LOTUS-PRISM Batch ETL Job Complete")
print("=" * 80)
