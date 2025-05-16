# Databricks notebook source

# MAGIC %pip install pyyaml

# COMMAND ----------

# MAGIC %md
# MAGIC # LOTUS-PRISM Streaming Data Processing Demo
# MAGIC 
# MAGIC This notebook demonstrates the real-time streaming data processing for the LOTUS-PRISM retail price intelligence system.
# MAGIC 
# MAGIC ## Overview
# MAGIC - Processing real-time price changes from competitors
# MAGIC - Using Structured Streaming with Kafka/Event Hubs
# MAGIC - Implementing real-time price change detection
# MAGIC - Generating notifications for significant price changes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Import necessary libraries and set up the environment

# COMMAND ----------

# Import required libraries
import os
import sys
import json
import yaml
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, expr, from_json, to_json, struct, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType

# Add the src directory to the path to import local modules
current_dir = os.path.dirname(os.path.abspath("__file__"))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configuration
# MAGIC Load the streaming processing configuration

# COMMAND ----------

# Load configuration
def load_config(config_path="dbfs:/FileStore/LOTUS-PRISM/configs/streaming_config.yaml"):
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
            "kafka": {
                "bootstrap_servers": "lotus-prism-eventhub.servicebus.windows.net:9093",
                "topics": {
                    "price_changes": "price-changes",
                    "competitor_prices": "competitor-prices"
                }
            },
            "processing": {
                "trigger_interval": "10 seconds",
                "checkpoint_location": "dbfs:/mnt/checkpoints/streaming"
            },
            "delta": {
                "silver_layer_path": "dbfs:/mnt/silver",
                "gold_layer_path": "dbfs:/mnt/gold"
            },
            "notifications": {
                "price_change_threshold": 0.05
            }
        }

# Get config path from parameters if available
try:
    config_path = dbutils.widgets.get("config_path")
except:
    config_path = "dbfs:/FileStore/LOTUS-PRISM/configs/streaming_config.yaml"

# Load the configuration
config = load_config(config_path)
print(f"Configuration loaded successfully from {config_path}")
print(f"Silver layer path: {config.get('delta', {}).get('silver_layer_path')}")
print(f"Gold layer path: {config.get('delta', {}).get('gold_layer_path')}")
print(f"Checkpoint location: {config.get('processing', {}).get('checkpoint_location')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Spark Session
# MAGIC Set up the Spark session with necessary configurations

# COMMAND ----------

# Create Spark session (in Databricks this is already available)
spark = SparkSession.builder \
    .appName("LOTUS-PRISM-Streaming") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("WARN")

print("Spark session initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Schema for Price Change Events
# MAGIC Create the schema for price change events

# COMMAND ----------

# Define the schema for price change events
price_change_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("retailer", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("old_price", DoubleType(), True),
    StructField("new_price", DoubleType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("location", StringType(), True),
    StructField("is_promotion", BooleanType(), True)
])

print("Schema defined for price change events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Sample Streaming Data
# MAGIC Create a sample data generator to simulate real-time price changes

# COMMAND ----------

# Create a stream of sample data for demonstration
def generate_sample_price_changes(batch_size=10):
    """Generate a batch of sample price change events"""
    
    now = datetime.now()
    retailers = ["aeon", "lotte", "winmart", "mm_mega"]
    categories = ["Beverages", "Dairy", "Bakery", "Meat", "Seafood", "Fruits", "Vegetables", "Snacks"]
    locations = ["HoChiMinh", "Hanoi", "DaNang", "CanTho", "NhaTrang"]
    
    data = []
    
    for i in range(batch_size):
        retailer = random.choice(retailers)
        category = random.choice(categories)
        product_id = f"{retailer}-{random.randint(1, 1000):04d}"
        old_price = random.uniform(10000, 500000)
        
        # Add some significant price changes
        if random.random() < 0.2:  # 20% chance of significant change
            change_pct = random.uniform(0.1, 0.3)  # 10-30% change
            direction = -1 if random.random() < 0.6 else 1  # More likely to be a price drop
        else:
            change_pct = random.uniform(0.01, 0.08)  # 1-8% change
            direction = -1 if random.random() < 0.5 else 1  # Equal chance of increase/decrease
            
        new_price = old_price * (1 + direction * change_pct)
        
        # Format to 2 decimal places
        old_price = round(old_price, 2)
        new_price = round(new_price, 2)
        
        # Create an event
        event = {
            "event_id": f"evt-{now.strftime('%Y%m%d%H%M%S')}-{i}",
            "product_id": product_id,
            "retailer": retailer,
            "timestamp": (now - timedelta(seconds=random.randint(0, 60))).isoformat(),
            "old_price": old_price,
            "new_price": new_price,
            "product_name": f"{category} Product {random.randint(1, 100)}",
            "category": category,
            "unit": random.choice(["kg", "g", "l", "ml", "pack", "unit"]),
            "location": random.choice(locations),
            "is_promotion": random.random() < 0.3  # 30% chance of being a promotion
        }
        
        data.append(event)
    
    return spark.createDataFrame(data, schema=price_change_schema)

# Create a continuous stream of data for testing
def create_data_stream():
    """Create a streaming DataFrame that generates data every few seconds"""
    
    return spark.readStream.format("rate") \
        .option("rowsPerSecond", 5) \
        .load() \
        .selectExpr("value", "timestamp") \
        .select(
            expr("uuid()").alias("event_id"),
            expr("concat('prod-', cast(rand() * 1000 as int))").alias("product_id"),
            expr("case when rand() < 0.25 then 'aeon' when rand() < 0.5 then 'lotte' when rand() < 0.75 then 'winmart' else 'mm_mega' end").alias("retailer"),
            col("timestamp").alias("timestamp"),
            (expr("rand() * 500000")).alias("old_price"),
            expr("rand() * 500000").alias("new_price"),
            expr("concat('Product ', cast(rand() * 100 as int))").alias("product_name"),
            expr("case when rand() < 0.125 then 'Beverages' when rand() < 0.25 then 'Dairy' when rand() < 0.375 then 'Bakery' when rand() < 0.5 then 'Meat' when rand() < 0.625 then 'Seafood' when rand() < 0.75 then 'Fruits' when rand() < 0.875 then 'Vegetables' else 'Snacks' end").alias("category"),
            expr("case when rand() < 0.167 then 'kg' when rand() < 0.334 then 'g' when rand() < 0.5 then 'l' when rand() < 0.667 then 'ml' when rand() < 0.834 then 'pack' else 'unit' end").alias("unit"),
            expr("case when rand() < 0.2 then 'HoChiMinh' when rand() < 0.4 then 'Hanoi' when rand() < 0.6 then 'DaNang' when rand() < 0.8 then 'CanTho' else 'NhaTrang' end").alias("location"),
            expr("rand() < 0.3").alias("is_promotion")
        )

print("Sample data generator created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Streaming Data
# MAGIC Process the streaming data with price change detection

# COMMAND ----------

# Start streaming data generator
price_change_stream = create_data_stream()

# Add processing_time and calculate price_change_pct
processed_stream = price_change_stream \
    .withColumn("processing_time", current_timestamp()) \
    .withColumn("price_change_pct", expr("(new_price - old_price) / old_price")) \
    .withColumn("price_change_abs", expr("abs(new_price - old_price)")) \
    .withColumn("is_significant_change", 
                expr(f"abs(price_change_pct) >= {config['notifications']['price_change_threshold']}"))

print("Stream processing started")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure Checkpoints and Storage Directories Exist
# MAGIC Create necessary directories for checkpoints and Delta Lake storage

# COMMAND ----------

# Ensure checkpoint and storage directories exist
def ensure_directory_exists(path):
    """Create directory if it doesn't exist"""
    try:
        dbutils.fs.mkdirs(path)
        print(f"Ensured directory exists: {path}")
        return True
    except Exception as e:
        print(f"Warning: Could not create directory {path}: {str(e)}")
        return False

# Get paths from config or use defaults
silver_path = config.get('delta', {}).get('silver_layer_path')
if silver_path is None:
    silver_path = "dbfs:/mnt/silver"
    print(f"Warning: Silver layer path not found in config, using default: {silver_path}")

gold_path = config.get('delta', {}).get('gold_layer_path')
if gold_path is None:
    gold_path = "dbfs:/mnt/gold"
    print(f"Warning: Gold layer path not found in config, using default: {gold_path}")

checkpoint_base = config.get('processing', {}).get('checkpoint_location')
if checkpoint_base is None:
    checkpoint_base = "dbfs:/mnt/checkpoints/streaming"
    print(f"Warning: Checkpoint location not found in config, using default: {checkpoint_base}")

# Create directories
ensure_directory_exists(silver_path)
ensure_directory_exists(gold_path)
ensure_directory_exists(checkpoint_base)

# Create checkpoint directories for each stream
silver_checkpoint_location = f"{checkpoint_base}/silver"
gold_checkpoint_location = f"{checkpoint_base}/gold"
notification_checkpoint_location = f"{checkpoint_base}/notifications"
window_checkpoint_location = f"{checkpoint_base}/windows"

ensure_directory_exists(silver_checkpoint_location)
ensure_directory_exists(gold_checkpoint_location)
ensure_directory_exists(notification_checkpoint_location)
ensure_directory_exists(window_checkpoint_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Lake Silver Layer
# MAGIC Persist the processed stream to Delta Lake Silver layer

# COMMAND ----------

# Write raw events to Silver layer
silver_output_path = f"{silver_path}/price_changes"

try:
    # Write stream to Silver layer with error handling
    silver_query = processed_stream \
        .writeStream \
        .format("delta") \
        .option("checkpointLocation", silver_checkpoint_location) \
        .option("mergeSchema", "true") \
        .outputMode("append") \
        .partitionBy("retailer", "is_significant_change") \
        .trigger(processingTime=config['processing']['trigger_interval']) \
        .start(silver_output_path)

    print(f"Stream writing to Silver layer at {silver_output_path}")
    print(f"Using checkpoint location: {silver_checkpoint_location}")
except Exception as e:
    print(f"Error setting up Silver layer stream: {str(e)}")
    print("Attempting to write with default settings...")
    
    try:
        # Simplify settings and try again
        silver_query = processed_stream \
            .writeStream \
            .format("delta") \
            .option("checkpointLocation", "dbfs:/mnt/checkpoints/silver_fallback") \
            .outputMode("append") \
            .trigger(processingTime="10 seconds") \
            .start("dbfs:/mnt/silver/price_changes_fallback")
            
        print("Fallback Silver layer stream started successfully")
    except Exception as e2:
        print(f"Failed to start Silver layer stream even with fallback settings: {str(e2)}")
        print("Silver layer stream will not be available")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor Significant Price Changes
# MAGIC Detect and alert on significant price changes

# COMMAND ----------

# Filter for significant price changes
significant_changes = processed_stream \
    .filter("is_significant_change = true") \
    .withColumn("price_change_direction", 
                expr("case when price_change_pct > 0 then 'increase' else 'decrease' end")) \
    .withColumn("notification_message", 
                expr("concat('Significant price ', price_change_direction, ' detected for ', product_name, ' at ', retailer, ': ', " +
                     "round(price_change_pct * 100, 2), '% (', old_price, ' -> ', new_price, ') in ', location)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Price Trend Analysis with Windows
# MAGIC Analyze price trends over sliding windows

# COMMAND ----------

# Analyze price trends over windows
window_analysis = processed_stream \
    .withWatermark("timestamp", "1 hour") \
    .groupBy(
        window("timestamp", "15 minutes", "5 minutes"),
        "retailer", 
        "category"
    ) \
    .agg(
        expr("count(*)").alias("price_change_count"),
        expr("avg(price_change_pct)").alias("avg_price_change_pct"),
        expr("sum(case when price_change_pct < 0 then 1 else 0 end)").alias("price_drop_count"),
        expr("sum(case when price_change_pct > 0 then 1 else 0 end)").alias("price_increase_count")
    ) \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .withColumn("is_competitive_window", 
                expr("price_drop_count > price_increase_count"))

# Write window analysis to console for demo
try:
    window_query = window_analysis \
        .writeStream \
        .format("console") \
        .outputMode("update") \
        .option("truncate", "false") \
        .option("numRows", 10) \
        .trigger(processingTime=config['processing']['trigger_interval']) \
        .start()

    print("Window analysis console output started")
except Exception as e:
    print(f"Error starting window analysis console output: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Notifications
# MAGIC Create notifications for significant price changes

# COMMAND ----------

# Send notifications for significant price changes
def send_notification(batch_df, batch_id):
    """Process each batch of significant price changes"""
    
    batch_count = batch_df.count()
    if batch_count > 0:
        print(f"Batch ID: {batch_id} - Processing {batch_count} notifications")
        
        # In a real implementation, this would send to a notification service
        # For demo, just print to console
        try:
            notifications = batch_df.select("notification_message").collect()
            
            print("=" * 80)
            print(f"PRICE CHANGE ALERTS - {datetime.now()}")
            print("=" * 80)
            for notification in notifications:
                print(f"ALERT: {notification[0]}")
            print("=" * 80)
        except Exception as e:
            print(f"Error processing notifications in batch {batch_id}: {str(e)}")
    
    return

# Write notifications using foreachBatch with error handling
try:
    notification_query = significant_changes \
        .writeStream \
        .foreachBatch(send_notification) \
        .outputMode("update") \
        .trigger(processingTime=config['processing']['trigger_interval']) \
        .start()

    print("Notification processor started")
except Exception as e:
    print(f"Error starting notification processor: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Aggregated Data to Gold Layer
# MAGIC Persist aggregated analytics to the Gold layer

# COMMAND ----------

# Write aggregated data to Gold layer
gold_output_path = f"{gold_path}/price_trends"

try:
    # Write window analysis to Gold layer with error handling
    gold_query = window_analysis \
        .writeStream \
        .format("delta") \
        .option("checkpointLocation", gold_checkpoint_location) \
        .option("mergeSchema", "true") \
        .outputMode("append") \
        .partitionBy("retailer", "is_competitive_window") \
        .trigger(processingTime=config['processing']['trigger_interval']) \
        .start(gold_output_path)

    print(f"Stream writing to Gold layer at {gold_output_path}")
    print(f"Using checkpoint location: {gold_checkpoint_location}")
except Exception as e:
    print(f"Error setting up Gold layer stream: {str(e)}")
    print("Attempting to write with default settings...")
    
    try:
        # Simplify settings and try again
        gold_query = window_analysis \
            .writeStream \
            .format("delta") \
            .option("checkpointLocation", "dbfs:/mnt/checkpoints/gold_fallback") \
            .outputMode("append") \
            .trigger(processingTime="10 seconds") \
            .start("dbfs:/mnt/gold/price_trends_fallback")
            
        print("Fallback Gold layer stream started successfully")
    except Exception as e2:
        print(f"Failed to start Gold layer stream even with fallback settings: {str(e2)}")
        print("Gold layer stream will not be available")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store Significant Changes
# MAGIC Save significant price changes to a separate Delta table

# COMMAND ----------

# Save significant price changes to a separate table
significant_changes_path = f"{gold_path}/significant_price_changes"
significant_checkpoint_location = f"{checkpoint_base}/significant_changes"

try:
    # Ensure checkpoint directory exists
    ensure_directory_exists(significant_checkpoint_location)
    
    # Write significant changes to Delta
    significant_query = significant_changes \
        .withColumn("detected_at", current_timestamp()) \
        .writeStream \
        .format("delta") \
        .option("checkpointLocation", significant_checkpoint_location) \
        .option("mergeSchema", "true") \
        .outputMode("append") \
        .partitionBy("retailer", "price_change_direction") \
        .trigger(processingTime=config['processing']['trigger_interval']) \
        .start(significant_changes_path)
    
    print(f"Significant price changes writing to {significant_changes_path}")
except Exception as e:
    print(f"Error starting significant changes stream: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait for Termination
# MAGIC Keep the streaming queries running

# COMMAND ----------

# Display active streams
active_streams = spark.streams.active
print(f"Number of active streams: {len(active_streams)}")

# Summary of stream destinations
print("\nStream Destinations:")
try:
    if 'silver_query' in locals():
        print(f"Silver layer: {silver_output_path}")
    if 'gold_query' in locals():
        print(f"Gold layer: {gold_output_path}")
    if 'significant_query' in locals():
        print(f"Significant changes: {significant_changes_path}")
    print(f"\nAll streams will continue running until manually stopped.")
    print(f"Data is being written to:")
    print(f"Silver: {silver_path}")
    print(f"Gold: {gold_path}")
    print(f"Checkpoints: {checkpoint_base}")
except Exception as e:
    print(f"Error displaying stream summary: {str(e)}")

# Keep all streams running for demonstration
print("\nStreams are running. Use 'spark.streams.active' to check status.")
print("To stop streams, use: for query in spark.streams.active: query.stop()")

# For demo purposes: Wait for streams to process data before stopping
# spark.streams.awaitAnyTermination(timeoutMs=60000)  # Wait for 1 minute
