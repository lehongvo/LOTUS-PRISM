"""
Spark Streaming job for LOTUS-PRISM.
This pipeline performs the following steps:
1. Connect to Event Hub
2. Read streaming data
3. Process price changes
4. Send notifications if there are significant changes
5. Write to Delta Lake
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when
from streaming.notification import send_notification


def main():
    # Initialize SparkSession
    spark = SparkSession.builder.appName("LotusStreaming").getOrCreate()
    
    # 1. Connect to Event Hub
    connection_str = 'YOUR_EVENT_HUB_CONNECTION_STRING'
    eventhub_name = 'YOUR_EVENT_HUB_NAME'
    
    # 2. Read streaming data
    df = spark.readStream.format('eventhubs').options(
        connectionString=connection_str,
        eventHubName=eventhub_name
    ).load()
    
    # 3. Process price changes
    df_processed = df.withColumn('price', col('body').cast('double'))
    df_processed = df_processed.withColumn('prev_price', lag('price').over())
    df_processed = df_processed.withColumn('price_change', 
                                          when(col('prev_price').isNotNull(), 
                                               (col('price') - col('prev_price')) / col('prev_price'))
                                          .otherwise(0))
    
    # 4. Send notifications if there are significant changes
    df_processed = df_processed.filter(col('price_change') > 0.1)
    df_processed.foreachBatch(lambda batch_df, batch_id: send_notification(batch_df))
    
    # 5. Write to Delta Lake
    output_path = 'abfss://silver@<storage_account>.dfs.core.windows.net/products/aeon/streaming/'
    query = df_processed.writeStream.format('delta').outputMode('append').start(output_path)
    query.awaitTermination()


if __name__ == "__main__":
    main()
