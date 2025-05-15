"""
Price change notification module for LOTUS-PRISM.
The send_notification() function takes an input DataFrame and sends notifications if there are significant price changes.
"""

from pyspark.sql.functions import col


def send_notification(df):
    """
    Send notifications if there are significant price changes.
    - Filter products with price change > 10%
    - Send notification for each product
    """
    df_alert = df.filter(col('price_change') > 0.1)
    for row in df_alert.collect():
        print(f"Notification: Product {row['product_id']} has a price change of {row['price_change'] * 100}%")
