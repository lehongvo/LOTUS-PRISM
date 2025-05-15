"""
RFM (Recency, Frequency, Monetary) analysis module for LOTUS-PRISM.

This module performs RFM analysis to segment customers based on their
purchase behavior - how recently they purchased (Recency), how often they
purchase (Frequency), and how much they spend (Monetary).
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    col, datediff, current_date, count, sum as spark_sum, 
    desc, ntile, when, expr, lit, round as spark_round
)
import yaml
import os
from typing import Dict, List, Tuple, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RFMAnalyzer:
    """
    RFM (Recency, Frequency, Monetary) analysis for customer segmentation.
    """
    
    def __init__(self, spark: SparkSession, config_path: str = "../config/batch_config.yaml"):
        """
        Initialize the RFMAnalyzer with Spark session and configuration.
        
        Args:
            spark: Spark session
            config_path: Path to the batch configuration file
        """
        self.spark = spark
        self.config = self._load_config(config_path)
        self.rfm_config = self.config.get("rfm_analysis", {})
        
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
                "rfm_analysis": {
                    "recency_weight": 0.35,
                    "frequency_weight": 0.35,
                    "monetary_weight": 0.3,
                    "lookback_days": 365,
                    "segments": [
                        {"name": "Champions", "min_score": 4.5, "max_score": 5.0},
                        {"name": "Loyal Customers", "min_score": 3.5, "max_score": 4.5},
                        {"name": "Potential Loyalists", "min_score": 3.0, "max_score": 3.5},
                        {"name": "At Risk Customers", "min_score": 2.0, "max_score": 3.0},
                        {"name": "Hibernating", "min_score": 1.0, "max_score": 2.0}
                    ]
                }
            }
    
    def calculate_rfm_metrics(self, df: DataFrame, 
                           customer_id_column: str = "customer_id",
                           purchase_date_column: str = "purchase_date",
                           purchase_amount_column: str = "purchase_amount") -> DataFrame:
        """
        Calculate RFM metrics for each customer.
        
        Args:
            df: Input DataFrame with purchase transaction data
            customer_id_column: Column name for customer ID
            purchase_date_column: Column name for purchase date
            purchase_amount_column: Column name for purchase amount
            
        Returns:
            DataFrame with RFM metrics for each customer
        """
        # Validate required columns
        required_columns = [customer_id_column, purchase_date_column, purchase_amount_column]
        for col_name in required_columns:
            if col_name not in df.columns:
                error_msg = f"Required column '{col_name}' not found in DataFrame."
                logger.error(error_msg)
                raise ValueError(error_msg)
        
        # Filter transactions for lookback period
        lookback_days = self.rfm_config.get("lookback_days", 365)
        df_filtered = df.filter(
            datediff(current_date(), col(purchase_date_column)) <= lookback_days
        )
        
        # Calculate Recency - days since last purchase (lower is better)
        recency_df = df_filtered.groupBy(customer_id_column) \
            .agg(expr(f"min(datediff(current_date(), {purchase_date_column}))").alias("recency_days"))
        
        # Calculate Frequency - number of purchases
        frequency_df = df_filtered.groupBy(customer_id_column) \
            .agg(count("*").alias("frequency_count"))
        
        # Calculate Monetary - total amount spent
        monetary_df = df_filtered.groupBy(customer_id_column) \
            .agg(spark_sum(purchase_amount_column).alias("monetary_value"))
        
        # Join the metrics together
        rfm_df = recency_df.join(frequency_df, customer_id_column) \
            .join(monetary_df, customer_id_column)
        
        # Calculate quartiles for each metric using ntile
        window_spec = Window.orderBy(desc("recency_days"))
        rfm_df = rfm_df.withColumn("recency_score", 6 - ntile(5).over(window_spec))
        
        window_spec = Window.orderBy("frequency_count")
        rfm_df = rfm_df.withColumn("frequency_score", ntile(5).over(window_spec))
        
        window_spec = Window.orderBy("monetary_value")
        rfm_df = rfm_df.withColumn("monetary_score", ntile(5).over(window_spec))
        
        return rfm_df
    
    def calculate_rfm_score(self, rfm_df: DataFrame) -> DataFrame:
        """
        Calculate overall RFM score based on weighted average of individual metrics.
        
        Args:
            rfm_df: DataFrame with RFM metrics
            
        Returns:
            DataFrame with overall RFM score and segment
        """
        # Get weights from config
        recency_weight = self.rfm_config.get("recency_weight", 0.35)
        frequency_weight = self.rfm_config.get("frequency_weight", 0.35)
        monetary_weight = self.rfm_config.get("monetary_weight", 0.3)
        
        # Calculate weighted RFM score
        rfm_score_df = rfm_df.withColumn(
            "rfm_score",
            spark_round(
                (col("recency_score") * recency_weight) +
                (col("frequency_score") * frequency_weight) +
                (col("monetary_score") * monetary_weight),
                2
            )
        )
        
        # Load segment definitions from config
        segments = self.rfm_config.get("segments", [
            {"name": "Champions", "min_score": 4.5, "max_score": 5.0},
            {"name": "Loyal Customers", "min_score": 3.5, "max_score": 4.5},
            {"name": "Potential Loyalists", "min_score": 3.0, "max_score": 3.5},
            {"name": "At Risk Customers", "min_score": 2.0, "max_score": 3.0},
            {"name": "Hibernating", "min_score": 1.0, "max_score": 2.0}
        ])
        
        # Create segment column using when-otherwise chain
        segment_expr = None
        for segment in segments:
            name = segment["name"]
            min_score = segment["min_score"]
            max_score = segment["max_score"]
            
            if segment_expr is None:
                segment_expr = when(
                    (col("rfm_score") >= min_score) & (col("rfm_score") < max_score),
                    lit(name)
                )
            else:
                segment_expr = segment_expr.when(
                    (col("rfm_score") >= min_score) & (col("rfm_score") < max_score),
                    lit(name)
                )
        
        # Add default segment for scores that don't match
        segment_expr = segment_expr.otherwise(lit("Other"))
        
        # Apply segment expression
        rfm_score_df = rfm_score_df.withColumn("customer_segment", segment_expr)
        
        return rfm_score_df
    
    def analyze_segments(self, rfm_score_df: DataFrame) -> DataFrame:
        """
        Generate aggregate metrics for each customer segment.
        
        Args:
            rfm_score_df: DataFrame with RFM scores and segments
            
        Returns:
            DataFrame with segment analysis
        """
        # Group by segment and calculate aggregate metrics
        segment_analysis = rfm_score_df.groupBy("customer_segment") \
            .agg(
                count("*").alias("customer_count"),
                spark_round(spark_sum("monetary_value"), 2).alias("total_revenue"),
                spark_round(spark_sum("monetary_value") / count("*"), 2).alias("avg_revenue_per_customer"),
                spark_round(spark_sum("frequency_count"), 0).alias("total_purchases"),
                spark_round(spark_sum("frequency_count") / count("*"), 2).alias("avg_purchases_per_customer"),
                spark_round(spark_sum("recency_days") / count("*"), 1).alias("avg_days_since_last_purchase")
            )
        
        # Calculate percentages
        total_customers = rfm_score_df.count()
        total_revenue = rfm_score_df.agg(spark_sum("monetary_value")).collect()[0][0]
        
        segment_analysis = segment_analysis.withColumn(
            "pct_of_customers",
            spark_round(col("customer_count") / lit(total_customers) * 100, 2)
        ).withColumn(
            "pct_of_revenue",
            spark_round(col("total_revenue") / lit(total_revenue) * 100, 2)
        )
        
        # Sort by revenue contribution
        segment_analysis = segment_analysis.orderBy(desc("total_revenue"))
        
        return segment_analysis
    
    def get_segment_recommendations(self, rfm_score_df: DataFrame) -> Dict[str, List[Dict]]:
        """
        Generate marketing recommendations for each customer segment.
        
        Args:
            rfm_score_df: DataFrame with RFM scores and segments
            
        Returns:
            Dictionary with recommendations by segment
        """
        # Define recommendations for each segment
        # These would typically come from a more sophisticated recommendation system
        # or be configured by business users
        recommendations = {
            "Champions": [
                {"action": "Reward program", "description": "Send exclusive offers and rewards"},
                {"action": "Loyalty program", "description": "Upgrade to top-tier loyalty status"},
                {"action": "Referral program", "description": "Incentivize to refer friends and family"}
            ],
            "Loyal Customers": [
                {"action": "Up-sell higher value products", "description": "Introduce premium product lines"},
                {"action": "Cross-sell related products", "description": "Recommend complementary products"},
                {"action": "Feedback program", "description": "Request product feedback and reviews"}
            ],
            "Potential Loyalists": [
                {"action": "Membership programs", "description": "Offer membership benefits"},
                {"action": "Special discounts", "description": "Send limited time offers"},
                {"action": "Educational content", "description": "Share product usage tips and tutorials"}
            ],
            "At Risk Customers": [
                {"action": "Reactivation campaign", "description": "Send personalized win-back offers"},
                {"action": "Feedback survey", "description": "Ask for feedback on potential issues"},
                {"action": "Special service", "description": "Offer special customer service attention"}
            ],
            "Hibernating": [
                {"action": "Reconnect campaign", "description": "Send we-miss-you promotions"},
                {"action": "Major discount", "description": "Offer significant discount to incentivize purchase"},
                {"action": "Alternative products", "description": "Suggest alternative products that might better suit needs"}
            ],
            "Other": [
                {"action": "Basic engagement", "description": "Send standard marketing communications"},
                {"action": "Profile completion", "description": "Request additional profile information"},
                {"action": "Survey", "description": "Ask for preferences to better target future communications"}
            ]
        }
        
        # Get actual segments in the data
        actual_segments = [row["customer_segment"] for row in rfm_score_df.select("customer_segment").distinct().collect()]
        
        # Filter recommendations to only include segments that exist in the data
        filtered_recommendations = {segment: recommendations.get(segment, recommendations["Other"]) 
                                   for segment in actual_segments}
        
        return filtered_recommendations
    
    def perform_rfm_analysis(self, df: DataFrame,
                          customer_id_column: str = "customer_id",
                          purchase_date_column: str = "purchase_date",
                          purchase_amount_column: str = "purchase_amount",
                          output_details: bool = True) -> Tuple[DataFrame, DataFrame, Optional[Dict]]:
        """
        Perform complete RFM analysis workflow.
        
        Args:
            df: Input DataFrame with purchase transaction data
            customer_id_column: Column name for customer ID
            purchase_date_column: Column name for purchase date
            purchase_amount_column: Column name for purchase amount
            output_details: Whether to output detailed analysis and recommendations
            
        Returns:
            Tuple of (customer_rfm_df, segment_analysis_df, recommendations)
        """
        logger.info(f"Starting RFM analysis for {df.count()} transactions")
        
        # Step 1: Calculate RFM metrics
        rfm_metrics_df = self.calculate_rfm_metrics(
            df, customer_id_column, purchase_date_column, purchase_amount_column
        )
        logger.info("RFM metrics calculation completed")
        
        # Step 2: Calculate RFM score and segment
        rfm_score_df = self.calculate_rfm_score(rfm_metrics_df)
        logger.info("RFM scoring and segmentation completed")
        
        # Step 3: Generate segment analysis if requested
        segment_analysis_df = None
        recommendations = None
        
        if output_details:
            segment_analysis_df = self.analyze_segments(rfm_score_df)
            logger.info("Segment analysis completed")
            
            recommendations = self.get_segment_recommendations(rfm_score_df)
            logger.info("Recommendation generation completed")
        
        logger.info("RFM analysis process completed successfully")
        return rfm_score_df, segment_analysis_df, recommendations
    
    def enrich_customers_with_rfm(self, customer_df: DataFrame, 
                               transaction_df: DataFrame,
                               customer_id_column: str = "customer_id") -> DataFrame:
        """
        Enrich customer data with RFM scores and segments.
        
        Args:
            customer_df: DataFrame with customer data
            transaction_df: DataFrame with transaction data
            customer_id_column: Column name for customer ID
            
        Returns:
            DataFrame with customer data enriched with RFM information
        """
        # Perform RFM analysis
        rfm_score_df, _, _ = self.perform_rfm_analysis(
            transaction_df,
            customer_id_column=customer_id_column,
            output_details=False
        )
        
        # Join RFM data with customer data
        enriched_df = customer_df.join(
            rfm_score_df.select(
                customer_id_column, 
                "recency_days", "recency_score",
                "frequency_count", "frequency_score",
                "monetary_value", "monetary_score",
                "rfm_score", "customer_segment"
            ),
            customer_id_column,
            "left"
        )
        
        return enriched_df


# Example usage (for documentation)
if __name__ == "__main__":
    # This would be used in a Spark job
    spark = SparkSession.builder.appName("RFMAnalysis").getOrCreate()
    
    # Create analyzer
    analyzer = RFMAnalyzer(spark)
    
    # Example usage (commented out)
    # transaction_df = spark.read.format("delta").load("abfss://data@lotusprismstorage.dfs.core.windows.net/silver/transactions")
    # rfm_df, segment_analysis, recommendations = analyzer.perform_rfm_analysis(transaction_df)
