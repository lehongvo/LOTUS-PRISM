"""
Product categorization module for LOTUS-PRISM.

This module handles the categorization of products based on their attributes
and maps them to standardized categories for cross-competitor analysis.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, udf, lit, lower, when, array_contains, array, expr
from pyspark.sql.types import StringType, ArrayType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.pipeline import Pipeline
from pyspark.ml import PipelineModel
import yaml
import os
import re
from typing import Dict, List, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ProductCategorizer:
    """
    Product categorization for the LOTUS-PRISM batch processing pipeline.
    Handles mapping products to standardized categories based on attributes.
    """
    
    def __init__(self, spark: SparkSession, config_path: str = "../config/batch_config.yaml"):
        """
        Initialize the ProductCategorizer with Spark session and configuration.
        
        Args:
            spark: Spark session
            config_path: Path to the batch configuration file
        """
        self.spark = spark
        self.config = self._load_config(config_path)
        self.categorization_config = self.config.get("product_categorization", {})
        self.categories_master_df = self._load_categories_master()
        
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
                "product_categorization": {
                    "categories_master_path": "abfss://data@lotusprismstorage.dfs.core.windows.net/reference/categories_master",
                    "match_threshold": 0.8
                }
            }
    
    def _load_categories_master(self) -> DataFrame:
        """
        Load categories master data from the specified path.
        
        Returns:
            DataFrame with categories and their attributes
        """
        try:
            categories_path = self.categorization_config.get(
                "categories_master_path", 
                "abfss://data@lotusprismstorage.dfs.core.windows.net/reference/categories_master"
            )
            
            # Load categories master reference table
            categories_df = self.spark.read.format("delta").load(categories_path)
            logger.info(f"Loaded categories master reference with {categories_df.count()} records")
            return categories_df
        except Exception as e:
            logger.error(f"Error loading categories master reference: {str(e)}")
            
            # Create a fallback categories dataframe
            data = [
                ("Beverages", ["coffee", "tea", "juice", "soda", "water", "drink"]),
                ("Dairy", ["milk", "cheese", "yogurt", "butter", "cream"]),
                ("Bakery", ["bread", "cake", "pastry", "cookie", "biscuit"]),
                ("Meat", ["beef", "pork", "chicken", "lamb", "sausage"]),
                ("Seafood", ["fish", "shrimp", "crab", "squid", "mussel"]),
                ("Fruits", ["apple", "banana", "orange", "grape", "berry"]),
                ("Vegetables", ["carrot", "potato", "tomato", "onion", "cucumber"]),
                ("Snacks", ["chip", "crisp", "popcorn", "pretzel", "nut"]),
                ("Condiments", ["sauce", "mayonnaise", "ketchup", "mustard", "dressing"]),
                ("Cleaning", ["detergent", "soap", "cleaner", "bleach"]),
            ]
            schema = ["category", "keywords"]
            
            # Create fallback dataframe
            fallback_df = self.spark.createDataFrame(data, schema=schema)
            logger.warning("Using fallback categories master reference")
            return fallback_df
    
    def categorize_by_keywords(self, df: DataFrame, 
                            product_name_column: str = "product_name", 
                            description_column: str = None) -> DataFrame:
        """
        Categorize products by matching keywords in product names and descriptions.
        
        Args:
            df: Input DataFrame with product data
            product_name_column: Column name for the product name
            description_column: Column name for the product description (optional)
            
        Returns:
            DataFrame with added category columns
        """
        if product_name_column not in df.columns:
            error_msg = f"Required column '{product_name_column}' not found in DataFrame."
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Convert product names to lowercase for better matching
        df = df.withColumn("product_name_lower", lower(col(product_name_column)))
        
        # Also include description if available
        if description_column and description_column in df.columns:
            df = df.withColumn("description_lower", lower(col(description_column)))
        
        # Collect categories and keywords for broadcasting
        categories_data = [(row["category"], row["keywords"]) for row in self.categories_master_df.collect()]
        
        # UDF to match keywords
        @udf(returnType=StringType())
        def match_category(product_name, description=None):
            combined_text = product_name
            if description:
                combined_text += " " + description
                
            combined_text = combined_text.lower()
            
            best_match = None
            max_matches = 0
            
            for category, keywords in categories_data:
                matches = sum(1 for keyword in keywords if keyword in combined_text)
                if matches > max_matches:
                    max_matches = matches
                    best_match = category
            
            return best_match
        
        # Apply categorization
        if description_column and description_column in df.columns:
            df = df.withColumn("category", match_category(col("product_name_lower"), col("description_lower")))
        else:
            df = df.withColumn("category", match_category(col("product_name_lower")))
        
        # Drop temporary columns
        df = df.drop("product_name_lower")
        if description_column and description_column in df.columns:
            df = df.drop("description_lower")
        
        return df
    
    def categorize_by_hierarchy(self, df: DataFrame,
                             category_path_column: str = "category_path") -> DataFrame:
        """
        Normalize category paths from different sources into standardized categories.
        
        Args:
            df: Input DataFrame with product data
            category_path_column: Column name containing hierarchical category path
            
        Returns:
            DataFrame with standardized category columns
        """
        if category_path_column not in df.columns:
            logger.warning(f"Column '{category_path_column}' not found in DataFrame. Skipping hierarchy categorization.")
            return df
        
        # Extract hierarchical levels if the category path is a delimited string
        df = df.withColumn("category_l1", expr(f"split({category_path_column}, '[/>\\\\_]')[0]"))
        df = df.withColumn("category_l2", 
                         when(
                             expr(f"size(split({category_path_column}, '[/>\\\\_]'))") > 1,
                             expr(f"split({category_path_column}, '[/>\\\\_]')[1]")
                         ).otherwise(None))
        
        df = df.withColumn("category_l3", 
                         when(
                             expr(f"size(split({category_path_column}, '[/>\\\\_]'))") > 2,
                             expr(f"split({category_path_column}, '[/>\\\\_]')[2]")
                         ).otherwise(None))
        
        # Create a mapping of source categories to standard categories
        # This would typically come from a reference table
        # For demonstration, we'll use a hard-coded mapping
        l1_mapping = {
            "Beverages": "Beverages",
            "Drinks": "Beverages",
            "Dairy": "Dairy & Eggs",
            "Dairy Products": "Dairy & Eggs",
            "Bakery": "Bakery",
            "Baked Goods": "Bakery",
            "Meat": "Meat & Seafood",
            "Seafood": "Meat & Seafood",
            "Fish": "Meat & Seafood",
            "Fruits": "Produce",
            "Vegetables": "Produce",
            "Produce": "Produce",
            "Snacks": "Snacks",
            "Condiments": "Condiments & Sauces",
            "Sauces": "Condiments & Sauces",
            "Cleaning": "Household",
            "Household": "Household"
        }
        
        # Convert mapping to Spark map expression
        mapping_expr = expr(f"""map(
            {', '.join([f"'{k}', '{v}'" for k, v in l1_mapping.items()])}
        )""")
        
        # Apply mapping
        df = df.withColumn(
            "standard_category", 
            when(
                mapping_expr[lower(col("category_l1"))].isNotNull(),
                mapping_expr[lower(col("category_l1"))]
            ).otherwise(col("category_l1"))
        )
        
        return df
    
    def categorize_by_ml(self, df: DataFrame, 
                      product_name_column: str = "product_name", 
                      description_column: str = None,
                      model_path: str = None) -> DataFrame:
        """
        Categorize products using machine learning model.
        
        Args:
            df: Input DataFrame with product data
            product_name_column: Column name for the product name
            description_column: Column name for the product description (optional)
            model_path: Path to saved ML model (if None, creates a new model)
            
        Returns:
            DataFrame with predicted category
        """
        if product_name_column not in df.columns:
            error_msg = f"Required column '{product_name_column}' not found in DataFrame."
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Create text field for classification based on available columns
        if description_column and description_column in df.columns:
            df = df.withColumn(
                "text_features", 
                expr(f"concat({product_name_column}, ' ', coalesce({description_column}, ''))")
            )
        else:
            df = df.withColumn("text_features", col(product_name_column))
        
        # Load or create ML model
        ml_model = self._load_or_create_ml_model(model_path)
        
        # Apply model to predict categories
        df_with_predictions = ml_model.transform(df)
        
        # Select only needed columns
        result_columns = df.columns + ["predicted_category"]
        result_df = df_with_predictions.select([
            *[col(c) for c in df.columns],
            col("prediction").cast(StringType()).alias("predicted_category")
        ])
        
        return result_df
    
    def _load_or_create_ml_model(self, model_path: str = None) -> PipelineModel:
        """
        Load existing ML model or create a new one if not available.
        
        Args:
            model_path: Path to saved ML model
            
        Returns:
            PipelineModel for product categorization
        """
        if model_path and os.path.exists(model_path):
            try:
                model = PipelineModel.load(model_path)
                logger.info(f"Loaded ML model from {model_path}")
                return model
            except Exception as e:
                logger.error(f"Error loading ML model: {str(e)}")
        
        logger.info("Creating new ML pipeline for product categorization")
        
        # Create ML pipeline for text classification
        tokenizer = Tokenizer(inputCol="text_features", outputCol="words")
        remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
        cv = CountVectorizer(inputCol="filtered_words", outputCol="word_counts")
        idf = IDF(inputCol="word_counts", outputCol="features")
        classifier = RandomForestClassifier(labelCol="category_id", featuresCol="features", numTrees=10)
        
        pipeline = Pipeline(stages=[tokenizer, remover, cv, idf, classifier])
        
        # Note: In a real implementation, we would train this model on labeled data
        # For this demo, we're creating a dummy model that won't actually be used
        # We would return the pipeline here, not a fitted model
        
        # Create a dummy model that just assigns a default category
        @udf(returnType=StringType())
        def default_category(text):
            return "Uncategorized"
        
        # This is just a placeholder - in practice we would return a real ML model
        # trained on labeled data
        return pipeline
    
    def categorize_by_fuzzy_matching(self, df: DataFrame, 
                                 product_name_column: str = "product_name", 
                                 source_category_column: str = "source_category",
                                 target_categories: List[str] = None) -> DataFrame:
        """
        Categorize products using fuzzy matching against known categories.
        
        Args:
            df: Input DataFrame with product data
            product_name_column: Column name for the product name
            source_category_column: Column name for the source's own category
            target_categories: List of target categories to match against
            
        Returns:
            DataFrame with added fuzzy matched category
        """
        if product_name_column not in df.columns:
            error_msg = f"Required column '{product_name_column}' not found in DataFrame."
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        if target_categories is None:
            # Extract unique categories from master reference
            target_categories = [row["category"] for row in self.categories_master_df.select("category").distinct().collect()]
        
        # Create UDF for fuzzy matching
        @udf(returnType=StringType())
        def fuzzy_match_category(product_name, source_category=None):
            product_name = product_name.lower() if product_name else ""
            source_category = source_category.lower() if source_category else ""
            
            combined_text = f"{product_name} {source_category}"
            
            # Simple word-based matching (in production, use a proper fuzzy matching library)
            best_match = None
            best_score = 0
            
            for category in target_categories:
                category_lower = category.lower()
                
                # Calculate simple similarity score
                # Would use proper Levenshtein distance or similar in production
                score = 0
                category_words = set(category_lower.split())
                input_words = set(combined_text.split())
                
                # Count matching words
                common_words = category_words.intersection(input_words)
                if category_words:
                    score = len(common_words) / len(category_words)
                
                if score > best_score:
                    best_score = score
                    best_match = category
            
            # Check if score exceeds threshold
            threshold = self.categorization_config.get("match_threshold", 0.8)
            if best_score >= threshold:
                return best_match
            else:
                return "Other"
        
        # Apply fuzzy matching
        if source_category_column in df.columns:
            df = df.withColumn("fuzzy_category", fuzzy_match_category(col(product_name_column), col(source_category_column)))
        else:
            df = df.withColumn("fuzzy_category", fuzzy_match_category(col(product_name_column)))
        
        return df
    
    def categorize_products(self, df: DataFrame, 
                         product_name_column: str = "product_name",
                         description_column: str = None,
                         category_path_column: str = None,
                         source_category_column: str = None) -> DataFrame:
        """
        Apply complete product categorization workflow.
        
        Args:
            df: Input DataFrame with product data
            product_name_column: Column name for the product name
            description_column: Column name for the product description
            category_path_column: Column name for hierarchical category path
            source_category_column: Column name for source's own category
            
        Returns:
            DataFrame with added category columns
        """
        logger.info(f"Starting product categorization for {df.count()} records")
        
        # Step 1: Categorize by keywords
        result_df = self.categorize_by_keywords(df, product_name_column, description_column)
        logger.info("Keyword-based categorization completed")
        
        # Step 2: Categorize by hierarchy if available
        if category_path_column and category_path_column in df.columns:
            result_df = self.categorize_by_hierarchy(result_df, category_path_column)
            logger.info("Hierarchy-based categorization completed")
        
        # Step 3: Apply fuzzy matching
        result_df = self.categorize_by_fuzzy_matching(
            result_df, product_name_column, source_category_column
        )
        logger.info("Fuzzy matching categorization completed")
        
        # Consolidate categories into a final category
        result_df = result_df.withColumn(
            "final_category",
            when(col("category").isNotNull(), col("category"))
            .when(col("standard_category").isNotNull(), col("standard_category"))
            .when(col("fuzzy_category").isNotNull() & (col("fuzzy_category") != "Other"), col("fuzzy_category"))
            .when(col("predicted_category").isNotNull(), col("predicted_category"))
            .otherwise("Uncategorized")
        )
        
        logger.info("Product categorization process completed successfully")
        return result_df


# Example usage (for documentation)
if __name__ == "__main__":
    # This would be used in a Spark job
    spark = SparkSession.builder.appName("ProductCategorization").getOrCreate()
    
    # Create categorizer
    categorizer = ProductCategorizer(spark)
    
    # Example usage (commented out)
    # df = spark.read.format("delta").load("abfss://data@lotusprismstorage.dfs.core.windows.net/bronze/competitor_products")
    # categorized_df = categorizer.categorize_products(df)
