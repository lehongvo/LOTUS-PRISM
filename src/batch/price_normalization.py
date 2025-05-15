"""
Price normalization module for LOTUS-PRISM.

This module handles price normalization across different products, units,
and competitors to enable fair comparison.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, lit, regexp_extract, create_map, round as spark_round
from pyspark.sql.types import FloatType, StringType
import yaml
import os
from typing import Dict, List, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PriceNormalizer:
    """
    Normalizes product prices for fair comparison across competitors.
    Handles different units, currencies, and promotional pricing.
    """
    
    def __init__(self, spark: SparkSession, config_path: str = "../config/batch_config.yaml"):
        """
        Initialize the PriceNormalizer with Spark session and configuration.
        
        Args:
            spark: Spark session
            config_path: Path to the batch configuration file
        """
        self.spark = spark
        self.config = self._load_config(config_path)
        self.normalization_config = self.config.get("price_normalization", {})
        self.unit_conversion_df = self._load_unit_conversion_reference()
        
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
                "price_normalization": {
                    "unit_conversion_reference": "abfss://data@lotusprismstorage.dfs.core.windows.net/reference/unit_conversion",
                    "currency": "VND",
                    "vat_rate": 0.1
                }
            }
    
    def _load_unit_conversion_reference(self) -> DataFrame:
        """
        Load unit conversion reference data from the specified path.
        
        Returns:
            DataFrame with unit conversion factors
        """
        try:
            unit_conversion_path = self.normalization_config.get(
                "unit_conversion_reference", 
                "abfss://data@lotusprismstorage.dfs.core.windows.net/reference/unit_conversion"
            )
            
            # Load unit conversion reference table
            unit_df = self.spark.read.format("delta").load(unit_conversion_path)
            logger.info(f"Loaded unit conversion reference with {unit_df.count()} records")
            return unit_df
        except Exception as e:
            logger.error(f"Error loading unit conversion reference: {str(e)}")
            
            # Create a fallback conversion dataframe with basic conversions
            data = [
                ("kg", "g", 1000.0),
                ("g", "kg", 0.001),
                ("l", "ml", 1000.0),
                ("ml", "l", 0.001),
                ("pack", "unit", 1.0),
                ("unit", "pack", 1.0),
                ("dozen", "unit", 12.0),
                ("unit", "dozen", 1/12.0),
            ]
            schema = ["from_unit", "to_unit", "conversion_factor"]
            
            # Create fallback dataframe
            fallback_df = self.spark.createDataFrame(data, schema=schema)
            logger.warning("Using fallback unit conversion reference")
            return fallback_df
    
    def normalize_units(self, df: DataFrame, 
                      unit_column: str = "unit", 
                      quantity_column: str = "quantity", 
                      price_column: str = "price", 
                      target_unit: str = "kg") -> DataFrame:
        """
        Normalize prices to a standard unit (e.g., price per kg, price per unit).
        
        Args:
            df: Input DataFrame with prices
            unit_column: Column name for the unit
            quantity_column: Column name for the quantity
            price_column: Column name for the price
            target_unit: Target unit to normalize to
            
        Returns:
            DataFrame with normalized prices
        """
        # Make sure required columns exist
        required_columns = [unit_column, price_column]
        if quantity_column not in df.columns:
            df = df.withColumn(quantity_column, lit(1.0))
            
        for col_name in required_columns:
            if col_name not in df.columns:
                error_msg = f"Required column '{col_name}' not found in DataFrame."
                logger.error(error_msg)
                raise ValueError(error_msg)
        
        # Create a broadcast variable for the conversion mapping
        conversion_factors = self.unit_conversion_df.filter(col("to_unit") == target_unit)
        
        # Join with the conversion factors
        normalized_df = df.join(
            conversion_factors,
            df[unit_column] == conversion_factors["from_unit"],
            "left"
        )
        
        # Apply conversion - if no conversion factor is found, keep original unit and add a flag
        normalized_df = normalized_df.withColumn(
            "conversion_factor", 
            when(col("conversion_factor").isNull(), lit(1.0)).otherwise(col("conversion_factor"))
        )
        
        # Calculate normalized price
        normalized_df = normalized_df.withColumn(
            "normalized_price", 
            col(price_column) / (col(quantity_column) * col("conversion_factor"))
        )
        
        # Add flag for records that couldn't be normalized
        normalized_df = normalized_df.withColumn(
            "is_normalized", 
            when(col("from_unit").isNull(), lit(False)).otherwise(lit(True))
        )
        
        # Select original columns plus new normalized price and flag
        result_columns = df.columns + ["normalized_price", "is_normalized", "target_unit"]
        result_df = normalized_df.select(
            *[col(c) for c in df.columns],
            col("normalized_price"),
            col("is_normalized"),
            lit(target_unit).alias("target_unit")
        )
        
        return result_df
    
    def remove_vat(self, df: DataFrame, price_column: str = "price", vat_inclusive: bool = True) -> DataFrame:
        """
        Remove VAT from prices if they are VAT inclusive.
        
        Args:
            df: Input DataFrame with prices
            price_column: Column name for the price
            vat_inclusive: Whether the prices include VAT
            
        Returns:
            DataFrame with prices excluding VAT
        """
        if not vat_inclusive:
            return df.withColumn("price_no_vat", col(price_column))
        
        vat_rate = self.normalization_config.get("vat_rate", 0.1)  # Default 10% VAT
        
        # Remove VAT from price
        # Formula: price_no_vat = price / (1 + vat_rate)
        df_no_vat = df.withColumn(
            "price_no_vat", 
            spark_round(col(price_column) / (1 + lit(vat_rate)), 2)
        )
        
        return df_no_vat
    
    def apply_currency_conversion(self, df: DataFrame, 
                              price_column: str = "price", 
                              currency_column: str = "currency", 
                              target_currency: str = None) -> DataFrame:
        """
        Convert prices to a target currency.
        
        Args:
            df: Input DataFrame with prices
            price_column: Column name for the price
            currency_column: Column name for the currency
            target_currency: Target currency (defaults to config value)
            
        Returns:
            DataFrame with prices in target currency
        """
        if target_currency is None:
            target_currency = self.normalization_config.get("currency", "VND")
            
        if currency_column not in df.columns:
            # If no currency column, assume all prices are already in target currency
            return df.withColumn("converted_price", col(price_column)).withColumn("target_currency", lit(target_currency))
        
        # For demonstration, we'll use hardcoded conversion rates
        # In a real implementation, we would fetch current exchange rates from a reference table
        conversion_rates = {
            "USD_to_VND": 23000,
            "EUR_to_VND": 26000,
            "JPY_to_VND": 155,
            "VND_to_VND": 1
        }
        
        # Create conversion map
        conversion_expr = create_map([
            lit(f"{curr}_to_{target_currency}"), lit(rate) 
            for curr, rate in conversion_rates.items() 
            if curr != target_currency
        ])
        
        # Apply conversion
        df_converted = df.withColumn(
            "conversion_key", 
            when(
                col(currency_column) == target_currency,
                lit(f"{target_currency}_to_{target_currency}")
            ).otherwise(
                concat(col(currency_column), lit("_to_"), lit(target_currency))
            )
        )
        
        df_converted = df_converted.withColumn(
            "conversion_rate",
            when(
                col("conversion_key") == f"{target_currency}_to_{target_currency}", 
                lit(1.0)
            ).when(
                col("conversion_key").isin(list(conversion_rates.keys())),
                conversion_expr.getItem(col("conversion_key"))
            ).otherwise(lit(1.0))
        )
        
        df_converted = df_converted.withColumn(
            "converted_price", 
            spark_round(col(price_column) * col("conversion_rate"), 2)
        ).withColumn(
            "target_currency", 
            lit(target_currency)
        )
        
        # Drop temporary columns
        result_df = df_converted.drop("conversion_key", "conversion_rate")
        
        return result_df
    
    def normalize_promotion_prices(self, df: DataFrame, 
                              regular_price_column: str = "regular_price", 
                              promo_price_column: str = "promo_price", 
                              is_promo_column: str = "is_promotion") -> DataFrame:
        """
        Normalize prices considering promotions.
        
        Args:
            df: Input DataFrame with prices
            regular_price_column: Column name for the regular price
            promo_price_column: Column name for the promotional price
            is_promo_column: Column name indicating if the product is on promotion
            
        Returns:
            DataFrame with a normalized effective price
        """
        # Check if promotion columns exist
        has_promo_column = promo_price_column in df.columns
        has_is_promo = is_promo_column in df.columns
        
        if not has_promo_column and not has_is_promo:
            # No promotion information, use regular price as effective price
            return df.withColumn("effective_price", col(regular_price_column))
        
        # If we have promotion price but no flag, create the flag
        if has_promo_column and not has_is_promo:
            df = df.withColumn(
                is_promo_column, 
                when(col(promo_price_column).isNotNull() & (col(promo_price_column) > 0), lit(True))
                .otherwise(lit(False))
            )
            has_is_promo = True
        
        # If we have flag but no promotion price, estimate it
        if has_is_promo and not has_promo_column:
            # Assuming a default 10% discount for promotions if no specific promotion price
            df = df.withColumn(
                promo_price_column, 
                when(col(is_promo_column) == True, col(regular_price_column) * 0.9)
                .otherwise(col(regular_price_column))
            )
            has_promo_column = True
        
        # Calculate effective price
        df_effective = df.withColumn(
            "effective_price",
            when(
                col(is_promo_column) == True, 
                col(promo_price_column)
            ).otherwise(
                col(regular_price_column)
            )
        )
        
        # Calculate discount percentage
        if has_promo_column and has_is_promo:
            df_effective = df_effective.withColumn(
                "discount_pct",
                when(
                    col(is_promo_column) == True,
                    spark_round((col(regular_price_column) - col(promo_price_column)) / col(regular_price_column) * 100, 1)
                ).otherwise(lit(0.0))
            )
        
        return df_effective
    
    def standardize_package_sizes(self, df: DataFrame, 
                             product_name_column: str = "product_name", 
                             size_column: str = "package_size", 
                             unit_column: str = "unit",
                             price_column: str = "price") -> DataFrame:
        """
        Extract and standardize package sizes from product names if not provided.
        
        Args:
            df: Input DataFrame with product data
            product_name_column: Column name for the product name
            size_column: Column name for the package size
            unit_column: Column name for the unit
            price_column: Column name for the price
            
        Returns:
            DataFrame with standardized package sizes and units
        """
        # If size and unit columns already exist with valid data, return as is
        if size_column in df.columns and unit_column in df.columns:
            # Check if most records have values
            size_null_count = df.filter(col(size_column).isNull()).count()
            unit_null_count = df.filter(col(unit_column).isNull()).count()
            
            if size_null_count / df.count() < 0.1 and unit_null_count / df.count() < 0.1:
                logger.info("Package size and unit columns already have valid data")
                return df
        
        # Create size and unit columns if they don't exist
        if size_column not in df.columns:
            df = df.withColumn(size_column, lit(None).cast(FloatType()))
            
        if unit_column not in df.columns:
            df = df.withColumn(unit_column, lit(None).cast(StringType()))
        
        # Extract size and unit from product name using regex patterns
        # Pattern for common formats: "500g", "1.5kg", "2 liters", "750ml", "6 x 355ml", etc.
        
        # Extract patterns like "500g", "1.5kg", "2 liters", "750ml", etc.
        df = df.withColumn(
            "extracted_size",
            regexp_extract(col(product_name_column), r"(\d+(?:\.\d+)?)\s*(g|kg|ml|l|liter|litre|oz|lb|pack|unit|piece|ct|count)", 1)
        )
        
        df = df.withColumn(
            "extracted_unit",
            regexp_extract(col(product_name_column), r"\d+(?:\.\d+)?\s*(g|kg|ml|l|liter|litre|oz|lb|pack|unit|piece|ct|count)", 1)
        )
        
        # Process multipack format like "6 x 355ml"
        df = df.withColumn(
            "multipack_count",
            regexp_extract(col(product_name_column), r"(\d+)\s*x\s*\d+(?:\.\d+)?\s*(g|kg|ml|l|liter|litre|oz|lb)", 1)
        )
        
        df = df.withColumn(
            "multipack_size",
            regexp_extract(col(product_name_column), r"\d+\s*x\s*(\d+(?:\.\d+)?)\s*(g|kg|ml|l|liter|litre|oz|lb)", 1)
        )
        
        df = df.withColumn(
            "multipack_unit",
            regexp_extract(col(product_name_column), r"\d+\s*x\s*\d+(?:\.\d+)?\s*(g|kg|ml|l|liter|litre|oz|lb)", 1)
        )
        
        # Compute final size and unit
        df = df.withColumn(
            size_column,
            when(
                col("multipack_count").isNotNull() & col("multipack_size").isNotNull(),
                col("multipack_count").cast(FloatType()) * col("multipack_size").cast(FloatType())
            ).when(
                col("extracted_size").isNotNull(),
                col("extracted_size").cast(FloatType())
            ).otherwise(
                col(size_column)
            )
        )
        
        df = df.withColumn(
            unit_column,
            when(
                col("multipack_unit").isNotNull(),
                col("multipack_unit")
            ).when(
                col("extracted_unit").isNotNull(),
                col("extracted_unit")
            ).otherwise(
                col(unit_column)
            )
        )
        
        # Standardize unit names
        unit_mapping = {
            "g": "g",
            "gram": "g",
            "grams": "g",
            "kg": "kg",
            "kilo": "kg",
            "kilos": "kg",
            "kilogram": "kg",
            "kilograms": "kg",
            "ml": "ml",
            "milliliter": "ml",
            "millilitre": "ml",
            "l": "l",
            "liter": "l",
            "litre": "l",
            "liters": "l",
            "litres": "l",
            "oz": "oz",
            "ounce": "oz",
            "ounces": "oz",
            "lb": "lb",
            "pound": "lb",
            "pounds": "lb",
            "pack": "pack",
            "package": "pack",
            "unit": "unit",
            "piece": "unit",
            "pc": "unit",
            "pcs": "unit",
            "ct": "unit",
            "count": "unit"
        }
        
        map_expr = create_map([lit(k), lit(v) for k, v in unit_mapping.items()])
        
        df = df.withColumn(
            unit_column,
            when(
                col(unit_column).isin(list(unit_mapping.keys())),
                map_expr.getItem(col(unit_column))
            ).otherwise(
                col(unit_column)
            )
        )
        
        # Calculate normalized price per unit (price per kg, price per liter, etc.)
        df = df.withColumn(
            "unit_price",
            when(
                col(size_column).isNotNull() & col(size_column) > 0,
                col(price_column) / col(size_column)
            ).otherwise(
                col(price_column)
            )
        )
        
        # Drop temporary columns
        result_df = df.drop("extracted_size", "extracted_unit", "multipack_count", "multipack_size", "multipack_unit")
        
        return result_df
    
    def normalize_prices(self, df: DataFrame, 
                      price_column: str = "price", 
                      unit_column: str = "unit",
                      quantity_column: str = "quantity",
                      currency_column: str = None,
                      product_name_column: str = "product_name",
                      target_unit: str = "kg",
                      target_currency: str = None,
                      vat_inclusive: bool = True) -> DataFrame:
        """
        Apply complete price normalization workflow.
        
        Args:
            df: Input DataFrame with prices
            price_column: Column name for the price
            unit_column: Column name for the unit
            quantity_column: Column name for the quantity
            currency_column: Column name for the currency (optional)
            product_name_column: Column name for the product name
            target_unit: Target unit to normalize to
            target_currency: Target currency (defaults to config value)
            vat_inclusive: Whether the prices include VAT
            
        Returns:
            DataFrame with normalized prices
        """
        logger.info(f"Starting price normalization process for {df.count()} records")
        
        # Step 1: Standardize package sizes
        result_df = self.standardize_package_sizes(
            df, product_name_column, "package_size", unit_column, price_column
        )
        logger.info("Package size standardization completed")
        
        # Step 2: Normalize to target unit
        result_df = self.normalize_units(
            result_df, unit_column, quantity_column, price_column, target_unit
        )
        logger.info(f"Unit normalization to {target_unit} completed")
        
        # Step 3: Apply currency conversion if needed
        if currency_column is not None:
            result_df = self.apply_currency_conversion(
                result_df, price_column, currency_column, target_currency
            )
            logger.info(f"Currency conversion to {target_currency} completed")
        
        # Step 4: Remove VAT if prices are VAT inclusive
        if vat_inclusive:
            result_df = self.remove_vat(result_df, price_column, vat_inclusive)
            logger.info("VAT removal completed")
        
        # Step 5: Handle promotion prices if applicable
        if "regular_price" in df.columns or "promo_price" in df.columns or "is_promotion" in df.columns:
            price_col_to_use = "converted_price" if "converted_price" in result_df.columns else price_column
            result_df = self.normalize_promotion_prices(
                result_df, 
                price_col_to_use, 
                "promo_price" if "promo_price" in result_df.columns else None,
                "is_promotion" if "is_promotion" in result_df.columns else None
            )
            logger.info("Promotion price normalization completed")
        
        logger.info("Price normalization process completed successfully")
        return result_df


# For documentation/example
if __name__ == "__main__":
    # This would be used in a Spark job
    spark = SparkSession.builder.appName("PriceNormalization").getOrCreate()
    
    # Create normalizer
    normalizer = PriceNormalizer(spark)
    
    # Example usage (commented out)
    # df = spark.read.format("delta").load("abfss://data@lotusprismstorage.dfs.core.windows.net/bronze/competitor_prices")
    # normalized_df = normalizer.normalize_prices(df)
