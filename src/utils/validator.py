import re
from typing import Any, Dict, List, Optional
from datetime import datetime
from utils.logger import get_logger

logger = get_logger(__name__)

class DataValidator:
    """Class for validating data from scrapers."""
    
    @staticmethod
    def validate_price(price: Any) -> bool:
        """
        Validate price data.
        
        Args:
            price: Price value to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        try:
            if price is None:
                return False
                
            # Convert to float if string
            if isinstance(price, str):
                # Remove currency symbols and commas
                price = re.sub(r'[^\d.]', '', price)
                price = float(price)
            elif isinstance(price, (int, float)):
                price = float(price)
            else:
                return False
                
            # Check if price is positive
            return price > 0
        except (ValueError, TypeError):
            return False
    
    @staticmethod
    def validate_product_id(product_id: Any) -> bool:
        """
        Validate product ID.
        
        Args:
            product_id: Product ID to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        if not product_id:
            return False
            
        # Check if product_id is string or number
        if isinstance(product_id, (str, int)):
            # Convert to string and check length
            return len(str(product_id)) > 0
        return False
    
    @staticmethod
    def validate_product_name(name: Any) -> bool:
        """
        Validate product name.
        
        Args:
            name: Product name to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        if not name:
            return False
            
        # Check if name is string and not empty
        if isinstance(name, str):
            return len(name.strip()) > 0
        return False
    
    @staticmethod
    def validate_category(category: Any) -> bool:
        """
        Validate category.
        
        Args:
            category: Category to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        if not category:
            return False
            
        # Check if category is string and not empty
        if isinstance(category, str):
            return len(category.strip()) > 0
        return False
    
    @staticmethod
    def validate_promotion(promotion: Any) -> bool:
        """
        Validate promotion data.
        
        Args:
            promotion: Promotion data to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        if not promotion:
            return False
            
        # Check if promotion is string and not empty
        if isinstance(promotion, str):
            return len(promotion.strip()) > 0
        return False
    
    @staticmethod
    def validate_timestamp(timestamp: Any) -> bool:
        """
        Validate timestamp.
        
        Args:
            timestamp: Timestamp to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        if not timestamp:
            return False
            
        try:
            # Try parsing as datetime
            if isinstance(timestamp, str):
                datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            elif isinstance(timestamp, (int, float)):
                datetime.fromtimestamp(timestamp)
            else:
                return False
            return True
        except (ValueError, TypeError):
            return False
    
    @staticmethod
    def validate_product_data(data: Dict[str, Any]) -> bool:
        """
        Validate complete product data.
        
        Args:
            data: Product data dictionary to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        required_fields = ['product_id', 'name', 'price', 'category']
        
        # Check all required fields exist
        if not all(field in data for field in required_fields):
            logger.warning(f"Missing required fields in product data: {data}")
            return False
            
        # Validate each field
        if not DataValidator.validate_product_id(data['product_id']):
            logger.warning(f"Invalid product_id: {data['product_id']}")
            return False
            
        if not DataValidator.validate_product_name(data['name']):
            logger.warning(f"Invalid product name: {data['name']}")
            return False
            
        if not DataValidator.validate_price(data['price']):
            logger.warning(f"Invalid price: {data['price']}")
            return False
            
        if not DataValidator.validate_category(data['category']):
            logger.warning(f"Invalid category: {data['category']}")
            return False
            
        # Validate optional fields if present
        if 'promotion' in data and not DataValidator.validate_promotion(data['promotion']):
            logger.warning(f"Invalid promotion: {data['promotion']}")
            return False
            
        if 'timestamp' in data and not DataValidator.validate_timestamp(data['timestamp']):
            logger.warning(f"Invalid timestamp: {data['timestamp']}")
            return False
            
        return True
    
    @staticmethod
    def validate_product_list(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate list of product data.
        
        Args:
            products: List of product data dictionaries to validate
            
        Returns:
            List[Dict[str, Any]]: List of valid product data
        """
        valid_products = []
        for product in products:
            if DataValidator.validate_product_data(product):
                valid_products.append(product)
            else:
                logger.warning(f"Skipping invalid product data: {product}")
        return valid_products 