from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class Product:
    """Class representing a product."""
    
    product_id: str
    name: str
    price: float
    category: str
    promotion: Optional[str] = None
    timestamp: Optional[datetime] = None
    
    def to_dict(self) -> dict:
        """
        Convert product to dictionary.
        
        Returns:
            dict: Dictionary representation of product
        """
        return {
            'product_id': self.product_id,
            'name': self.name,
            'price': self.price,
            'category': self.category,
            'promotion': self.promotion,
            'timestamp': self.timestamp.isoformat() if self.timestamp else None
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Product':
        """
        Create product from dictionary.
        
        Args:
            data: Dictionary containing product data
            
        Returns:
            Product: Product instance
        """
        # Convert timestamp string to datetime if present
        timestamp = None
        if data.get('timestamp'):
            try:
                timestamp = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
            except (ValueError, TypeError):
                pass
        
        return cls(
            product_id=str(data['product_id']),
            name=str(data['name']),
            price=float(data['price']),
            category=str(data['category']),
            promotion=data.get('promotion'),
            timestamp=timestamp
        ) 