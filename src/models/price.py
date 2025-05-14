from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class Price:
    """Class representing a price."""
    
    product_id: str
    price: float
    timestamp: datetime
    promotion: Optional[str] = None
    
    def to_dict(self) -> dict:
        """
        Convert price to dictionary.
        
        Returns:
            dict: Dictionary representation of price
        """
        return {
            'product_id': self.product_id,
            'price': self.price,
            'timestamp': self.timestamp.isoformat(),
            'promotion': self.promotion
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Price':
        """
        Create price from dictionary.
        
        Args:
            data: Dictionary containing price data
            
        Returns:
            Price: Price instance
        """
        # Convert timestamp string to datetime
        timestamp = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
        
        return cls(
            product_id=str(data['product_id']),
            price=float(data['price']),
            timestamp=timestamp,
            promotion=data.get('promotion')
        ) 