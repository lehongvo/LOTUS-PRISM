from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class Promotion:
    """Class representing a promotion."""
    
    product_id: str
    promotion_text: str
    start_date: datetime
    end_date: Optional[datetime] = None
    discount_percentage: Optional[float] = None
    discount_amount: Optional[float] = None
    
    def to_dict(self) -> dict:
        """
        Convert promotion to dictionary.
        
        Returns:
            dict: Dictionary representation of promotion
        """
        return {
            'product_id': self.product_id,
            'promotion_text': self.promotion_text,
            'start_date': self.start_date.isoformat(),
            'end_date': self.end_date.isoformat() if self.end_date else None,
            'discount_percentage': self.discount_percentage,
            'discount_amount': self.discount_amount
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Promotion':
        """
        Create promotion from dictionary.
        
        Args:
            data: Dictionary containing promotion data
            
        Returns:
            Promotion: Promotion instance
        """
        # Convert date strings to datetime
        start_date = datetime.fromisoformat(data['start_date'].replace('Z', '+00:00'))
        end_date = None
        if data.get('end_date'):
            end_date = datetime.fromisoformat(data['end_date'].replace('Z', '+00:00'))
        
        return cls(
            product_id=str(data['product_id']),
            promotion_text=str(data['promotion_text']),
            start_date=start_date,
            end_date=end_date,
            discount_percentage=data.get('discount_percentage'),
            discount_amount=data.get('discount_amount')
        ) 