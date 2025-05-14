import logging
import random
import time
from typing import Dict, List, Any, Optional
from abc import ABC, abstractmethod
import requests

from utils.logger import get_logger
from utils.retry import retry_with_backoff
from utils.storage import AzureDataLakeStorage
from utils.validator import DataValidator
from settings import (
    SCRAPER_TIMEOUT,
    SCRAPER_RETRY_COUNT,
    SCRAPER_RETRY_DELAY,
    RETAILER_CONFIG
)


class BaseScraper(ABC):
    """Base class for all scrapers."""
    
    def __init__(self, retailer_name: str, config: Dict = None):
        self.retailer_name = retailer_name
        self.logger = get_logger(f"{retailer_name}_scraper")
        self.proxy_manager = None  # Will be initialized by child class
        self.user_agent_rotator = None  # Will be initialized by child class
        self.config = config or RETAILER_CONFIG.get(retailer_name, {})
        self.storage = AzureDataLakeStorage()
        self.validator = DataValidator()
        
    @abstractmethod
    def init_session(self) -> requests.Session:
        """Initialize a new session with appropriate headers and proxy."""
        pass
        
    @abstractmethod
    def scrape_products(self, category: str = None) -> List[Dict]:
        """Scrape products from the retailer's website."""
        pass
        
    @abstractmethod
    def scrape_prices(self, product_ids: List[str] = None) -> List[Dict]:
        """Scrape prices for products."""
        pass
        
    @abstractmethod
    def scrape_promotions(self) -> List[Dict]:
        """Scrape promotions from the retailer's website."""
        pass
        
    @abstractmethod
    def scrape_market_trends(self) -> Dict[str, Any]:
        """Scrape market trends data."""
        pass
        
    def get_next_proxy(self) -> Optional[str]:
        """Get next proxy from the proxy manager."""
        if self.proxy_manager:
            return self.proxy_manager.get_next_proxy()
        return None
        
    def get_next_user_agent(self) -> str:
        """Get next user agent from the user agent rotator."""
        if self.user_agent_rotator:
            return self.user_agent_rotator.get_next_user_agent()
        return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        
    def throttle_request(self):
        """Add random delay between requests to avoid detection."""
        delay = random.uniform(1, 3)
        time.sleep(delay)
        
    @retry_with_backoff(max_retries=SCRAPER_RETRY_COUNT, backoff_factor=SCRAPER_RETRY_DELAY)
    def request_url(self, url: str, session=None, **kwargs) -> requests.Response:
        """Make request with retry logic and throttling."""
        self.throttle_request()
        session = session or self.init_session()
        timeout = self.config.get('timeout', SCRAPER_TIMEOUT)
        return session.get(url, timeout=timeout, **kwargs)
        
    def save_data(self, data: Dict, data_type: str):
        """Save scraped data to Azure Data Lake Storage."""
        if self.validator.validate_data(data, data_type):
            filename = f"{self.retailer_name}/{data_type}/{int(time.time())}.json"
            self.storage.save_data(data, filename)
            self.logger.info(f"Saved {data_type} data to {filename}")
        else:
            self.logger.error(f"Invalid {data_type} data, skipping save")
            
    def run_scraping_job(self):
        """Run a complete scraping job."""
        result = {
            "products": 0,
            "prices": 0,
            "promotions": 0,
            "market_trends": 0
        }
        
        try:
            # Scrape products
            self.logger.info("Starting product scraping...")
            products = self.scrape_products()
            self.save_data(products, "products")
            result["products"] = len(products) if products else 0
            
            # Scrape prices
            self.logger.info("Starting price scraping...")
            prices = self.scrape_prices()
            self.save_data(prices, "prices")
            result["prices"] = len(prices) if prices else 0
            
            # Scrape promotions
            self.logger.info("Starting promotion scraping...")
            promotions = self.scrape_promotions()
            self.save_data(promotions, "promotions")
            result["promotions"] = len(promotions) if promotions else 0
            
            # Scrape market trends
            self.logger.info("Starting market trends scraping...")
            market_trends = self.scrape_market_trends()
            self.save_data(market_trends, "market_trends")
            result["market_trends"] = len(market_trends.get("trends", [])) if market_trends else 0
            
            self.logger.info(f"Completed full scraping job for {self.retailer_name}. Results: {result}")
        except Exception as e:
            self.logger.error(f"Error during scraping job: {e}")
            raise 