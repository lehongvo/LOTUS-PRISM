from scrapers.aeon_scraper import AeonScraper
from utils.storage import AzureDataLakeStorage
import logging
import json
from datetime import datetime
import os

def setup_logging():
    """Setup logging configuration"""
    os.makedirs('logs', exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/aeon_scraper.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def default_converter(o):
    if isinstance(o, datetime):
        return o.isoformat()
    raise TypeError(f'Object of type {o.__class__.__name__} is not JSON serializable')

def save_to_local(data, filename):
    """Save data to local file"""
    os.makedirs('data/aeon', exist_ok=True)
    filepath = f'data/aeon/{filename}'
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=default_converter)
    return filepath

def product_to_dict(p):
    d = p.__dict__.copy()
    if hasattr(p, "timestamp") and isinstance(p.timestamp, datetime):
        d["timestamp"] = p.timestamp.isoformat()
    return d

def main():
    logger = setup_logging()
    storage = AzureDataLakeStorage()
    
    try:
        # Khởi tạo AEON scraper
        logger.info("Initializing AEON scraper...")
        aeon_scraper = AeonScraper()
        
        # Scrape products
        logger.info("Scraping products...")
        products = aeon_scraper.scrape_products()
        logger.info(f"Scraped {len(products)} products")
        
        # Convert products to dict for storage
        products_data = [product_to_dict(p) for p in products]
        
        # Save to local
        local_file = save_to_local(products_data, f'products_{datetime.now().strftime("%Y%m%d")}.json')
        logger.info(f"Saved products to {local_file}")
        
        # Upload to Azure
        azure_path = f'products/aeon/{datetime.now().strftime("%Y-%m-%d")}/products.json'
        if storage.upload_file(local_file, azure_path):
            logger.info(f"Uploaded products to Azure: {azure_path}")
        
        # Scrape promotions
        logger.info("Scraping promotions...")
        promotions = aeon_scraper.scrape_promotions()
        logger.info(f"Scraped {len(promotions)} promotions")
        
        # Convert promotions to dict for storage
        promotions_data = [p.__dict__ for p in promotions]
        
        # Save to local
        local_file = save_to_local(promotions_data, f'promotions_{datetime.now().strftime("%Y%m%d")}.json')
        logger.info(f"Saved promotions to {local_file}")
        
        # Upload to Azure
        azure_path = f'promotions/aeon/{datetime.now().strftime("%Y-%m-%d")}/promotions.json'
        if storage.upload_file(local_file, azure_path):
            logger.info(f"Uploaded promotions to Azure: {azure_path}")
        
        logger.info("AEON scraping completed successfully!")
        
    except Exception as e:
        logger.error(f"Error running AEON scraper: {str(e)}")
        raise

if __name__ == "__main__":
    main() 