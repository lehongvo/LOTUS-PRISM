from scrapers.aeon_trending_scraper import AeonTrendingScraper, save_trending_products
from utils.storage import AzureDataLakeStorage
import logging
from datetime import datetime
import os


def setup_logging():
    os.makedirs('logs', exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/aeon_trending_scraper.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def main():
    logger = setup_logging()
    storage = AzureDataLakeStorage()
    try:
        logger.info("Initializing AEON trending scraper...")
        scraper = AeonTrendingScraper()
        logger.info("Scraping trending products...")
        products = scraper.scrape_all()
        logger.info(f"Scraped {len(products)} trending products")

        # Save to local
        today = datetime.now().strftime("%Y%m%d")
        local_file = f'data/aeon/market_trend_aeon_{today}.json'
        save_trending_products(products, local_file)
        logger.info(f"Saved trending products to {local_file}")

        # Upload to Azure
        azure_path = f'market_trend/aeon/{datetime.now().strftime("%Y-%m-%d")}/market_trend.json'
        if storage.upload_file(local_file, azure_path):
            logger.info(f"Uploaded trending products to Azure: {azure_path}")
        logger.info("AEON trending scraping completed successfully!")
    except Exception as e:
        logger.error(f"Error running AEON trending scraper: {str(e)}")
        raise

if __name__ == "__main__":
    main() 