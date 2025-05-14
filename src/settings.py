import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Azure Storage Configuration
AZURE_STORAGE_ACCOUNT_NAME = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
AZURE_STORAGE_ACCOUNT_KEY = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
AZURE_STORAGE_CONTAINER = os.getenv('AZURE_STORAGE_CONTAINER')

# Proxy Configuration
PROXY_API_URL = os.getenv('PROXY_API_URL')
PROXY_API_KEY = os.getenv('PROXY_API_KEY')
PROXY_CHECK_INTERVAL = int(os.getenv('PROXY_CHECK_INTERVAL', 3600))

# Scraper Configuration
SCRAPER_TIMEOUT = int(os.getenv('SCRAPER_TIMEOUT', 30))
SCRAPER_RETRY_COUNT = int(os.getenv('SCRAPER_RETRY_COUNT', 3))
SCRAPER_RETRY_DELAY = int(os.getenv('SCRAPER_RETRY_DELAY', 2))

# Logging Configuration
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FILE = os.getenv('LOG_FILE', 'scraper.log')

# Market Trends Configuration
MARKET_TRENDS_API_URL = os.getenv('MARKET_TRENDS_API_URL')
MARKET_TRENDS_API_KEY = os.getenv('MARKET_TRENDS_API_KEY')
MARKET_TRENDS_UPDATE_INTERVAL = int(os.getenv('MARKET_TRENDS_UPDATE_INTERVAL', 86400))

# API Configuration
API_BASE_URL = os.getenv('API_BASE_URL')
API_KEY = os.getenv('API_KEY')
API_TIMEOUT = int(os.getenv('API_TIMEOUT', 30))

# Data Configuration
DATA_DIR = os.getenv('DATA_DIR', '/data')
PRODUCTS_DIR = os.getenv('PRODUCTS_DIR', f"{DATA_DIR}/products")
PRICES_DIR = os.getenv('PRICES_DIR', f"{DATA_DIR}/prices")
PROMOTIONS_DIR = os.getenv('PROMOTIONS_DIR', f"{DATA_DIR}/promotions")
MARKET_TRENDS_DIR = os.getenv('MARKET_TRENDS_DIR', f"{DATA_DIR}/market_trends")

# Data Retention
DATA_RETENTION_DAYS = int(os.getenv('DATA_RETENTION_DAYS', 90))
BACKUP_DIR = os.getenv('BACKUP_DIR', f"{DATA_DIR}/backups")
ARCHIVE_DIR = os.getenv('ARCHIVE_DIR', f"{DATA_DIR}/archives")

# Data Processing
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 1000))
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 4))
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', 1024))

# Data Validation
MIN_PRICE = float(os.getenv('MIN_PRICE', 0))
MAX_PRICE = float(os.getenv('MAX_PRICE', 1000000000))
REQUIRED_FIELDS = os.getenv('REQUIRED_FIELDS', '["id", "name", "price", "retailer"]')
VALID_CATEGORIES = os.getenv('VALID_CATEGORIES', '["food", "beverage", "household", "personal_care"]')

# Data Export
EXPORT_FORMAT = os.getenv('EXPORT_FORMAT', 'json')
EXPORT_DIR = os.getenv('EXPORT_DIR', f"{DATA_DIR}/exports")
EXPORT_FREQUENCY = os.getenv('EXPORT_FREQUENCY', 'daily')
EXPORT_TIME = os.getenv('EXPORT_TIME', '00:00')

# Data Analysis
ANALYSIS_DIR = os.getenv('ANALYSIS_DIR', f"{DATA_DIR}/analysis")
REPORT_DIR = os.getenv('REPORT_DIR', f"{DATA_DIR}/reports")
CHART_DIR = os.getenv('CHART_DIR', f"{DATA_DIR}/charts")
STATS_DIR = os.getenv('STATS_DIR', f"{DATA_DIR}/stats")

# Data Quality
QUALITY_THRESHOLD = float(os.getenv('QUALITY_THRESHOLD', 0.95))
VALIDATION_RULES_DIR = os.getenv('VALIDATION_RULES_DIR', f"{DATA_DIR}/rules")
ERROR_LOG_DIR = os.getenv('ERROR_LOG_DIR', f"{DATA_DIR}/errors")
CORRECTION_DIR = os.getenv('CORRECTION_DIR', f"{DATA_DIR}/corrections")

# Retailer Configuration
RETAILER_CONFIG = {
    'aeon': {
        'base_url': 'https://aeoneshop.com',
        'categories_url': 'https://aeoneshop.com/categories',
        'timeout': SCRAPER_TIMEOUT,
        'retry_count': SCRAPER_RETRY_COUNT,
        'retry_delay': SCRAPER_RETRY_DELAY,
        'data_dir': f"{PRODUCTS_DIR}/aeon"
    },
    'lotte': {
        'base_url': 'https://www.lottemart.vn',
        'categories_url': 'https://www.lottemart.vn/categories',
        'timeout': SCRAPER_TIMEOUT,
        'retry_count': SCRAPER_RETRY_COUNT,
        'retry_delay': SCRAPER_RETRY_DELAY,
        'data_dir': f"{PRODUCTS_DIR}/lotte"
    },
    'mm_mega': {
        'base_url': 'https://mmmegamart.com.vn',
        'categories_url': 'https://mmmegamart.com.vn/categories',
        'timeout': SCRAPER_TIMEOUT,
        'retry_count': SCRAPER_RETRY_COUNT,
        'retry_delay': SCRAPER_RETRY_DELAY,
        'data_dir': f"{PRODUCTS_DIR}/mm_mega"
    }
} 