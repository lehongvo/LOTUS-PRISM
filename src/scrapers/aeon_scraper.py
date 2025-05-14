import requests
from typing import Dict, List, Any, Optional
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from scrapers.base import BaseScraper
from models.product import Product
from models.price import Price
from models.promotion import Promotion
from utils.logger import get_logger
from utils.retry import retry_with_backoff

logger = get_logger(__name__)

class AeonScraper(BaseScraper):
    """Scraper for AEON website."""
    
    def __init__(self, config: Dict = None):
        """Initialize AEON scraper."""
        super().__init__("aeon", config=config)
        self.base_url = "https://www.aeon.com.vn"
        self.products_url = f"{self.base_url}/san-pham"
        self.promotions_url = f"{self.base_url}/tet-2025"
        self.logger = logger
        
    def init_session(self) -> requests.Session:
        """Initialize a new session with appropriate headers and proxy."""
        session = requests.Session()
        session.headers.update({
            'User-Agent': self.get_next_user_agent(),
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml',
            'Referer': self.base_url
        })
        
        proxy = self.get_next_proxy()
        if proxy:
            session.proxies = {
                'http': proxy,
                'https': proxy
            }
            
        return session
        
    def get_all_category_links(self, session) -> List[str]:
        """Get all main category links from /san-pham."""
        response = self.request_url(self.products_url, session)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Lấy tất cả link danh mục từ menu
        category_links = []
        
        # Tìm tất cả các link trong menu danh mục
        menu_items = soup.select('.menu-item a[href*="/san-pham/"]')
        for item in menu_items:
            if item.has_attr('href'):
                href = item['href']
                if href.startswith('/'):
                    href = self.base_url + href
                category_links.append(href)
                
        # Thêm link chính /san-pham
        category_links.append(self.products_url)
        
        # Loại bỏ trùng lặp
        return list(set(category_links))

    def get_all_subcategory_links(self, category_url: str, session) -> List[str]:
        """Get all subcategory links from a main category page."""
        response = self.request_url(category_url, session)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        subcategory_links = []
        
        # Tìm tất cả link phụ danh mục
        sub_links = soup.select('a[href*="/san-pham/"]')
        for link in sub_links:
            if link.has_attr('href'):
                href = link['href']
                if href.startswith('/'):
                    href = self.base_url + href
                subcategory_links.append(href)
                
        # Thêm link chính
        subcategory_links.append(category_url)
        
        # Loại bỏ trùng lặp
        return list(set(subcategory_links))

    def get_total_pages(self, soup: BeautifulSoup) -> int:
        """Get total number of pages from pagination."""
        try:
            # Tìm phần tử phân trang
            pagination = soup.select('.pagination a')
            if not pagination:
                return 1
                
            # Lấy số trang lớn nhất
            page_numbers = []
            for page in pagination:
                try:
                    num = int(page.text.strip())
                    page_numbers.append(num)
                except ValueError:
                    continue
                    
            return max(page_numbers) if page_numbers else 1
        except Exception as e:
            self.logger.error(f"Error getting total pages: {e}")
            return 1

    def scrape_products_in_category(self, category_url: str, session) -> List[Product]:
        """Scrape all products in a given (sub)category using AEON's actual HTML structure."""
        products = []
        page = 1
        
        # Lấy trang đầu tiên để xác định tổng số trang
        response = self.request_url(category_url, session)
        if response.status_code != 200:
            return products
            
        soup = BeautifulSoup(response.text, 'html.parser')
        total_pages = self.get_total_pages(soup)
        
        self.logger.info(f"Found {total_pages} pages in category: {category_url}")
        
        # Duyệt qua từng trang
        while page <= total_pages:
            try:
                url = f"{category_url}?page={page}" if page > 1 else category_url
                self.logger.info(f"Scraping page {page}/{total_pages}: {url}")
                
                response = self.request_url(url, session)
                if response.status_code != 200:
                    break
                    
                soup = BeautifulSoup(response.text, 'html.parser')
                product_blocks = soup.select('.article-fud__col')
                
                if not product_blocks:
                    break
                    
                for block in product_blocks:
                    try:
                        # Ảnh sản phẩm
                        img_tag = block.select_one('.article-fud__col--img img')
                        image_url = img_tag['src'] if img_tag else ''
                        
                        # Tên sản phẩm
                        name_tag = block.select_one('.article-fud__col--title')
                        name = name_tag.text.strip() if name_tag else ''
                        
                        # Giá sản phẩm
                        price_tag = block.select_one('.article-fud__col--price')
                        price = price_tag.text.strip() if price_tag else ''
                        
                        # Link sản phẩm
                        link_tag = block.select_one('a')
                        product_url = link_tag['href'] if link_tag and link_tag.has_attr('href') else ''
                        if product_url.startswith('/'):
                            product_url = self.base_url + product_url
                            
                        # Tạo Product object
                        product = Product(
                            product_id=product_url or image_url,  # Ưu tiên dùng URL sản phẩm làm ID
                            name=name,
                            price=price,
                            category=category_url,
                            promotion=None,
                            timestamp=datetime.now()
                        )
                        products.append(product)
                        
                    except Exception as e:
                        self.logger.error(f"Error parsing product: {e}")
                        
                # Thêm delay giữa các request
                time.sleep(1)
                page += 1
                
            except Exception as e:
                self.logger.error(f"Error scraping page {page}: {e}")
                break
                
        return products

    @retry_with_backoff(max_retries=3)
    def scrape_products(self, category: str = None) -> List[Product]:
        """Scrape all products from all categories and subcategories in parallel."""
        self.logger.info("Scraping all products from AEON (parallel)...")
        session = self.init_session()
        all_products = []
        
        # Lấy tất cả danh mục chính
        main_cats = self.get_all_category_links(session)
        self.logger.info(f"Found {len(main_cats)} main categories")
        
        # Lấy tất cả phụ danh mục
        subcat_urls = []
        for cat_url in main_cats:
            subcats = self.get_all_subcategory_links(cat_url, session)
            subcat_urls.extend(subcats)
            self.logger.info(f"Found {len(subcats)} subcategories in {cat_url}")
            
        # Loại bỏ trùng lặp
        subcat_urls = list(set(subcat_urls))
        self.logger.info(f"Total unique categories to scrape: {len(subcat_urls)}")

        # Song song hóa việc scrape từng danh mục/phụ danh mục
        with ThreadPoolExecutor(max_workers=4) as executor:  # Giảm số worker để tránh quá tải
            future_to_url = {executor.submit(self.scrape_products_in_category, url, session): url for url in subcat_urls}
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    products = future.result()
                    all_products.extend(products)
                    self.logger.info(f"Scraped {len(products)} products from: {url}")
                except Exception as exc:
                    self.logger.error(f"Error scraping {url}: {exc}")
                    
        self.logger.info(f"Total products scraped: {len(all_products)}")
        return all_products
    
    def _get_categories(self, session) -> List[str]:
        """Get all available product categories from /san-pham."""
        response = self.request_url(self.products_url, session)
        soup = BeautifulSoup(response.text, 'html.parser')
        # Tìm các nút danh mục, ví dụ: <button>PHÒNG NGỦ</button>
        # Có thể thay đổi selector nếu HTML khác
        category_buttons = soup.find_all('button')
        categories = [btn.text.strip() for btn in category_buttons if btn.text.strip()]
        return categories
    
    def _get_product_details(self, session, product_url) -> Dict:
        """Get detailed product information."""
        full_url = self.base_url + product_url
        response = self.request_url(full_url, session)
        
        if response.status_code != 200:
            self.logger.error(f"Failed to fetch product details from {full_url}")
            return {}
            
        soup = BeautifulSoup(response.text, 'html.parser')
        details = {}
        
        try:
            details['description'] = soup.select_one('.product-description').text.strip()
        except:
            details['description'] = ''
            
        # Extract specifications
        specs = {}
        spec_rows = soup.select('.specifications tr')
        for row in spec_rows:
            try:
                key = row.select_one('th').text.strip()
                value = row.select_one('td').text.strip()
                specs[key] = value
            except:
                continue
                
        details['specifications'] = specs
        
        # Extract brand and sku
        details['brand'] = soup.select_one('.brand-name').text.strip() if soup.select_one('.brand-name') else ''
        details['sku'] = soup.select_one('.sku').text.strip() if soup.select_one('.sku') else ''
        
        return details
        
    @retry_with_backoff(max_retries=3)
    def scrape_prices(self, product_ids: List[str] = None) -> List[Price]:
        """
        Scrape price information for given products.
        
        Args:
            product_ids: List of product IDs to scrape prices for
            
        Returns:
            List[Price]: List of scraped prices
        """
        self.logger.info(f"Scraping prices for {len(product_ids) if product_ids else 'all products'}...")
        prices = []
        session = self.init_session()
        
        # If product_ids provided, use them; otherwise scrape all products
        if not product_ids:
            products = self.scrape_products()
            product_ids = [p.id for p in products]
            
        for product_id in product_ids:
            try:
                url = f"{self.base_url}/api/products/{product_id}/price"
                response = self.request_url(url, session)
                
                if response.status_code != 200:
                    self.logger.error(f"Failed to fetch price for product {product_id}")
                    continue
                    
                price_data = response.json()
                
                # Parse prices
                current_price = self._parse_price(str(price_data.get('current_price', 0)))
                original_price = self._parse_price(str(price_data.get('original_price', 0)))
                
                price = Price(
                    product_id=product_id,
                    retailer="aeon",
                    current_price=current_price,
                    original_price=original_price,
                    currency="VND",
                    discount_percentage=price_data.get('discount_percentage', 0),
                    timestamp=price_data.get('timestamp')
                )
                
                prices.append(price)
            except Exception as e:
                self.logger.error(f"Error scraping price for product {product_id}: {e}")
                
        return prices
        
    @retry_with_backoff(max_retries=3)
    def scrape_promotions(self) -> List[Promotion]:
        """
        Scrape promotion information from AEON website.
        
        Returns:
            List[Promotion]: List of scraped promotions
        """
        self.logger.info("Scraping promotions from AEON...")
        promotions = []
        session = self.init_session()
        
        try:
            url = f"{self.base_url}/promotions"
            response = self.request_url(url, session)
            
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch promotions, status: {response.status_code}")
                return promotions
                
            soup = BeautifulSoup(response.text, 'html.parser')
            promo_elements = soup.select('.promotion-card')
            
            for element in promo_elements:
                try:
                    promo_id = element.get('data-promotion-id')
                    title = element.select_one('.promotion-title').text.strip()
                    description = element.select_one('.promotion-desc').text.strip()
                    
                    # Parse dates
                    start_date = self._parse_date(element.select_one('.promotion-date .start').text.strip())
                    end_date = self._parse_date(element.select_one('.promotion-date .end').text.strip())
                    
                    image_url = element.select_one('img').get('src')
                    
                    # Get affected products if available
                    affected_products = []
                    product_elements = element.select('.promotion-products .product')
                    for product in product_elements:
                        affected_products.append(product.get('data-product-id'))
                    
                    # Determine promotion type
                    promotion_type = self._determine_promotion_type(title, description)
                    
                    # Extract discount information
                    discount_info = self._extract_discount_info(title, description)
                    
                    promotion = Promotion(
                        id=promo_id,
                        retailer="aeon",
                        title=title,
                        description=description,
                        start_date=start_date,
                        end_date=end_date,
                        image_url=image_url,
                        promotion_type=promotion_type,
                        affected_products=affected_products,
                        discount_value=discount_info.get('value'),
                        discount_percentage=discount_info.get('percentage'),
                        minimum_purchase=discount_info.get('minimum_purchase'),
                        maximum_discount=discount_info.get('maximum_discount')
                    )
                    
                    promotions.append(promotion)
                except Exception as e:
                    self.logger.error(f"Error processing promotion: {e}")
            
        except Exception as e:
            self.logger.error(f"Error scraping promotions: {e}")
            
        return promotions
    
    def _parse_date(self, date_str: str) -> str:
        """Parse date string to ISO format."""
        try:
            # Try different date formats
            formats = ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"]
            for fmt in formats:
                try:
                    date = datetime.strptime(date_str, fmt)
                    return date.isoformat()
                except ValueError:
                    continue
            return date_str
        except Exception as e:
            self.logger.error(f"Error parsing date {date_str}: {e}")
            return date_str
    
    def _parse_price(self, price_str: str) -> float:
        """Parse price string to float."""
        try:
            # Remove currency symbols and convert to float
            price = re.sub(r'[^\d.]', '', price_str)
            return float(price)
        except ValueError as e:
            self.logger.error(f"Error parsing price {price_str}: {e}")
            return 0.0
    
    def _determine_promotion_type(self, title: str, description: str) -> str:
        """Determine the type of promotion based on title and description."""
        text = (title + " " + description).lower()
        
        if any(word in text for word in ["giảm giá", "khuyến mãi", "sale"]):
            return "discount"
        elif any(word in text for word in ["combo", "bundle", "gói"]):
            return "bundle"
        elif any(word in text for word in ["quà tặng", "gift", "tặng"]):
            return "gift"
        elif any(word in text for word in ["flash sale", "sốc"]):
            return "flash_sale"
        elif any(word in text for word in ["thanh lý", "clearance"]):
            return "clearance"
        else:
            return "other"
    
    def _extract_discount_info(self, title: str, description: str) -> Dict[str, Any]:
        """Extract discount information from promotion text."""
        text = (title + " " + description).lower()
        info = {}
        
        # Extract percentage discount
        percentage_match = re.search(r'(\d+)%', text)
        if percentage_match:
            info['percentage'] = float(percentage_match.group(1))
            
        # Extract fixed discount value
        value_match = re.search(r'giảm\s+(\d+(?:\.\d+)?)\s*k', text)
        if value_match:
            info['value'] = self._parse_price(value_match.group(1)) * 1000  # Convert to VND
            
        # Extract minimum purchase
        min_match = re.search(r'từ\s+(\d+(?:\.\d+)?)\s*k', text)
        if min_match:
            info['minimum_purchase'] = self._parse_price(min_match.group(1)) * 1000
            
        # Extract maximum discount
        max_match = re.search(r'tối đa\s+(\d+(?:\.\d+)?)\s*k', text)
        if max_match:
            info['maximum_discount'] = self._parse_price(max_match.group(1)) * 1000
            
        return info
    
    def request_url(self, url: str, session=None, **kwargs) -> requests.Response:
        """Make request with retry logic and throttling."""
        self.throttle_request()
        session = session or self.init_session()
        timeout = self.config.get('timeout', 30)
        return session.get(url, timeout=timeout, **kwargs)
    
    def scrape_market_trends(self) -> dict:
        """
        Scrape market trends information.
        
        Returns:
            dict: Market trends data
        """
        self.logger.info("Scraping market trends...")
        # TODO: Implement actual scraping logic
        return {} 