import os
import aiohttp
import asyncio
from bs4 import BeautifulSoup
import json
import logging
from datetime import datetime
import time

# Đảm bảo thư mục logs tồn tại
os.makedirs('logs', exist_ok=True)
logger = logging.getLogger('aeon_trending_scraper')
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler('logs/aeon_trending_scraper.log')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

BASE_URL = 'https://www.aeon.com.vn/gia-thap-moi-ngay'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
}

class AeonTrendingScraper:
    def __init__(self):
        self.session = None
        self.areas = [
            'Tất cả khu vực',
            'AEON Tân Phú',
            'AEON Long Biên',
            'AEON Bình Dương',
            'AEON Bình Tân',
            'AEON Hà Đông',
            'AEON Hải Phòng',
            'AEON The Nine',
            'AEON Bình Dương New City',
            'AEON Huế',
            'AEON Nguyễn Văn Linh',
            'AEON Xuân Thủy'
        ]
        self.categories = [
            'Tất cả danh mục',
            'Thịt cá tươi các loại',
            'Rau củ quả tươi',
            'Gia vị',
            'Thực phẩm đóng gói',
            'Thực phẩm đông lạnh, kem sữa',
            'Vệ sinh nhà cửa',
            'Làm đẹp',
            'Thức uống và bánh kẹo các loại',
            'TOPVALU',
            'HOME COORDY'
        ]

    async def init_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession(headers=HEADERS)

    async def close_session(self):
        if self.session:
            await self.session.close()
            self.session = None

    async def fetch_url(self, url):
        """Fetch URL with retry logic"""
        await self.init_session()
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    logger.error(f"Failed to fetch {url}, status: {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Error fetching {url}: {str(e)}")
            return None

    async def scrape_trending_products(self, area="Tất cả khu vực", category="Tất cả danh mục"):
        """Scrape trending products for a specific area and category"""
        url = BASE_URL
        params = {}
        
        if area != "Tất cả khu vực":
            params['area'] = area.lower().replace(' ', '-')
        if category != "Tất cả danh mục":
            params['category'] = self.categories.index(category)
            
        if params:
            url = f"{url}?{'&'.join(f'{k}={v}' for k, v in params.items())}"

        logger.info(f"Fetching URL: {url}")
        html = await self.fetch_url(url)
        if not html:
            return []

        soup = BeautifulSoup(html, 'html.parser')
        products = []
        
        # Parse products from HTML
        product_elements = soup.select('.product-item')
        for element in product_elements:
            try:
                product = {
                    'name': element.select_one('.product-name').text.strip(),
                    'price': element.select_one('.product-price').text.strip(),
                    'area': area,
                    'category': category,
                    'url': element.select_one('a')['href'],
                    'image_url': element.select_one('img')['src']
                }
                products.append(product)
            except Exception as e:
                logger.error(f"Error parsing product: {str(e)}")
                continue

        logger.info(f"Found {len(products)} products for area={area}, category={category}")
        return products

    async def scrape_all_trending(self):
        """Scrape all trending products across all areas and categories"""
        all_products = []
        tasks = []

        # Create tasks for all area-category combinations
        for area in self.areas:
            for category in self.categories:
                tasks.append(self.scrape_trending_products(area, category))

        # Run all tasks concurrently
        results = await asyncio.gather(*tasks)
        
        # Flatten results
        for products in results:
            all_products.extend(products)

        # Remove duplicates based on product URL
        unique_products = {p['url']: p for p in all_products}.values()
        return list(unique_products)

async def main():
    scraper = AeonTrendingScraper()
    try:
        products = await scraper.scrape_all_trending()
        
        # Save to local file
        os.makedirs('data/aeon', exist_ok=True)
        filename = f'data/aeon/trending_products_{datetime.now().strftime("%Y%m%d")}.json'
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(products, f, ensure_ascii=False, indent=2)
            
        logger.info(f"Saved {len(products)} trending products to {filename}")
        
    finally:
        await scraper.close_session()

if __name__ == "__main__":
    asyncio.run(main()) 