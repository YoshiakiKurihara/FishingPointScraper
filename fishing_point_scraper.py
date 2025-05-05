from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from tqdm import tqdm
import time
import os
from datetime import datetime
import json
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from functools import lru_cache
import logging
from lxml import html as lxml_html
import urllib.parse
import hashlib

# ロギングの設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AsyncCache:
    def __init__(self, maxsize=5000):
        self.cache = {}
        self.maxsize = maxsize
        self.order = []

    async def get(self, key, fetch_func):
        if key in self.cache:
            return self.cache[key]
        
        value = await fetch_func()
        self.cache[key] = value
        self.order.append(key)

        # サイズ制限を超えたら最も古いものを削除
        if len(self.order) > self.maxsize:
            oldest = self.order.pop(0)
            del self.cache[oldest]

        return value
    
class FishingPointScraper:
    def __init__(self):
        self.base_url = "https://www.point-official.shop/shop/goods/search.aspx"
        self.params = {
            "po": "ライン・ハリス・道糸"
        }
        self.itemUrls = []
        self.products = []
        self.setup_driver()
        self.session = None
        
        self.page_cache = AsyncCache(maxsize=5000)
        
        # 画像保存用のディレクトリを作成
        self.image_dir = "product_images"
        os.makedirs(self.image_dir, exist_ok=True)

    def setup_driver(self):
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--lang=ja')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        
        # パフォーマンス最適化のための追加オプション
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-notifications')
        chrome_options.add_argument('--disable-popup-blocking')
        chrome_options.add_argument('--disable-infobars')
        chrome_options.add_argument('--disable-browser-side-navigation')
        chrome_options.add_argument('--disable-features=NetworkService')
        
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.wait = WebDriverWait(self.driver, 10)

    async def setup_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession()

    #@lru_cache(maxsize=5000)
    async def fetch_page(self, url):
        async def fetch_func():
            if not self.session:
                await self.setup_session()
            async with self.session.get(url) as response:
                return await response.text()

        return await self.page_cache.get(url, fetch_func)
        #"""ページの内容を取得し、キャッシュする"""
        #if not self.session:
        #    await self.setup_session()
        #async with self.session.get(url) as response:
        #    return await response.text()

    def wait_for_element(self, by, value, timeout=10):
        """要素の出現を待機する効率的なメソッド"""
        try:
            return self.wait.until(EC.presence_of_element_located((by, value)))
        except Exception as e:
            logger.warning(f"要素の待機中にエラーが発生: {value} - {str(e)}")
            return None

    async def get_total_pages(self):
        """総ページ数を取得"""
        self.driver.get(f"{self.base_url}?po={self.params['po']}")
        
        try:
            pagination = self.wait_for_element(
                By.XPATH, 
                "/html/body/div[1]/div[3]/div/main/div[1]/div/div[3]/ul/li[8]/a"
            )
            if pagination:
                last_page = pagination.text
                return int(last_page)
        except Exception as e:
            logger.error(f"ページ数の取得に失敗: {str(e)}")
        return 1

    async def download_image(self, image_url, product_name):
        """画像をダウンロードして保存する"""
        try:
            if not image_url:
                return None

            # URLからファイル名を取得
            parsed_url = urllib.parse.urlparse(image_url)
            filename = os.path.basename(parsed_url.path)
            filepath = os.path.join(self.image_dir, filename)

            # 画像が既に存在する場合はスキップ
            if os.path.exists(filepath):
                return filepath

            # 画像をダウンロード
            async with self.session.get(image_url) as response:
                if response.status == 200:
                    with open(filepath, 'wb') as f:
                        f.write(await response.read())
                    return filepath
                else:
                    logger.error(f"画像のダウンロードに失敗: {image_url} - ステータス: {response.status}")
                    return None

        except Exception as e:
            logger.error(f"画像のダウンロード中にエラー: {image_url} - {str(e)}")
            return None

    async def get_product_detail(self, item_url):
        """商品詳細情報を取得"""
        try:
            html_text = await self.fetch_page(item_url)
            tree = lxml_html.fromstring(html_text)
            #soup = BeautifulSoup(html, 'lxml')
            
            # 商品コメントの取得
            goods_comment = self._extract_text_from_class(
                tree, 
                ["block-goods-comment"],
                element_id="spec_goods_comment"
            )
            
            # 商品説明の取得
            goods_description = self._extract_text_from_class(
                tree, 
                ["h1 block-goods-name--text js-enhanced-ecommerce-goods-name"],
            )
            
            # 商品価格の取得
            goods_price = self._extract_text_from_class(
                tree, 
                ["block-goods-price--price js-enhanced-ecommerce-goods-price",
                "block-goods-price--price price js-enhanced-ecommerce-goods-price"],
                start_text="￥"
            )

            # 在庫状況の取得
            stock_status = self._extract_text_from_class(
                tree, 
                ["block-goods-price--price_stock mb10"]
            )
        
            # スペックタイトルの取得
            spec_title_status = self._extract_text_from_class(
                tree, 
                ["goods-detail-description-mtit01 mb20"]
            )

            # スペック1の取得
            spec_1_status = self._extract_text_from_class(
                tree, 
                ["block-goods-comment3 mb20"]
            )

            # スペック2の取得
            spec_2_status = self._extract_text_from_class(
                tree, 
                ["block-goods-comment4 mb20"]
            )
            
            # 商品詳細説明１の取得
            detail_1_status = self._extract_text_from_xpath(tree, [
                "/html/body/div[1]/div[3]/div/main/div[2]/div[4]/p[1]"
            ])
            
            # 商品詳細説明２の取得
            detail_2_status = self._extract_text_from_xpath(tree, [
                "/html/body/div[1]/div[3]/div/main/div[2]/div[4]/p[2]"
            ])
            
            # 商品詳細説明３の取得
            detail_3_status = self._extract_text_from_xpath(tree, [
                "/html/body/div[1]/div[3]/div/main/div[2]/div[4]/dl"
            ])
            
            # 写真URLの取得
            picture_url = self._extract_image_src_from_class(
                tree, 
                ["block-src-l--image"]
            )
            
            # 画像をダウンロード
            image_path = None
            if picture_url and goods_description:
                image_path = await self.download_image(picture_url, goods_description)
            
            product_data = {
                '商品コメント': goods_comment,
                '商品説明': goods_description,
                '商品価格': goods_price,
                '在庫状況': stock_status,
                'スペックタイトル': spec_title_status,
                'スペック1': spec_1_status,
                'スペック2': spec_2_status,
                '商品詳細説明１': detail_1_status,
                '商品詳細説明２': detail_2_status,
                '商品詳細説明３': detail_3_status,
                '商品画像URL': picture_url,
                '商品画像パス': image_path,
                'URL': item_url
            }
            
            self.products.append(product_data)
            return product_data
            
        except Exception as e:
            logger.error(f"商品詳細の取得中にエラー: {item_url} - {str(e)}")
            return None

    def _extract_text_from_xpath(self, tree, xpaths):
        """XPathからテキストを抽出する"""
        for xpath in xpaths:
            try:
                elements = tree.xpath(xpath)
                if elements:
                    return elements[0].text_content().strip()
            except Exception as e:
                logging.error(f"XPath抽出中にエラー: {xpath} - {str(e)}")
                continue
        return None

    def _extract_text_from_class(self, tree, class_names, start_text=None, element_id=None):
        """class名とidからテキストを抽出する
        
        Args:
            tree: lxmlのHTMLツリー
            class_names: 検索対象のclass名のリスト
            start_text: テキストの開始文字列（指定された場合、この文字列で始まる要素のみを取得）
            element_id: 要素のid（指定された場合、このidを持つ要素のみを取得）
        """
        for class_name in class_names:
            try:
                # class名を含む要素を検索
                xpath_query = f"//*[contains(@class, '{class_name}')]"
                if element_id:
                    xpath_query = f"//*[@id='{element_id}' and contains(@class, '{class_name}')]"
                
                elements = tree.xpath(xpath_query)
                if elements:
                    if start_text:
                        # 開始文字列が指定されている場合、条件に合う要素を探す
                        for element in elements:
                            text = element.text_content().strip()
                            if text.startswith(start_text):
                                return text
                    else:
                        # 開始文字列が指定されていない場合、最初の要素を返す
                        return elements[0].text_content().strip()
            except Exception as e:
                logging.error(f"要素の抽出中にエラー: class={class_name}, id={element_id} - {str(e)}")
                continue
        return None

    def _extract_image_src_from_class(self, tree, class_names, element_id=None):
        """class名とidから画像のsrc属性を抽出する
        
        Args:
            tree: lxmlのHTMLツリー
            class_names: 検索対象のclass名のリスト
            element_id: 要素のid（指定された場合、このidを持つ要素のみを取得）
        """
        for class_name in class_names:
            try:
                # class名を含むimg要素を検索
                xpath_query = f"//img[contains(@class, '{class_name}')]"
                if element_id:
                    xpath_query = f"//img[@id='{element_id}' and contains(@class, '{class_name}')]"
                
                elements = tree.xpath(xpath_query)
                if elements:
                    # src属性を取得
                    src = elements[0].get('src')
                    if src:
                        # 相対パスの場合、絶対パスに変換
                        if src.startswith('/'):
                            full_url = f"https://www.point-official.shop{src}"
                            logger.info(f"画像URLを変換: {src} -> {full_url}")
                            return full_url
                        logger.info(f"画像URL: {src}")
                        return src
            except Exception as e:
                logging.error(f"画像srcの抽出中にエラー: class={class_name}, id={element_id} - {str(e)}")
                continue
        return None

    async def scrape_page(self, page):
        """ページの商品URLを取得"""
        try:
            if page == 1:
                self.driver.get(f"{self.base_url}?po={self.params['po']}")
            else:
                current_li = WebDriverWait(self.driver, 10).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "li.pager-current"))
                            )
                
                # 次の<li>に含まれる<a>タグを取得してクリック
                next_a = current_li.find_element(By.XPATH, "following-sibling::li[1]/a")
                
                # ページネーションボタンをクリック
                if next_a:
                    # JavaScriptでクリックをシミュレート
                    self.driver.execute_script("arguments[0].click();", next_a)
                    # ページ遷移の待機
                    self.wait_for_element(
                        By.CLASS_NAME,
                        "js-enhanced-ecommerce-image"
                    )
            
            # 商品リンクを取得
            items = self.driver.find_elements(By.CLASS_NAME, "js-enhanced-ecommerce-image")
            for item in items:
                href = item.get_attribute('href')
                if href:
                    self.itemUrls.append(href)
                    
        except Exception as e:
            logger.error(f"ページ {page} のスクレイピング中にエラー: {str(e)}")

    async def process_products(self):
        """商品詳細情報を並列で取得"""
        tasks = [self.get_product_detail(url) for url in self.itemUrls]
        await asyncio.gather(*tasks)

    def save_to_excel(self):
        """結果をExcelに保存"""
        df = pd.DataFrame(self.products)
        filename = f'fishing_point_products_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        df.to_excel(filename, index=False)
        logger.info(f"データを {filename} に保存しました")

    async def run(self):
        """メインの実行メソッド"""
        logger.info("スクレイピングを開始します...")
        
        try:
            total_pages = await self.get_total_pages()
            logger.info(f"全 {total_pages} ページを処理します")
            
            # ページのURLを収集
            for page in tqdm(range(1, total_pages + 1)):
                await self.scrape_page(page)
            
            # 商品詳細を並列で取得
            await self.process_products()
            
            # 結果を保存
            self.save_to_excel()
            
        except Exception as e:
            logger.error(f"スクレイピング中にエラーが発生: {str(e)}")
        finally:
            if self.session:
                await self.session.close()
            self.driver.quit()

if __name__ == "__main__":
    scraper = FishingPointScraper()
    asyncio.run(scraper.run()) 