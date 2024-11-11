import requests
import pickle
from datetime import datetime, timedelta
import re
import hashlib
from collections import deque
from threading import Thread
import time
import copy
import os
import json
import sys
from datetime import timezone
#from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.common.exceptions import TimeoutException, WebDriverException

from seleniumwire import webdriver


from urllib3.exceptions import MaxRetryError

from requests.exceptions import HTTPError

from loguru import logger


from enum import Enum

import traceback
import argparse


# Configure logger
logger.remove()
logger.add(sys.stdout, format="{time} {level} {message}", level="INFO")
logger.add("logs/crawler_debug.log", rotation="50 MB", level="DEBUG")
logger.add("logs/crawler_info.log", rotation="50 MB", level="INFO")


class RegexPatterns:
    HTML_A_HREF = re.compile(r'<a\s+(?:[^>]*?\s+)?href="([^"]*)"')
    PRODUCT_LIST = re.compile(r'(https://world\.openfoodfacts\.org/\d+$)|(https://world.openfoodfacts.org$)')
    REMOVE_JAVASCRIPT = re.compile(r'<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>')
    PRODUCT_URL = re.compile(r'https://world\.openfoodfacts\.org/product/.*')


class Crawler:
    def __init__(self, max_retries=10, save_interval=20, initial_crawl_delay=5, driver_type='chrome'):
        self.base_url: str = "https://world.openfoodfacts.org"
        self.to_visit: deque = deque([[self.base_url, 0]])  # [url, retry_count]
        self.visited: set = set()
        self.failed: set = set()
        self.never_crawl: set = set()
        self.max_retries: int = max_retries
        self.save_interval: int = save_interval
        self.iteration: int = 0
        self.crawl_delay: float = initial_crawl_delay
        self.headers: dict = {
            'User-Agent': 'University Project Crawler (Contact: xstrbol@stuba.sk)'
        }
        self.robots_cache: set = set()
        self.url_hashes: dict = {}
        self.total_data_crawled: int = 0
        self.last_save_data_crawled: int = 0
        self.too_many_requests_count: int = 0
        self.successful_requests_count: int = 0
        self.last_delay_adjustment: datetime = datetime.now()
        
        self.reorder_interval: int = 500

        self.forbidden_extensions = [
            'jpg', 'jpeg', 'png', 'gif', 'bmp', 'svg', 'webp',  # Images
            'mp4', 'avi', 'mov', 'wmv', 'flv', 'webm',  # Videos
            'mp3', 'wav', 'ogg', 'flac',  # Audio
            'pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx',  # Documents
            'zip', 'rar', '7z', 'tar', 'gz',  # Archives
            'exe', 'msi', 'bin',  # Executables
            'css', 'js', 'json', 'xml',  # Web assets and data formats
            'ico', 'ttf', 'woff', 'woff2',  # Icons and fonts
        ]

        self.get_robots_rules()
        
        self.driver_type = driver_type
        self.setup_driver()
        
        # Create data directory if it doesn't exist
        if not os.path.exists('data'):
            os.makedirs('data')

    def setup_driver(self):
        if self.driver_type == 'chrome':
            self.CHROMEDRIVER_PATH = '/usr/local/bin/chromedriver'
            self.chrome_service = Service(self.CHROMEDRIVER_PATH)
            self.chrome_options = Options()
            self.chrome_options.add_argument("--headless")
            self.chrome_options.add_argument("--no-sandbox")
            self.chrome_options.add_argument('--disable-dev-shm-usage')
            self.chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            self.chrome_options.add_argument('--no-zygote')
            self.chrome_options.add_argument(f"user-agent={self.headers['User-Agent']}")
            self.driver = webdriver.Chrome(service=self.chrome_service, options=self.chrome_options)
        elif self.driver_type == 'firefox':
            self.FIREFOX_GECKO_PATH = '/usr/local/bin/geckodriver'
            self.firefox_service = FirefoxService(self.FIREFOX_GECKO_PATH)
            self.firefox_options = FirefoxOptions()
            self.firefox_options.add_argument("--headless")
            self.firefox_options.set_preference("general.useragent.override", self.headers['User-Agent'])
            self.firefox_options.set_preference("dom.webdriver.enabled", False)
            self.firefox_options.set_preference('useAutomationExtension', False)
            self.driver = webdriver.Firefox(options=self.firefox_options, service=self.firefox_service)
        else:
            raise ValueError("Invalid driver type. Choose 'chrome' or 'firefox'.")
        

        self.driver.set_page_load_timeout(15)
            
    def _remake_driver(self):
        self.driver.quit()
        self.setup_driver()
    
    def load_state(self):
        logger.info("=" * 50)
        logger.info("Loading previous state...")
        try:
            with open('crawler_state.pkl', 'rb') as f:
                state = pickle.load(f)
                
                self.iteration = state.get('iteration', 0)
                
                _combined = list(set([x[0] for x in state.get('to_visit', [])] + [x[0] for x in state.get('failed', [])]))
                
                combined_with_retry = [[url, 0] for url in _combined]
                
                self.to_visit = deque(combined_with_retry)
                _failed = state.get('failed', [])
                
                _to_visit = state.get('to_visit', [])
                
                # Add failed URLs to the end of the to_visit deque
                self.to_visit.extend(_failed)
                
                self.visited = state.get('visited', set())
                self.failed = set()
                self.never_crawl = state.get('never_crawl', set())
                self.url_hashes = state.get('url_hashes', {})
                self.total_data_crawled = sum(os.path.getsize(os.path.join('data', f)) for f in os.listdir('data') if os.path.isfile(os.path.join('data', f)))
                self.last_save_data_crawled = state.get('last_save_data_crawled', 0)
                self.crawl_delay = state.get('crawl_delay', self.crawl_delay)
                
                if self.crawl_delay < 5:
                    self.crawl_delay = 5
                
                self.reorder_interval = state.get('reorder_interval', 500)
                self.too_many_requests_count = state.get('too_many_requests_count', 0)
                self.successful_requests_count = state.get('successful_requests_count', 0)
                self.last_delay_adjustment = state.get('last_delay_adjustment', datetime.now())
                
                self.reorder_to_visit()
                
                self.next_reorder_iteration = self.iteration + self.reorder_interval
                
                logger.success(f"Previous state loaded | Iteration: {self.iteration} | To visit: {len(self.to_visit)} (Previously: {len(_to_visit)}) | Visited: {len(self.visited)} | Previously Failed: {len(_failed)} | Never crawl: {len(self.never_crawl)} | Total data crawled: {self.total_data_crawled / (1024 * 1024):.2f} MB")
                logger.info(f"Reorder interval: {self.reorder_interval} (at {self.next_reorder_iteration} iteration) | Crawl delay: {self.crawl_delay:.2f}s | Too many requests: {self.too_many_requests_count} | Successful requests: {self.successful_requests_count}")
                
                
                logger.info("=" * 50)
                
                
        except FileNotFoundError:
            logger.info("No previous state found ... Starting from the beginning")

    def save_state(self):
        state = {
            'iteration': self.iteration,
            'to_visit': self.to_visit,
            'visited': self.visited,
            'failed': self.failed,
            'never_crawl': self.never_crawl,
            'url_hashes': self.url_hashes,
            'total_data_crawled': self.total_data_crawled,
            'last_save_data_crawled': self.last_save_data_crawled,
            'crawl_delay': self.crawl_delay,
            'too_many_requests_count': self.too_many_requests_count,
            'successful_requests_count': self.successful_requests_count,
            'last_delay_adjustment': self.last_delay_adjustment,
            'reorder_interval': self.reorder_interval
        }
        with open('crawler_state.pkl', 'wb') as f:
            pickle.dump(state, f)

        data_crawled_since_last_save = self.total_data_crawled - self.last_save_data_crawled
        self.last_save_data_crawled = self.total_data_crawled

        logger.success(f"Saved current state | Iteration: {self.iteration} | To visit: {len(self.to_visit)} | Visited: {len(self.visited)} | Failed: {len(self.failed)} | Never crawl: {len(self.never_crawl)}")
        logger.info(f"Data crawled in last {self.save_interval} iterations: {data_crawled_since_last_save / (1024 * 1024):.2f} MB")
        logger.info(f"Total data crawled: {self.total_data_crawled / (1024 * 1024):.2f} MB | Average file size: {(self.total_data_crawled / len(self.visited) / (1024 * 1024)):.3f} MB")
        logger.info(f"Reorder interval: {self.reorder_interval} (at {self.iteration + self.reorder_interval} iteration - left: {self.reorder_interval - self.iteration % self.reorder_interval}) | Current crawl delay: {self.crawl_delay:.2f}s | Too many requests: {self.too_many_requests_count} | Successful requests: {self.successful_requests_count}")

    def get_robots_rules(self):
        try:
            robots_url = f"{self.base_url}/robots.txt"
            response = requests.get(robots_url, timeout=10, headers=self.headers)
            response.raise_for_status()
            robots_content = response.text
            
            parsing_user_agent_all = False
            for line in robots_content.split('\n'):
                line = line.strip().lower()
                if line.startswith('user-agent:'):
                    parsing_user_agent_all = (line.split(':', 1)[1].strip() == '*')
                elif parsing_user_agent_all and line.startswith('disallow:'):
                    path = line.split(':', 1)[1].strip()
                    self.robots_cache.add(re.compile(re.escape(path).replace('\*', '.*')))
            
        except requests.RequestException:
            logger.error(f"Failed to fetch robots.txt. Exiting...")
            exit(1)

    def can_crawl(self, url):
        if url.startswith('https://'):
            if not url.startswith(self.base_url):
                return False
            path = url[len(self.base_url):]
        elif url.startswith('/'):
            path = url
        else:
            return False

        # Check if the URL has a forbidden extension
        if any(url.lower().endswith(f'.{ext}') for ext in self.forbidden_extensions):
            return False

        return not any(rule.match(path) for rule in self.robots_cache)

    def hash_url(self, url):
        return hashlib.sha256(url.encode()).hexdigest()

    def save_url_hash(self, url_hash, url):
        self.url_hashes[url_hash] = url
        with open('url_hashes.txt', 'a') as f:
            f.write(f"{url_hash}\t{url}\n")
    
    def save_html(self, url, html):
        url_hash = self.hash_url(url)
        file_path = f'data/{url_hash}.html'
        if os.path.exists(file_path):
            logger.critical(f"Saving the same file twice: [Hash: {url_hash}] => {url}")
        
        html_without_javascript = RegexPatterns.REMOVE_JAVASCRIPT.sub('', html)
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(html_without_javascript)
        self.save_url_hash(url_hash, url)
        self.total_data_crawled += os.path.getsize(file_path)

    def adjust_crawl_delay(self):
        now = datetime.now()
        if (now - self.last_delay_adjustment).total_seconds() >= 30 or self.too_many_requests_count > 4 :  # Check every 5 minutes
            old_crawl_delay = self.crawl_delay
            adjusted = False
            if self.too_many_requests_count > 2:
                self.crawl_delay = min(self.crawl_delay * 1.25, 15)  # Increase delay, max 30 seconds
                logger.info(f"Increased crawl delay to {self.crawl_delay:.2f}s due to too many requests")
                adjusted = True
            elif self.successful_requests_count > 20:
                self.crawl_delay = max(self.crawl_delay * 0.75, 0.5)  # Decrease delay, min 5 seconds
                logger.info(f"Decreased crawl delay to {self.crawl_delay:.2f}s due to successful requests")
                adjusted = True
            
            if adjusted:
                logger.info(f"Adjusting crawl delay [{old_crawl_delay:.2f}s => {self.crawl_delay:.2f}s] | Too many requests: {self.too_many_requests_count} | Successful requests: {self.successful_requests_count}")
                self.too_many_requests_count = 0
                self.successful_requests_count = 0
                self.last_delay_adjustment = now

    def reorder_to_visit(self):
        logger.warning(f"Reordering to_visit queue | Iteration: {self.iteration}")
        product_urls = deque()
        other_urls = deque()
        product_list_urls = deque()
        
        for entry in self.to_visit:
            if isinstance(entry, list):
                url, retry_count = entry
            else:
                url = entry[0]
                retry_count = 0
            
            if RegexPatterns.PRODUCT_URL.search(url):
                product_urls.append([url, retry_count])
            elif RegexPatterns.PRODUCT_LIST.search(url):
                product_list_urls.append([url, retry_count])
            else:
                other_urls.append([url, retry_count])
        
        # Sort product_list_urls by the lowest number in the URL
        sorted_product_list_urls = sorted(product_list_urls, key=lambda x: int(re.search(r'/(\d+)$', x[0]).group(1)) if re.search(r'/(\d+)$', x[0]) else float('inf'))
        
        self.to_visit = deque(list(product_urls) + sorted_product_list_urls + list(other_urls))
        
        self.reorder_interval = len(product_urls) + len(sorted_product_list_urls) + round(len(other_urls)*0.00025)
        
        self.next_reorder_iteration = self.iteration + self.reorder_interval
        
        #self.to_visit = product_urls + other_urls
        logger.info(f"Reordered to_visit queue | Product URLs: {len(product_urls)} | Product list URLs: {len(sorted_product_list_urls)} | Other URLs: {len(other_urls)} | New reorder at: {self.next_reorder_iteration} ({self.next_reorder_iteration - self.iteration} iterations left)")
        
        logger.success(f"Reordered to_visit queue | Iteration: {self.iteration}")

    def _close_driver(self):
        self.driver.close()
        self.driver.quit()
        


    def crawl(self):
        #reorder_interval = 2000  # Reorder every 100 iterations
        while self.to_visit:
            if self.iteration >= self.next_reorder_iteration:
                self.reorder_to_visit()

            
            url, retry_count = self.to_visit.popleft()
        
            if url in self.visited or url in self.never_crawl or not self.can_crawl(url):
                self.iteration += 1
                continue
            
            try:
                self.iteration += 1
                time.sleep(self.crawl_delay)
                #input("Press Enter to continue...")
                self.adjust_crawl_delay()
                
                #self._remake_driver()
                
                del self.driver.requests
                self.driver.get(url)
                
                
                status_code = self.driver.requests[0].response.status_code
                reason = self.driver.requests[0].response.reason
                
                if status_code != 200:
                    self._close_driver()
                    raise HTTPError(f"[{status_code}][Retry:{retry_count}][Reason:{reason}] HTTP Error - {url}")
                
                html: str = self.driver.page_source
                
                self.successful_requests_count += 1
                self.save_html(url, html)
                self.visited.add(url)
                self.extract_links(html, url)
                logger.success(f"Crawled: {url}")

                #self._close_driver()
                if self.iteration % self.save_interval == 0:
                    self.save_state()

            except TimeoutException as e:
                logger.warning(f"[TIMEOUT] Failed to load [Retry:{retry_count}] {url}: {str(e)}")
                self.handle_failed_url(url, retry_count)
                
            except MaxRetryError as e:
                logger.warning(f"[DRIVER][MaxRetryError] Driver Error, remake driver")
                self._remake_driver()
                self.handle_failed_url(url, retry_count)
                
            except WebDriverException as e:
                logger.warning(f"[DRIVER][WebDriverException] Selenium WebDriver Error: {str(e)}")
                self._remake_driver()
                self.handle_failed_url(url, retry_count)
                
            except Exception as e:
                error_message = str(e)
                logger.warning(error_message)
                
                if status_code == 429:
                    self.too_many_requests_count += 1
                    #logger.warning(f"[429] Too many requests [Retry:{retry_count}][Reason: {reason}] => {url}")
                    
                    logger.debug(f"Current crawl delay: {self.crawl_delay:.2f}s")
                    self.handle_failed_url(url, retry_count)
                    continue
                
                if status_code in [404, 403, 443, 301,302]:
                    #logger.warning(f"[404] Not Found [Reason: {reason}] => {url}")
                    self.never_crawl.add(url)
                    self.successful_requests_count += 1
                    continue

                #logger.warning(f"[{status_code}] Selenium failed to load [Retry:{retry_count}][Reason: {reason}] => {url}: {error_message}")
                self.handle_failed_url(url, retry_count)
                

    def handle_failed_url(self, url, retry_count):
        retry_count += 1
        if retry_count > self.max_retries:
            self.failed.add(url)
            logger.error(f"URL {url} failed more than {self.max_retries} times. Moving to failed set.")
        else:
            if len(self.to_visit) > 10:
                self.to_visit.insert(10, [url, retry_count])
            else:
                self.to_visit.append([url, retry_count])

    def extract_links(self, html, base_url):
        links = RegexPatterns.HTML_A_HREF.findall(html)
        
        unique_links = set()
        
        for link in links:
            if link.startswith('http://'):
                link = link.replace('http://', 'https://')
            
            if link.startswith('/'):
                link = f"{self.base_url}{link}"
                
            link = link.rstrip('/')
            unique_links.add(link)
                
        for link in unique_links:
            if link in self.visited or link in self.never_crawl:
                logger.debug(f"Link already visited or in never_crawl: {link}")
                continue
            
            if not self.can_crawl(link):
                logger.debug(f"Link cannot be crawled: {link}")
                continue
            
            if link.startswith('https://'):
                self.to_visit.append((link.rstrip('/'), 0))
                logger.debug(f"Added link: {link.rstrip('/')}")
                
            elif link.startswith('/'):
                _link = f"{base_url.rstrip('/')}{link.rstrip('/')}"
                self.to_visit.append((_link, 0))
                logger.debug(f"Added link: {_link.rstrip('/')}")
            else:
                logger.debug(f"Wrong Link: {link}")

    def run(self):
        self.load_state()
        try:
            self.crawl()
        except KeyboardInterrupt:
            logger.warning("Crawler stopped by user")
        except Exception as e:
            logger.error(f"Unexpected error occurred - {str(e)}")
            logger.debug(f"Traceback:\n\n{traceback.format_exc()}\n\n")
        finally:
            self.save_state()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Web Crawler')
    parser.add_argument('--driver', type=str, choices=['chrome', 'firefox'], default='chrome', help='Choose the browser driver (chrome or firefox)')
    args = parser.parse_args()

    start_time = datetime.now(tz=timezone(timedelta(hours=2), 'Europe/Bratislava'))
    logger.info("=" * 50)
    logger.info(f"Starting crawler at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Using {args.driver} driver")
    logger.info(f"PID: {os.getpid()}")
    logger.info("=" * 50)
    
    crawler = Crawler(max_retries=10, save_interval=10, initial_crawl_delay=5, driver_type=args.driver)
    crawler.run()
