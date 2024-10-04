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
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

from loguru import logger


from enum import Enum


# Configure logger
logger.remove()
logger.add(sys.stdout, format="{time} {level} {message}", level="INFO")
logger.add("logs/crawler_debug.log", rotation="50 MB", level="DEBUG")
logger.add("logs/crawler_info.log", rotation="50 MB", level="INFO")


class RegexPatterns:
    HTML_A_HREF = re.compile(r'<a\s+(?:[^>]*?\s+)?href="([^"]*)"')
    PRODUCT_LIST = re.compile(r'(https://world\.openfoodfacts\.org/\d+$)|(https://world.openfoodfacts.org$)')


class Crawler:
    def __init__(self, max_retries=10, save_interval=20, initial_crawl_delay=5):
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
        
        self.CHROMEDRIVER_PATH = '/usr/local/bin/chromedriver'
        
        self.chrome_service = Service(self.CHROMEDRIVER_PATH)
        self.chrome_options = Options()
        self.chrome_options.add_argument("--headless")
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument(f"user-agent={self.headers['User-Agent']}")
        self.driver = webdriver.Chrome(service=self.chrome_service, options=self.chrome_options)
        # Create data directory if it doesn't exist
        if not os.path.exists('data'):
            os.makedirs('data')

    def load_state(self):
        try:
            with open('crawler_state.pkl', 'rb') as f:
                state = pickle.load(f)
                
                self.iteration = state.get('iteration', 0)
                
                self.to_visit = deque(state.get('to_visit', []))
                _failed = state.get('failed', [])
                
                # Add failed URLs to the end of the to_visit deque
                self.to_visit.extend(_failed)
                
                self.visited = state.get('visited', set())
                self.failed = state.get('failed', set())
                self.never_crawl = state.get('never_crawl', set())
                self.url_hashes = state.get('url_hashes', {})
                self.total_data_crawled = sum(os.path.getsize(os.path.join('data', f)) for f in os.listdir('data') if os.path.isfile(os.path.join('data', f)))
                self.last_save_data_crawled = state.get('last_save_data_crawled', 0)
                self.crawl_delay = state.get('crawl_delay', self.crawl_delay)
                self.too_many_requests_count = state.get('too_many_requests_count', 0)
                self.successful_requests_count = state.get('successful_requests_count', 0)
                self.last_delay_adjustment = state.get('last_delay_adjustment', datetime.now())
                logger.info(f"Previous state loaded | Iteration: {self.iteration} | To visit: {len(self.to_visit)} | Visited: {len(self.visited)} | Failed: {len(self.failed)} | Never crawl: {len(self.never_crawl)} | Total data crawled: {self.total_data_crawled / (1024 * 1024):.2f} MB")
                logger.info(f"Crawl delay: {self.crawl_delay:.2f}s | Too many requests: {self.too_many_requests_count} | Successful requests: {self.successful_requests_count}")
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
            'last_delay_adjustment': self.last_delay_adjustment
        }
        with open('crawler_state.pkl', 'wb') as f:
            pickle.dump(state, f)

        data_crawled_since_last_save = self.total_data_crawled - self.last_save_data_crawled
        self.last_save_data_crawled = self.total_data_crawled

        logger.info(f"Saved current state | Iteration: {self.iteration} | To visit: {len(self.to_visit)} | Visited: {len(self.visited)} | Failed: {len(self.failed)} | Never crawl: {len(self.never_crawl)}")
        logger.info(f"Data crawled in last {self.save_interval} iterations: {data_crawled_since_last_save / (1024 * 1024):.2f} MB")
        logger.info(f"Total data crawled: {self.total_data_crawled / (1024 * 1024):.2f} MB | Average file size: {(self.total_data_crawled / len(self.visited) / (1024 * 1024)):.3f} MB")
        logger.info(f"Current crawl delay: {self.crawl_delay:.2f}s | Too many requests: {self.too_many_requests_count} | Successful requests: {self.successful_requests_count}")

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
            logger.error(f"Failed to fetch robots.txt")
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
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(html)
        self.save_url_hash(url_hash, url)
        self.total_data_crawled += os.path.getsize(file_path)

    def adjust_crawl_delay(self):
        now = datetime.now()
        if (now - self.last_delay_adjustment).total_seconds() >= 30:  # Check every 5 minutes
            old_crawl_delay = self.crawl_delay
            adjusted = False
            if self.too_many_requests_count > 5:
                self.crawl_delay = min(self.crawl_delay * 1.15, 15)  # Increase delay, max 30 seconds
                logger.info(f"Increased crawl delay to {self.crawl_delay:.2f}s due to too many requests")
                adjusted = True
            elif self.successful_requests_count > 20:
                self.crawl_delay = max(self.crawl_delay * 0.85, 4)  # Decrease delay, min 5 seconds
                logger.info(f"Decreased crawl delay to {self.crawl_delay:.2f}s due to successful requests")
                adjusted = True
            
            if adjusted:
                logger.info(f"Adjusting crawl delay [{old_crawl_delay:.2f}s => {self.crawl_delay:.2f}s] | Too many requests: {self.too_many_requests_count} | Successful requests: {self.successful_requests_count}")
                self.too_many_requests_count = 0
                self.successful_requests_count = 0
                self.last_delay_adjustment = now

    def crawl(self):
        while self.to_visit:
            url, retry_count = self.to_visit.popleft()
        
            if url in self.visited or url in self.never_crawl or not self.can_crawl(url):
                continue
            
            # if RegexPatterns.PRODUCT_LIST.match(url):
            #         try:
            #             logger.debug(f"Selenium loading: {url}")
            #             self.driver.set_page_load_timeout(10)
            #             self.driver.get(url)
            #             html = self.driver.page_source
                        
            #         except Exception as e:
            #             logger.warning(f"Selenium failed to load [Retry:{retry_count}] {url}: {str(e)}")
            #             raise requests.RequestException(str(e))
            #     else:
            #         try:
            #             response = requests.get(url, timeout=10, headers=self.headers)
            #             response.raise_for_status()
            #             html = response.text
            #         except requests.RequestException as e:
            #             if "429" in str(e):
            #                 self.too_many_requests_count += 1
            #                 logger.warning(f"[429] Too many requests. Increasing delay.")
            #                 logger.warning(f"[Retry:{retry_count}] {url}")
            #                 logger.debug(f"Current crawl delay: {self.crawl_delay:.2f}s")
                            
            #                 raise
                            
            #             if "404" in str(e):
            #                 logger.warning(f"[404] Not Found: {url}")
            #                 self.never_crawl.add(url)
            #                 self.successful_requests_count += 1
            #                 continue
                        
            #             if "443" in str(e):
            #                 logger.warning(f"[443] SSL Error / Forbidden: {url}")
            #                 self.successful_requests_count += 1
            #                 self.never_crawl.add(url)
            #                 continue
                        
            #             if "403" in str(e):
            #                 logger.warning(f"[403] Forbidden: {url}")
            #                 self.successful_requests_count += 1
            #                 self.never_crawl.add(url)
            #                 continue
                            
            #             logger.warning(f"Requests failed to load [Retry:{retry_count}] {url}: {str(e)}")
            #             raise
            
            try:
                time.sleep(self.crawl_delay)
                self.adjust_crawl_delay()
                
                logger.debug(f"Selenium loading: {url}")
                self.driver.set_page_load_timeout(10)
                self.driver.get(url)
                html = self.driver.page_source
                
                self.successful_requests_count += 1
                self.save_html(url, html)
                self.visited.add(url)
                self.extract_links(html, url)
                logger.success(f"Crawled: {url}")

                self.iteration += 1
                if self.iteration % self.save_interval == 0:
                    self.save_state()

            except Exception as e:
                error_message = str(e)
                if "timeout" in error_message.lower():
                    self.too_many_requests_count += 1
                    logger.warning(f"[Timeout] Selenium failed to load. Increasing delay.")
                    logger.warning(f"[Retry:{retry_count}] {url}")
                    logger.debug(f"Current crawl delay: {self.crawl_delay:.2f}s")
                elif "no such element" in error_message.lower():
                    logger.warning(f"[404] Not Found: {url}")
                    self.never_crawl.add(url)
                    self.successful_requests_count += 1
                    continue
                elif "ssl" in error_message.lower():
                    logger.warning(f"[SSL Error] Forbidden: {url}")
                    self.successful_requests_count += 1
                    self.never_crawl.add(url)
                    continue
                elif "forbidden" in error_message.lower():
                    logger.warning(f"[403] Forbidden: {url}")
                    self.successful_requests_count += 1
                    self.never_crawl.add(url)
                    continue
                else:
                    logger.warning(f"Selenium failed to load [Retry:{retry_count}] {url}: {error_message}")
                
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
        finally:
            self.save_state()

if __name__ == "__main__":
    start_time = datetime.now(tz=timezone(timedelta(hours=2), 'Europe/Bratislava'))
    logger.info("=" * 50)
    logger.info(f"Starting crawler at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 50)
    
    crawler = Crawler(max_retries=10, save_interval=10, initial_crawl_delay=5)
    crawler.run()
