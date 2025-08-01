import datetime
import os
import random
import time
from stem import Signal
from stem.control import Controller
import requests
from googlesearch import search
import pandas as pd
from parsers.base_parser import BaseParser
from models import NewsItem
from config.settings import settings
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


class GoogleParser(BaseParser):
    def __init__(self, category: str, region: str, period: str, date_from: datetime, to_excel: bool):
        super().__init__()
        self.class_name = 'Google'
        self.category = category
        self.region = region
        self.period = period
        self.date_from = date_from
        self.tor_controller = None
        self.session = None
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        ]
        # self._init_tor_controller()
        # self._create_session()
        self.raw_data = pd.DataFrame(
            [i.get_full_data_dict() for i in list(set(self.parse(category, region, period, date_from)))])
        if to_excel:
            self.to_excel()

    def _init_tor_controller(self):
        """Initialize Tor controller connection"""
        try:
            self.tor_controller = Controller.from_port(port=9051)
            self.tor_controller.authenticate()
            print("Successfully connected to Tor ControlPort")
        except Exception as e:
            print(f"Error connecting to Tor ControlPort: {e}")
            self.tor_controller = None

    def _create_session(self):
        """Create new requests session with Tor proxy"""
        self.session = requests.Session()
        self.session.proxies = {
            'http': 'socks5h://localhost:9050',
            'https': 'socks5h://localhost:9050'
        }
        self.session.headers.update({
            'User-Agent': random.choice(self.user_agents),
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7'
        })

    def _renew_tor_connection(self):
        """Renew Tor IP address with verification"""
        if not self.tor_controller:
            print("Tor ControlPort not available - cannot renew IP")
            return False

        try:
            old_ip = self._get_current_ip()
            print(f"Current IP: {old_ip}")

            self.tor_controller.signal(Signal.NEWNYM)
            time.sleep(random.uniform(10, 20))  # Random delay between 10-20 seconds

            new_ip = self._get_current_ip()
            if new_ip and new_ip != old_ip:
                print(f"Successfully changed IP to: {new_ip}")
                self._create_session()  # Create fresh session with new IP
                return True
            else:
                print("IP did not change after renewal")
                return False
        except Exception as e:
            print(f"Error changing Tor circuit: {e}")
            return False

    def _get_current_ip(self):
        """Get current external IP through Tor"""
        try:
            with requests.Session() as temp_session:
                temp_session.proxies = {
                    'http': 'socks5h://localhost:9050',
                    'https': 'socks5h://localhost:9050'
                }
                response = temp_session.get('https://api.ipify.org?format=json', timeout=30)
                return response.json().get('ip') if response.status_code == 200 else None
        except Exception as e:
            print(f"Error getting current IP: {e}")
            return None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=10, max=30),
        retry=retry_if_exception_type((requests.RequestException,))
    )
    def parse(self, category: str, region: str, period: str, date_from: datetime) -> list[NewsItem]:
        """Parse Google search results with Tor"""
        search_params = {
            "num_results": settings.GOOGLE_SEARCH_LIMIT * len(settings.CATEGORIES_SEARCH[category]),
            "lang": "ru",
            "region": "ru",
            "advanced": True,
            "sleep_interval": random.uniform(1, 2),  # Random delay between queries
            # "proxy": 'socks5h://localhost:9050'
        }

        print(f'GOOGLE SCRAPING {category}, {region}, {period}, {date_from}')
        # self._renew_tor_connection()
        news_items = []

        # for category_query in settings.CATEGORIES_SEARCH[category]:
        #
        category_query = f'({" OR ".join([i for i in settings.CATEGORIES_SEARCH[category]])})'
        query = f'{region} {period} after:{date_from.strftime("%Y-%m-%d")} {category_query}'
        print(f'    QUERY: {query}')

        try:

            for obj in search(query, **search_params):
                news_items.append(
                    NewsItem(
                        category=category,
                        region=region,
                        period=period,
                        source=self.class_name,
                        url=obj.url,
                        approved=any(source in obj.url.lower() for source in settings.TRUSTED_WEB_SOURCE),
                        title=obj.title,
                        date_from=date_from
                    )
                )
                # Добавляем случайную задержку между разными категориями
            time.sleep(random.uniform(5, 10))
            # print(self._get_current_ip())

        except Exception as e:
            print(f"Error processing query '{query}': {e}")
            # self._renew_tor_connection()
            # continue

        return news_items

    def to_excel(self):
        filepath = os.path.join(settings.OUTPUT_DIR_PROCESSED,
                                f"{self.class_name}_{self.category}_{self.region}_{self.period}_{self.date_from}.xlsx")
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            self.raw_data.to_excel(writer, index=False)
            print(f"Data saved to: {filepath}")
            self.print_statistics()

    def print_statistics(self):
        total = len(self.raw_data)
        verified = len(self.raw_data[self.raw_data["approved"]])
        print(f"Total unique sources collected: {total}")
        print(f"Verified sources: {verified} ({verified / total:.1%})")
