import datetime
import os
import pandas as pd
import requests
from tavily import TavilyClient

from parsers.base_parser import BaseParser
from models import NewsItem
from config.settings import settings
from tenacity import retry, stop_after_attempt, wait_exponential


class TavilyParser(BaseParser):

    def __init__(self, category: str, region: str, period:str, date_from: datetime, to_excel: bool):
        super().__init__()
        self.tavily_client = TavilyClient(api_key=settings.AUTHENTICATION['TAVILY_API_KEY'])
        self.class_name = 'Tavily'
        self.category = category
        self.region = region
        self.period = period
        self.date_from = date_from
        self.raw_data = pd.DataFrame([i.get_full_data_dict() for i in list(set(self.parse(category, region, period, date_from)))])
        if to_excel:
            self.to_excel()

    def to_excel(self):
        # data_df = pd.DataFrame([i.get_full_data_dict() for i in self.raw_data])
        filepath = os.path.join(settings.OUTPUT_DIR_PROCESSED,
                                f"{self.class_name}_{self.category}_{self.region}_{self.period}_{self.date_from}.xlsx")
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            self.raw_data.to_excel(writer, index=False)

            print(f"Данные сохранены в файл: {filepath}")
            self.print_statistics()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def parse(self, category: str, region: str, period: str, date_from: datetime) -> list[NewsItem]:
        """Реализация парсинга с помощью Tavily"""
        print(f'TAVILY SCRAPING {category, region, period}')

        news_items = []
        # Формирование запроса
        filter_categories = []
        for cat in settings.CATEGORIES_SEARCH[category]:
            if len(f'{" OR ".join(filter_categories + [cat])} {region} {period}') <= 400:
                filter_categories.append(cat)
            else:
                break
        categories_query = f'({" OR ".join([i for i in filter_categories])})'

        query = f'{categories_query} {region} {period}'
        print(f'    QUERY: {query}')
        try:
            raw_data = self.tavily_client.search(
                    query=query,
                    search_depth="advanced",
                    include_answer=True,
                    max_results=settings.TAVILY_LIMIT,
                )
        except requests.exceptions.ConnectionError:
            print("Connection failed, retrying...")
            raise

        for result in raw_data['results']:
            # Собранные новости
            news_items.append(
                        NewsItem(
                            category=category,
                            region=region,
                            period=period,
                            source=self.class_name,
                            url=result['url'],
                            approved=any(source in result['url'].lower() for source in settings.TRUSTED_WEB_SOURCE),
                            title=result['title'],
                            date_from=date_from
                    )
                )
        return news_items

    def print_statistics(self):
        # print(f'''Всего собрано уникальных источников {len(self.raw_data)}''')
        # print(f'''Проверенные источники: {len(self.raw_data.loc[self.raw_data["approved"]])}''')
        total = len(self.raw_data)
        verified = len(self.raw_data[self.raw_data["approved"]])
        print(f"Всего собрано уникальных источников: {total}")
        print(f"Проверенные источники: {verified} ({verified / total:.1%})")