import datetime
import os
from googlesearch import search
import pandas as pd

from parsers.base_parser import BaseParser
from models import NewsItem
from config.settings import settings


class GoogleParser(BaseParser):

    def __init__(self, category: str, region: str, period:str, date_from: datetime, to_excel: bool):
        super().__init__()
        self.class_name = 'Google'
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

    def parse(self, category: str, region: str, period: str, date_from: datetime) -> list[NewsItem]:
        """Реализация парсинга с помощью Google Search"""

        search_params = {
            "num_results": settings.GOOGLE_SEARCH_LIMIT,
            "lang": "ru",
            "advanced": True
        }
        print(f'GOOGLE SCRAPING {category, region, period, date_from, search_params}')

        news_items = []
        # Формирование запроса
        for category_query in settings.CATEGORIES_SEARCH[category]:

            query = f'{category_query} {region} {period} after:{date_from.strftime("%Y-%m-%d")}'
            print(f'    QUERY: {query}')

            # Собранные новости
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
                        # content=self.get_full_page_text_by_url(obj.url)
                    )
                )
        return news_items

    def print_statistics(self):
        print(f'''Всего собрано уникальных источников {len(self.raw_data)}''')
        print(f'''Проверенные источники: {len(self.raw_data.loc[self.raw_data["approved"]])}''')
