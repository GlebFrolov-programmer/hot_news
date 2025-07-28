from datetime import datetime
from dataclasses import dataclass
from typing import Optional

from config.settings import settings


@dataclass
class NewsItem:
    category: str  # Категория
    region: str  # Регион
    period: str  # За какой период надо найти данные
    source: str  # Источник данных (Telegram, Google, Tavily)
    url: str  # Ссылка на источник с данными
    approved: bool  # Проверенный ли источник информации
    title: str = None # Заголовок новости
    content: str = None  # Очищенный контент от ненужных данных
    date_publish: datetime = None  # Дата публикации источника (без UTC)
    date_from: datetime = None  # Дата начала актульаности данных (без UTC)
    raw_data: Optional[str] = None  # Сырые данные без обработки

    def get_full_data_dict(self):
        return {
            "category": self.category,
            "region": self.region,
            "period": self.period,
            "source": self.source,
            "url": self.url,
            "approved": self.approved,
            "title": self.title,
            "raw_data": self.raw_data,
            "content": self.content,
            "date_publish": self.date_publish,
            "date_from": self.date_from,
        }

    def get_scraped_data_dict(self):
        return {
            "source": self.source,
            "categoy": self.category,
            "region": self.region,
            "period": self.period,
            "content": self.content,
            "approved": self.approved,
            "date_from": self.date_from,
        }

    # @staticmethod
    def identification_region(self, region: str) -> None:
        """Проверяет наличие региона в тексте по ключевым словам и устанавливает self.region при нахождении."""
        if not self.raw_data or not settings.REGION_KEYWORDS.get(region):
            self.region = ""
            return

        text_lower = self.raw_data.lower()

        # Проверяем все ключевые слова для текущего региона
        for keyword in settings.REGION_KEYWORDS[region]:
            if keyword.lower() in text_lower:
                self.region = region
                return

        self.region = ""

    # def __eq__(self, other):
    #     if isinstance(other, NewsItem):
    #         return self.url == other.url
    #     return False

    def __hash__(self):
        return hash(self.url)
