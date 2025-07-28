import re
from datetime import datetime
from abc import ABC, abstractmethod

import requests
from bs4 import BeautifulSoup

from models import NewsItem
from tools.normalize_data import clean_text


class BaseParser(ABC):
    def __init__(self, authentication_keys: dict = None):
        self.authentication_keys = authentication_keys

    @abstractmethod
    def parse(self, category: str, region: str, period: str, date_from: datetime) -> list[NewsItem]:
        """Основной метод для парсинга данных из разных источников"""

    @abstractmethod
    def to_excel(self):
        """Метод позволяет сохранить результат выполнения парсинга в Excel"""
        pass

    @abstractmethod
    def print_statistics(self):
        """Метод выводит статистику по собранным данным"""
        pass

    @staticmethod
    def get_full_page_text_by_url(url):
        """Получаем весь текст из body страницы"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }

            # Добавляем таймауты и обработку ошибок
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            # Удаляем ненужные элементы перед извлечением текста
            for element in soup(
                    ['script', 'style', 'nav', 'footer', 'iframe', 'noscript', 'svg', 'img', 'button', 'form']):
                element.decompose()

            # Получаем весь текст из body
            body = soup.find('body')
            if not body:
                return "Не удалось найти body на странице"

            full_text = body.get_text(separator='\n', strip=True)
            return clean_text(full_text)

        except Exception as e:
            return f"Ошибка при загрузке страницы: {str(e)}"
