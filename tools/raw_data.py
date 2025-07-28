from datetime import datetime, timezone, timedelta, date
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
import requests
import chardet

from config.settings import settings
from parsers.google_parser import GoogleParser
from parsers.tavily_parser import TavilyParser
from parsers.telegram_parser import TelegramParser
from parsers.website_parser import WebsiteParser
from tools.normalize_data import identification_region, clean_text


# def get_full_page_text_by_url(url):
#     """Получаем весь текст из body страницы"""
#     print(f'Собирается весь текст из body страницы {url}')
#
#     try:
#         headers = {
#             'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
#             'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
#             'Accept-Language': 'en-US,en;q=0.5',
#             'Accept-Encoding': 'gzip, deflate, br',
#             'Connection': 'keep-alive'
#         }
#
#         # Добавляем таймауты и обработку ошибок
#         response = requests.get(url, headers=headers, timeout=(10, 15), allow_redirects=True)
#         response.raise_for_status()
#
#         encoding = chardet.detect(response.content)['encoding']
#         html = response.content.decode(encoding)
#
#         soup = BeautifulSoup(html, 'html.parser')
#
#         # Удаляем ненужные элементы перед извлечением текста
#         for element in soup(
#                 ['script', 'style', 'nav', 'footer', 'iframe', 'noscript', 'svg', 'img', 'button', 'form']):
#             element.decompose()
#
#         # Получаем весь текст из body
#         body = soup.find('body')
#         if not body:
#             return "Не удалось найти body на странице"
#
#         full_text = body.get_text(separator='\n', strip=True)
#         return clean_text(full_text)
#
#     except Exception as e:
#         return f"Ошибка при загрузке страницы: {str(e)}"


# def fill_raw_data_html(df: pd.DataFrame):
#     # subset = ['url', 'region', 'category', 'period', 'date_from', 'approved', 'raw_data']
#     df = df.drop_duplicates()
#
#     # Находим индексы строк, где нужно собрать данные
#     mask = (df['raw_data'].isna()) | (df['raw_data'] == '')
#
#     parser = UniversalParser()
#     # Применяем функцию сбора данных только к этим строкам
#     if mask.any():
#         df.loc[mask, 'raw_data'] = df.loc[mask, 'url'].apply(get_full_page_text_by_url)
#
#     return df

def fill_raw_data_html(df: pd.DataFrame) -> pd.DataFrame:
    """
    Заполняет недостающие raw_data HTML-контентом страниц, используя UniversalParser

    Параметры:
        df: DataFrame с колонками ['url', 'raw_data', ...]

    Возвращает:
        Обновленный DataFrame с заполненными raw_data
    """
    # Удаляем дубликаты
    df = df.drop_duplicates()

    # Создаем экземпляр парсера один раз для всех запросов
    parser = WebsiteParser()

    def parse_url(url: str) -> str:
        """Вспомогательная функция для парсинга URL"""
        try:
            content = parser.parse(url)
            return content if content else ""
        except Exception as e:
            print(f"Ошибка при парсинге {url}: {str(e)}")
            return ""

    # Находим строки, где нужно собрать данные
    mask = (df['raw_data'].isna()) | (df['raw_data'] == '')

    # Применяем парсер только к этим строкам
    if mask.any():
        # Используем progress_apply для отображения прогресса (если установлен tqdm)
        try:
            from tqdm import tqdm
            tqdm.pandas(desc="Парсинг URL")
            df.loc[mask, 'raw_data'] = df.loc[mask, 'url'].progress_apply(parse_url)
        except ImportError:
            df.loc[mask, 'raw_data'] = df.loc[mask, 'url'].apply(parse_url)

    return df


def get_raw_data(sources: list[str],
                 category: str,
                 region: str,
                 period: str,
                 to_excel: bool,
                 month_begin: datetime = date.today().replace(day=1),
                 month_begin_utc: datetime = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0,
                                                                                microsecond=0)) -> pd.DataFrame:
    print(f'**** СБОР СЫРЫХ ДАННЫХ ****')

    global new_data
    fields = {
        # 'source': pd.Series(dtype='str'),
        'region': pd.Series(dtype='str'),
        'category': pd.Series(dtype='str'),
        'period': pd.Series(dtype='str'),
        'month_begin': pd.Series(dtype='datetime64[ns]'),
        'approved': pd.Series(dtype='bool')
    }
    full_data = pd.DataFrame(fields)
    for source in sources:

        match source:
            case 'Google':
                new_data = GoogleParser(category,
                                        region,
                                        period,
                                        month_begin,
                                        to_excel
                                        ).raw_data
            case 'Tavily':
                new_data = TavilyParser(category,
                                        region,
                                        period,
                                        month_begin,
                                        to_excel).raw_data
            case 'Telegram':

                dir = Path(settings.OUTPUT_DIR_PROCESSED)

                # Ищем файл с нужным паттерном в названии
                matching_files = [
                    f for f in dir.iterdir()
                    if f.is_file() and f"Telegram_{category}_BASE_{period}_{month_begin_utc}.xlsx" in f.name
                ]

                if matching_files:
                    # Берем первый найденный файл (если их несколько)
                    print(f'TELEGRAM SCRAPING')
                    print(f'Файл {matching_files[0]} найден!')
                    new_data = pd.read_excel(matching_files[0])
                else:
                    new_data = TelegramParser(category,
                                              region,
                                              period,
                                              month_begin_utc,
                                              to_excel).raw_data

                new_data = identification_region(region, new_data)
                new_data = new_data.loc[new_data['region'] == region]
                print(f'Размер данных: {len(new_data)}')

        new_data = new_data[['url', 'region', 'category', 'period', 'date_from', 'approved', 'raw_data']]
        full_data = pd.concat([full_data, new_data])
        print()
    print(f'**** ПАРСИНГ ДАННЫХ С САЙТОВ ****')
    full_data = fill_raw_data_html(full_data)

    print(f'Сырые данные из источников {sources} собраны. Размер данных {len(full_data)}')

    return full_data
