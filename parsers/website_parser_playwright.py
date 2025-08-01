import asyncio
import pandas as pd
from typing import List, Optional
from tqdm.asyncio import tqdm_asyncio
from bs4 import BeautifulSoup
import re
from playwright.async_api import async_playwright
import random


class WebsiteParser:
    def __init__(self, headless: bool = True, timeout: int = 30000):
        self.headless = headless
        self.timeout = timeout
        self.browser = None
        self.context = None
        self.playwright = None

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def start(self):
        """Инициализация Playwright"""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--start-maximized"
            ]
        )
        self.context = await self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent=self._generate_user_agent()
        )

    async def close(self):
        """Закрытие ресурсов Playwright"""
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    def _generate_user_agent(self) -> str:
        """Генерация случайного User-Agent"""
        agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0"
        ]
        return random.choice(agents)

    async def parse(self, url: str) -> Optional[str]:
        """Асинхронный парсинг страницы"""
        page = None
        try:
            page = await self.context.new_page()

            # Настройка stealth-режима
            await page.add_init_script("""
                delete navigator.__proto__.webdriver;
                Object.defineProperty(navigator, 'webdriver', { get: () => false });
            """)

            # Переход на страницу с таймаутом
            await page.goto(url, timeout=self.timeout, wait_until="domcontentloaded")

            # Эмуляция человеческого поведения
            await self._simulate_human_behavior(page)

            # Получение контента страницы
            content = await page.content()
            return self._clean_content(content)

        except Exception as e:
            print(f"Ошибка при парсинге {url}: {str(e)}")
            return None
        finally:
            if page:
                await page.close()

    async def _simulate_human_behavior(self, page):
        """Эмуляция человеческого поведения"""
        # Прокрутка страницы
        viewport_size = await page.evaluate("""() => ({
            width: window.innerWidth,
            height: window.innerHeight,
            scrollHeight: document.body.scrollHeight
        })""")

        scroll_positions = [
            viewport_size['scrollHeight'] * 0.3,
            viewport_size['scrollHeight'] * 0.6,
            viewport_size['scrollHeight']
        ]

        for position in scroll_positions:
            await page.evaluate(f"(y) => window.scrollTo(0, {position})")
            await asyncio.sleep(random.uniform(0.5, 1.5))

        # Случайные движения мыши
        await page.mouse.move(
            random.randint(100, viewport_size['width'] - 100),
            random.randint(100, viewport_size['height'] - 100)
        )
        await asyncio.sleep(random.uniform(0.2, 0.7))

    def _clean_content(self, html: str) -> str:
        """Очистка HTML контента"""
        soup = BeautifulSoup(html, 'html.parser')

        # Удаление ненужных элементов
        for element in soup(['script', 'style', 'nav', 'footer', 'iframe',
                             'noscript', 'svg', 'img', 'button', 'form',
                             'header', 'aside', 'figure', 'video', 'audio',
                             'link', 'meta', 'select']):
            element.decompose()

        # Извлечение текста
        body = soup.find('body') or soup
        text = body.get_text(separator='\n', strip=True)

        # Очистка текста
        cleaned_lines = []
        for line in text.splitlines():
            if line.strip():
                # Удаление управляющих символов
                cleaned_line = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', line.strip())
                cleaned_line = re.sub(r'[\u2028-\u202f]', '', cleaned_line)
                cleaned_line = cleaned_line[:32767]  # Ограничение Excel
                cleaned_lines.append(cleaned_line)

        return '\n'.join(cleaned_lines)


async def parse_urls_batch(urls: List[str], max_concurrent: int = 5) -> List[Optional[str]]:
    """
    Парсинг списка URL с ограничением на количество одновременных запросов

    Параметры:
        urls: список URL для парсинга
        max_concurrent: максимальное количество одновременных запросов

    Возвращает:
        Список результатов парсинга (может содержать None при ошибках)
    """
    semaphore = asyncio.Semaphore(max_concurrent)

    async def limited_parse(url: str, parser: WebsiteParser) -> Optional[str]:
        async with semaphore:
            return await parser.parse(url)

    async with WebsiteParser() as parser:
        tasks = [limited_parse(url, parser) for url in urls]
        return await tqdm_asyncio.gather(*tasks, desc="Парсинг URL")


async def fill_raw_data_html_async(df: pd.DataFrame, max_concurrent: int = 5) -> pd.DataFrame:
    """
    Асинхронное заполнение raw_data HTML-контентом

    Параметры:
        df: DataFrame с колонками ['url', 'raw_data', ...]
        max_concurrent: максимальное количество одновременных запросов

    Возвращает:
        Обновленный DataFrame с заполненными raw_data
    """
    df = df.drop_duplicates()

    # Находим строки для парсинга
    mask = (df['raw_data'].isna()) | (df['raw_data'] == '')
    urls_to_parse = df.loc[mask, 'url'].tolist()

    if urls_to_parse:
        # Парсим URL асинхронно
        parsed_contents = await parse_urls_batch(urls_to_parse, max_concurrent)

        # Обновляем DataFrame
        df.loc[mask, 'raw_data'] = parsed_contents

    return df


def fill_raw_data_html(df: pd.DataFrame, max_concurrent: int = 5) -> pd.DataFrame:
    """
    Синхронная обертка для асинхронного парсинга

    Параметры:
        df: DataFrame с колонками ['url', 'raw_data', ...]
        max_concurrent: максимальное количество одновременных запросов

    Возвращает:
        Обновленный DataFrame с заполненными raw_data
    """
    return asyncio.run(fill_raw_data_html_async(df, max_concurrent))