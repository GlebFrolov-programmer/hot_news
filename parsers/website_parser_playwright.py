# import asyncio
# import pandas as pd
# from typing import List, Optional
# from tqdm.asyncio import tqdm_asyncio
# from bs4 import BeautifulSoup
# import re
# from playwright.async_api import async_playwright
# import random
#
#
# class WebsiteParser:
#     def __init__(self, headless: bool = True, timeout: int = 15000):
#         self.headless = headless
#         self.timeout = timeout
#         self.browser = None
#         self.context = None
#         self.playwright = None
#
#     async def __aenter__(self):
#         await self.start()
#         return self
#
#     async def __aexit__(self, exc_type, exc_val, exc_tb):
#         await self.close()
#
#     async def start(self):
#         """Инициализация Playwright"""
#         self.playwright = await async_playwright().start()
#         self.browser = await self.playwright.chromium.launch(
#             headless=self.headless,
#             args=[
#                 "--disable-blink-features=AutomationControlled",
#                 "--start-maximized"
#             ]
#         )
#         self.context = await self.browser.new_context(
#             viewport={'width': 1920, 'height': 1080},
#             user_agent=self._generate_user_agent()
#         )
#
#     async def close(self):
#         """Закрытие ресурсов Playwright"""
#         if self.context:
#             await self.context.close()
#         if self.browser:
#             await self.browser.close()
#         if self.playwright:
#             await self.playwright.stop()
#
#     def _generate_user_agent(self) -> str:
#         """Генерация случайного User-Agent"""
#         agents = [
#             "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
#             "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
#             "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0"
#         ]
#         return random.choice(agents)
#
#     async def parse(self, url: str) -> Optional[str]:
#         """Асинхронный парсинг страницы"""
#         page = None
#         try:
#             # print(url)
#             page = await self.context.new_page()
#
#             # Настройка stealth-режима
#             await page.add_init_script("""
#                 delete navigator.__proto__.webdriver;
#                 Object.defineProperty(navigator, 'webdriver', { get: () => false });
#             """)
#
#             # Переход на страницу с таймаутом
#             await page.goto(url, timeout=self.timeout, wait_until="domcontentloaded")
#
#             # Эмуляция человеческого поведения
#             await self._simulate_human_behavior(page)
#
#             # Получение контента страницы
#             content = await page.content()
#             return self._clean_content(content)
#
#         except Exception as e:
#             print(f"Ошибка при парсинге {url}: {str(e)}")
#             return None
#         finally:
#             if page:
#                 await page.close()
#
#     async def _simulate_human_behavior(self, page):
#         """Эмуляция человеческого поведения"""
#         # Прокрутка страницы
#         viewport_size = await page.evaluate("""() => ({
#             width: window.innerWidth,
#             height: window.innerHeight,
#             scrollHeight: document.body.scrollHeight
#         })""")
#
#         scroll_positions = [
#             viewport_size['scrollHeight'] * 0.3,
#             viewport_size['scrollHeight'] * 0.6,
#             viewport_size['scrollHeight']
#         ]
#
#         for position in scroll_positions:
#             await page.evaluate(f"(y) => window.scrollTo(0, {position})")
#             await asyncio.sleep(random.uniform(0.5, 1.5))
#
#         # Случайные движения мыши
#         await page.mouse.move(
#             random.randint(100, viewport_size['width'] - 100),
#             random.randint(100, viewport_size['height'] - 100)
#         )
#         await asyncio.sleep(random.uniform(0.2, 0.7))
#
#
#     @staticmethod
#     def _remove_sensitive_and_urls(text: str) -> str:
#         # Удаление полных URL с популярными доменами и протоколами
#         url_pattern = r'''(?i)\b((?:https?://|ftp://|www\d{0,3}[.]|[a-z0-9.\-]+[.](?:com|ru|org|net|info|biz|gov|edu|mil|co|io|me|tv|online|site|shop|xyz|top|club|space|tech|ai|app|dev))
#                           (?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))*
#                           (?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'".,<>?«»“”‘’]))'''
#         text = re.sub(url_pattern, '', text, flags=re.VERBOSE)
#
#         # Удаление "разбитых" URL с пробелами
#         broken_url_pattern = r'https?:\s*[:/\\]*\s*[\w\.-]+(?:\s+[\w\./-]+)*'
#         text = re.sub(broken_url_pattern, '', text, flags=re.IGNORECASE)
#
#         # Удаление отдельных частей ссылок и популярных доменов
#         fragments_pattern = r'\b(?:http|https|ftp|www|://|\.com|\.ru|\.org|\.net|\.info|\.biz|\.gov|\.edu|\.mil|\.co|\.io|\.me|\.tv|\.online|\.site|\.shop|\.xyz|\.top|\.club|\.space|\.tech|\.ai|\.app|\.dev)\b'
#         text = re.sub(fragments_pattern, '', text, flags=re.IGNORECASE)
#
#         # Удаление слов, которые могут указывать на персональные или финансовые данные
#         sensitive_words_pattern = r'\b(ИНН|БИК|ОГРН|Паспорт|СНИЛС|КПП|Код\s*подразделения|Рассчётный\s*счёт|Карта|Телефон|Email|E-mail)\b'
#         text = re.sub(sensitive_words_pattern, '', text, flags=re.IGNORECASE)
#
#         # Очистка лишних пробелов и табов
#         text = re.sub(r'\s{2,}', ' ', text).strip()
#
#         return text
#
#     def _clean_content(self, html: str) -> str:
#         """Очистка HTML контента"""
#         soup = BeautifulSoup(html, 'html.parser')
#
#         # Удаление ненужных элементов
#         for element in soup(['script', 'style', 'nav', 'footer', 'iframe',
#                              'noscript', 'svg', 'img', 'button', 'form',
#                              'header', 'aside', 'figure', 'video', 'audio',
#                              'link', 'meta', 'select']):
#             element.decompose()
#
#         # Извлечение текста
#         body = soup.find('body') or soup
#         text = body.get_text(separator='\n', strip=True)
#
#         # Очистка текста
#         cleaned_lines = []
#         for line in text.splitlines():
#             if line.strip():
#                 # Удаление управляющих символов
#                 cleaned_line = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', line.strip())
#                 cleaned_line = re.sub(r'[\u2028-\u202f]', '', cleaned_line)
#                 cleaned_line = cleaned_line[:32767]  # Ограничение Excel
#                 cleaned_lines.append(cleaned_line)
#
#         clean_text = self._remove_sensitive_and_urls('\n'.join(cleaned_lines))
#         return clean_text
#
#
# async def parse_urls_batch(urls: List[str], max_concurrent: int = 5) -> List[Optional[str]]:
#     """
#     Парсинг списка URL с ограничением на количество одновременных запросов
#
#     Параметры:
#         urls: список URL для парсинга
#         max_concurrent: максимальное количество одновременных запросов
#
#     Возвращает:
#         Список результатов парсинга (может содержать None при ошибках)
#     """
#     semaphore = asyncio.Semaphore(max_concurrent)
#
#     async def limited_parse(url: str, parser: WebsiteParser) -> Optional[str]:
#         async with semaphore:
#             return await parser.parse(url)
#
#     async with WebsiteParser() as parser:
#         tasks = [limited_parse(url, parser) for url in urls]
#         return await tqdm_asyncio.gather(*tasks, desc="Парсинг URL")
#
#
# async def fill_raw_data_html_async(df: pd.DataFrame, max_concurrent: int = 5) -> pd.DataFrame:
#     """
#     Асинхронное заполнение raw_data HTML-контентом
#
#     Параметры:
#         df: DataFrame с колонками ['url', 'raw_data', ...]
#         max_concurrent: максимальное количество одновременных запросов
#
#     Возвращает:
#         Обновленный DataFrame с заполненными raw_data
#     """
#     df = df.drop_duplicates()
#
#     # Находим строки для парсинга
#     mask = (df['raw_data'].isna()) | (df['raw_data'] == '')
#     urls_to_parse = df.loc[mask, 'url'].tolist()
#
#     if urls_to_parse:
#         # Парсим URL асинхронно
#         parsed_contents = await parse_urls_batch(urls_to_parse, max_concurrent)
#
#         # Обновляем DataFrame
#         df.loc[mask, 'raw_data'] = parsed_contents
#
#     return df
#
#
# def fill_raw_data_html(df: pd.DataFrame, max_concurrent: int = 5) -> pd.DataFrame:
#     """
#     Синхронная обертка для асинхронного парсинга
#
#     Параметры:
#         df: DataFrame с колонками ['url', 'raw_data', ...]
#         max_concurrent: максимальное количество одновременных запросов
#
#     Возвращает:
#         Обновленный DataFrame с заполненными raw_data
#     """
#     return asyncio.run(fill_raw_data_html_async(df, max_concurrent))


import asyncio
import pandas as pd
from typing import List, Optional
from tqdm.asyncio import tqdm_asyncio
from bs4 import BeautifulSoup
import re
from playwright.async_api import async_playwright
import random
import async_timeout


class WebsiteParser:
    def __init__(self, headless: bool = True, timeout: int = 10000, process_timeout: int = 15000):
        self.headless = headless
        self.timeout = timeout  # Таймаут для page.goto
        self.process_timeout = process_timeout  # Общий таймаут для всего парсинга
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
        """Асинхронный парсинг страницы с жестким таймаутом"""
        page = None
        try:
            # Жесткий таймаут на всю операцию парсинга
            async with async_timeout.timeout(self.process_timeout / 1000):
                page = await self.context.new_page()

                # Устанавливаем таймаут на навигацию
                await page.goto(url, timeout=self.timeout, wait_until="domcontentloaded")

                # Минимальная эмуляция поведения
                await self._minimal_behavior(page)

                # Быстрое получение контента
                content = await page.content()
                return self._clean_content(content)

        except asyncio.TimeoutError:
            # print(f"Таймаут парсинга: {url}")
            return None
        except Exception:
            # Игнорируем все остальные ошибки
            return None
        finally:
            if page:
                try:
                    await page.close()
                except:
                    pass

    async def _minimal_behavior(self, page):
        """Минимальная эмуляция поведения"""
        try:
            # Только быстрая прокрутка
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(0.3)
        except:
            pass

    @staticmethod
    def _remove_sensitive_and_urls(text: str) -> str:
        # Упрощенная очистка текста
        url_pattern = r'https?://\S+|www\.\S+'
        text = re.sub(url_pattern, '', text, flags=re.IGNORECASE)

        sensitive_words_pattern = r'\b(ИНН|БИК|ОГРН|Паспорт|СНИЛС|КПП|Карта|Телефон|Email)\b'
        text = re.sub(sensitive_words_pattern, '', text, flags=re.IGNORECASE)

        return re.sub(r'\s+', ' ', text).strip()

    def _clean_content(self, html: str) -> str:
        """Быстрая очистка HTML контента"""
        try:
            soup = BeautifulSoup(html, 'html.parser')

            # Удаляем только самые критичные элементы
            for element in soup(['script', 'style', 'nav', 'footer']):
                element.decompose()

            text = soup.get_text(separator='\n', strip=True)
            cleaned_lines = []

            for line in text.splitlines():
                if line.strip() and len(line.strip()) > 10:  # Фильтруем очень короткие строки
                    cleaned_line = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', line.strip())
                    cleaned_lines.append(cleaned_line[:10000])  # Ограничение длины

            clean_text = self._remove_sensitive_and_urls('\n'.join(cleaned_lines))
            return clean_text[:50000]  # Общее ограничение

        except Exception:
            return ""


async def parse_single_url_with_timeout(url: str, parser: WebsiteParser, timeout: int) -> Optional[str]:
    """Парсинг одного URL с гарантированным таймаутом"""
    try:
        # Создаем задачу с таймаутом
        return await asyncio.wait_for(parser.parse(url), timeout=timeout / 1000)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        return None
    except Exception:
        return None


async def parse_urls_batch(urls: List[str], max_concurrent: int = 5, process_timeout: int = 15000) -> List[
    Optional[str]]:
    """
    Парсинг списка URL с жестким ограничением времени
    """
    results = [None] * len(urls)  # Предзаполняем None

    async with WebsiteParser(process_timeout=process_timeout) as parser:
        semaphore = asyncio.Semaphore(max_concurrent)

        async def process_url(index: int, url: str):
            async with semaphore:
                results[index] = await parse_single_url_with_timeout(url, parser, process_timeout)

        # Создаем и запускаем все задачи сразу
        tasks = []
        for i, url in enumerate(urls):
            task = asyncio.create_task(process_url(i, url))
            tasks.append(task)

        # Ждем завершения всех задач с прогресс-баром
        for f in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Парсинг URL"):
            try:
                await f
            except Exception:
                continue

    return results


async def fill_raw_data_html_async(df: pd.DataFrame, max_concurrent: int = 5,
                                   process_timeout: int = 15000) -> pd.DataFrame:
    """
    Асинхронное заполнение raw_data HTML-контентом
    """
    df = df.drop_duplicates()

    mask = (df['raw_data'].isna()) | (df['raw_data'] == '')
    urls_to_parse = df.loc[mask, 'url'].tolist()

    if urls_to_parse:
        print(f"Найдено {len(urls_to_parse)} URL для парсинга")
        parsed_contents = await parse_urls_batch(urls_to_parse, max_concurrent, process_timeout)
        df.loc[mask, 'raw_data'] = parsed_contents

    return df


def fill_raw_data_html(df: pd.DataFrame, max_concurrent: int = 5, process_timeout: int = 15000) -> pd.DataFrame:
    """
    Синхронная обертка для асинхронного парсинга
    """
    return asyncio.run(fill_raw_data_html_async(df, max_concurrent, process_timeout))