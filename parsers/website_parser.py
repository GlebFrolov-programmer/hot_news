import re

import requests
from bs4 import BeautifulSoup
import chardet
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import random
import urllib3
import logging
from fake_useragent import UserAgent

# Настройка логирования
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)
# urllib3.disable_warnings()


class WebsiteParser:
    def __init__(self, headless=True, selenium_timeout=30):
        self.ua = UserAgent()
        self.selenium_timeout = selenium_timeout
        self.chrome_options = self._setup_chrome_options(headless)

    def _setup_chrome_options(self, headless):
        """Конфигурация Chrome для Selenium"""
        options = Options()
        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument(f"user-agent={self.ua.random}")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        return options

    def parse(self, url, force_selenium=False):
        """Основной метод парсинга с автоматическим выбором движка"""
        print(f'Собираются данные с сайта {url}')

        try:
            if not force_selenium:
                # Сначала пробуем легкий режим (Requests)
                content = self._parse_with_requests(url)

                # Проверяем результат на валидность
                if content and len(content) > 100:
                    # Дополнительная проверка на битую кодировку
                    if not self._has_broken_encoding(content):
                        return content

            print("Обнаружена битая кодировка или ошибка при парсинге, переключаемся на Selenium")

            # Если легкий режим не сработал или обнаружена битая кодировка - используем Selenium
            return self._parse_with_selenium(url)

        except Exception as e:
            print(f"Ошибка при парсинге {url}: {str(e)}")
            return None

    def _has_broken_encoding(self, text):
        """Проверяет текст на признаки битой кодировки"""
        # Проверка на replacement character (�)
        if '�' in text:
            return True

        # Проверка на нечитаемые последовательности (кириллица в utf-8)
        if re.search(r'[ÐÂðâÐð][\x80-\xBF]', text):
            return True

        # Проверка на странные сочетания символов
        unusual_chars = re.findall(r'[^\w\s.,!?@#$%^&*()_+-=;:\'"<>/\\|{}\[\]`~]', text)
        if len(unusual_chars) > len(text) * 0.1:  # Если больше 10% странных символов
            return True

        return False

    # def parse(self, url, force_selenium=False):
    #     """Основной метод парсинга с автоматическим выбором движка"""
    #     print(f'Собираются данные с сайта {url}')
    #     try:
    #         if not force_selenium:
    #             # Сначала пробуем легкий режим (Requests)
    #             content = self._parse_with_requests(url)
    #             if content and len(content) > 100:  # Проверяем, что контент не пустой
    #                 return content
    #         print('Применяется SELENIUM')
    #         # Если легкий режим не сработал - используем Selenium
    #         return self._parse_with_selenium(url)
    #
    #     except Exception as e:
    #         print(f"Ошибка при парсинге {url}: {str(e)}")
    #         return None

    # def _parse_with_requests(self, url):
    #     """Парсинг с помощью Requests + BeautifulSoup"""
    #     try:
    #         headers = {
    #             'User-Agent': self.ua.random,
    #             'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    #             'Accept-Language': 'en-US,en;q=0.5',
    #             'Referer': 'https://www.google.com/',
    #             'DNT': '1'
    #         }
    #
    #         response = requests.get(
    #             url,
    #             headers=headers,
    #             timeout=15,
    #             verify=False,
    #             allow_redirects=True
    #         )
    #
    #         if response.status_code == 200:
    #             soup = BeautifulSoup(response.text, 'html.parser')
    #             return self._clean_content(soup)
    #
    #     except Exception as e:
    #         print(f"Requests не сработал для {url}: {str(e)}")
    #         return None
    @staticmethod
    def fix_broken_encoding(text, possible_encodings=['utf-8', 'windows-1251', 'koi8-r']):
        for enc in possible_encodings:
            try:
                return text.encode(enc).decode('utf-8')
            except UnicodeError:
                continue
        return text

    def _parse_with_requests(self, url):
        """Парсинг страницы с автоматическим определением кодировки через chardet"""
        try:
            headers = {
                'User-Agent': self.ua.random,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Referer': 'https://www.google.com/',
                'DNT': '1'
            }

            # Выполняем HTTP-запрос
            response = requests.get(
                url,
                headers=headers,
                timeout=15,
                verify=False,
                allow_redirects=True
            )

            # Проверяем успешность запроса
            if response.status_code != 200:
                print(f"Страница вернула код {response.status_code}: {url}")
                return None

            # Определяем кодировку контента
            try:
                encoding = chardet.detect(response.content)['encoding']

                # Если chardet не смог определить кодировку, используем utf-8 как fallback
                # if not encoding:
                #     encoding = 'utf-8'
                #     logger.debug(f"Не удалось определить кодировку для {url}, используется utf-8")

                # Декодируем контент с определенной кодировкой
                html = response.content.decode(encoding, errors='replace')

                # Создаем BeautifulSoup объект
                soup = BeautifulSoup(html, 'html.parser')

                # Проверяем, что контент не пустой
                if not soup.find('body'):
                    print(f"Пустое тело страницы: {url}")
                    return None

                return self._clean_content(soup)

            except UnicodeDecodeError as e:
                print(f"Ошибка декодирования {url} (обнаруженная кодировка: {encoding}): {str(e)}")
                # Пробуем fallback кодировки
                for fallback_encoding in ['utf-8', 'windows-1251', 'cp1251']:
                    try:
                        html = response.content.decode(fallback_encoding)
                        soup = BeautifulSoup(html, 'html.parser')
                        return self._clean_content(soup)
                    except UnicodeDecodeError:
                        continue
                return None

        except requests.exceptions.RequestException as e:
            print(f"Ошибка запроса к {url}: {str(e)}")
            return None
        except Exception as e:
            print(f"Неожиданная ошибка при обработке {url}: {str(e)}", exc_info=True)
            return None

    def _parse_with_selenium(self, url):
        """Парсинг с помощью Selenium"""
        driver = None
        try:
            driver = webdriver.Chrome(options=self.chrome_options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            driver.get(url)

            # Ожидание загрузки контента
            WebDriverWait(driver, self.selenium_timeout).until(
                EC.presence_of_element_located((By.XPATH, "//body"))
            )

            # Дополнительное время для JS-рендеринга
            time.sleep(1.5)

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            return self._clean_content(soup)

        except Exception as e:
            print(f"Selenium ошибка для {url}: {str(e)}")
            return None

        finally:
            if driver:
                driver.quit()

    def _clean_content(self, soup):
        """Очистка HTML контента с удалением запрещенных символов для Excel"""
        # Удаляем ненужные HTML-элементы
        for element in soup(['script', 'style', 'nav', 'footer', 'iframe',
                             'noscript', 'svg', 'img', 'button', 'form',
                             'header', 'aside', 'figure', 'video', 'audio',
                             'link', 'meta', 'select']):
            element.decompose()

        # Извлекаем текст
        body = soup.find('body') or soup
        text = body.get_text(separator='\n', strip=True)

        text = self.fix_broken_encoding(text)

        # Очищаем и обрабатываем текст
        cleaned_lines = []
        for line in text.splitlines():
            if line.strip():
                # Удаляем управляющие символы (кроме \t, \n, \r)
                cleaned_line = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', line.strip())
                # Удаляем другие проблемные символы
                cleaned_line = re.sub(r'[\u2028-\u202f]', '', cleaned_line)
                # Обрезаем строку до 32767 символов (ограничение Excel)
                cleaned_line = cleaned_line[:32767]
                cleaned_lines.append(cleaned_line)

        return '\n'.join(cleaned_lines)
