import os
import asyncio
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from telethon.sync import TelegramClient
from telethon.errors import FloodWaitError, ChannelPrivateError
import pandas as pd

from parsers.base_parser import BaseParser
from models import NewsItem
from config.settings import settings
from tools.normalize_data import clean_text


class TelegramParser(BaseParser):
    def __init__(self, category: str, region: str, period: str, date_from: datetime, to_excel: bool):
        super().__init__()
        self.class_name = 'Telegram'
        self.category = category
        self.region = region
        self.period = period
        self.date_from = date_from
        # self.raw_data = self.parse(category, region, period, date_from)
        self.raw_data = pd.DataFrame([i.get_full_data_dict() for i in self.parse(category, region, period, date_from)])

        if to_excel:
            self.to_excel()

    def to_excel(self):
        data_df = self.raw_data
        data_df['date_from'] = data_df['date_from'].dt.tz_localize(None)
        data_df['date_publish'] = data_df['date_publish'].dt.tz_localize(None)
        # data_df = data_df.loc[data_df['region'] == self.region]
        filepath = os.path.join(settings.OUTPUT_DIR_PROCESSED,
                                f"{self.class_name}_{self.category}_BASE_{self.period}_{self.date_from}.xlsx")
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            data_df.to_excel(writer, index=False)
            print(f"Данные сохранены в файл: {filepath}")
            self.print_statistics()

    async def _get_channel_messages(self, channel_username, period, date_from, region, category):
        load_dotenv()

        # Настройки клиента
        api_id = settings.AUTHENTICATION['TELEGRAM_API_ID']
        api_hash = settings.AUTHENTICATION['TELEGRAM_API_HASH']
        phone = settings.AUTHENTICATION['PHONE_NUM']
        session_name = os.getenv('SESSION_NAME', 'default_session')

        async with TelegramClient(session_name, api_id, api_hash) as client:
            await client.start(phone)

            try:
                channel = await client.get_entity(channel_username)

                messages = []
                async for message in client.iter_messages(channel):
                    if message.date > date_from:
                        if message.text:
                            messages.append(
                                NewsItem(
                                    # region=self._identification_region(region, message.text),
                                    category=category,
                                    region='Undefined',
                                    period=period,
                                    source=self.class_name,
                                    url=f"https://t.me/s/{channel_username}",
                                    approved=any(source in channel_username for source in settings.TELEGRAM_CHANNELS[category]['approved']),
                                    # title=self.get_title(message.text),
                                    date_from=date_from,
                                    date_publish=message.date,
                                    raw_data=clean_text(message.text)
                                ))
                    else:
                        break

                return messages

            except ChannelPrivateError:
                print(f"Ошибка: Канал {channel_username} приватный или у вас нет доступа.")
                return None
            except Exception as e:
                print(f"Ошибка в канале {channel_username}: {e}")
                return None

    async def _process_channels(self, channel_list, period, date_from, region, category):
        all_messages = []

        for channel in channel_list:
            print(f"    CHANNEL {channel}: ", end='')
            messages = await self._get_channel_messages(channel, period, date_from, region, category)

            if messages:
                all_messages.extend(messages)
                print(f"{len(messages)} сообщений")
            else:
                print(f"Не удалось получить сообщения из {channel}")

        return all_messages

    def parse(self, category: str, region: str, period: str, date_from: datetime) -> list[NewsItem]:
        """Реализация парсинга Telegram каналов"""
        print(f'TELEGRAM SCRAPING {category, region, period, date_from}')

        # Список каналов для обработки (можно вынести в настройки)
        channel_list = (settings.TELEGRAM_CHANNELS[category]['approved']
                        + settings.TELEGRAM_CHANNELS[category]['not approved'])

        return asyncio.run(self._process_channels(channel_list, period, date_from, region, category))

    def print_statistics(self):
        print(f'''Всего собрано уникальных сообщений по региону: {len(self.raw_data.loc[self.raw_data["approved"]])}''')

    # Возможно надо использовать embandings: векторизовать ключевые слова, векторизовать текст сообщения
    # и находить наиболее похожий вектор
    # @staticmethod
    # def _identification_region(region: str, text: str) -> str:
    #     """Проверяет наличие региона в тексте по ключевым словам из словаря."""
    #     if not text or not settings.REGION_KEYWORDS[region]:
    #         return ""
    #
    #     text_lower = text.lower()
    #
    #     # Проверяем все ключевые слова для текущего региона
    #     for keyword in settings.REGION_KEYWORDS[region]:
    #         if keyword.lower() in text_lower:
    #             return region
    #
    #     return ""

    # @staticmethod
    # def get_title(text):
    #     """Метод возвращает либо жирный текст, либо первое предложение"""
    #     if '**' in text:
    #         return [i for i in text.split('**') if i != ''][1]
    #     else:
    #         return text.split('.')[0]
