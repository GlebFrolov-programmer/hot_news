import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


class Settings:
    # Авторизационные данные API

    AUTHENTICATION = {
        'GIGACHAT_API_AUTH': os.getenv("GIGACHAT_API_AUTH"),
        'TAVILY_API_KEY': os.getenv("TAVILY_API_KEY"),
        'TELEGRAM_API_ID': os.getenv("TELEGRAM_API_ID"),
        'TELEGRAM_API_HASH': os.getenv("TELEGRAM_API_HASH"),
        'PHONE_NUM': os.getenv("PHONE_NUM"),
        'OPENROUTER_AI_MODEL': os.getenv("OPENROUTER_AI_MODEL"),
        'TOGETHER_API_KEY': os.getenv("TOGETHER_API_KEY")
    }

    # Настройки парсеров
    GOOGLE_SEARCH_LIMIT = 5
    TAVILY_LIMIT = 20

    TRUSTED_WEB_SOURCE = ["ria.ru", "tass.ru", "rbc.ru", "vedomosti.ru", "kommersant.ru", "rg.ru", 'realty.rbc.ru',
                          'gipernn.ru', 'pravda-nn.ru', 'vremyan.ru', 'government-nnov.ru', 'nn.rbc.ru', 'domostroynn.ru']
    AVAILABLE_SOURCES = ['Telegram', 'Google', 'Tavily']

    REGION_KEYWORDS = {
        "Нижегородская область": ["нижний новгород", "нижегород", "н.новгород"],
        "Москва": ["москва", "мск", "московский"],
        "Санкт-Петербург": ["санкт-петербург", "питер", "спб", "петербург"],
        "Краснодарский край": ["краснодар", "краснодарский", "крд", "кубань"]
    }
    CATEGORIES_SEARCH = {
        'Недвижимость': [
                         'Обзор рынка недвижимости',
                         'Цены на недвижимость',
                         'Спрос и предложение на рынке недвижимости',
                         'Новости недвижимости',
                         'Анализ рынка недвижимости',
                         'Факторы изменения цены',
                         'Почему изменилась цена на недвижимость',
                         'Первичное жильё',
                         'Вторичное жильё',
                         'Элитная недвижимость',
                         'Новостройки и застройщики',
                         'Регулирование и законы в недвижимости',
                         'Тенденции и тренды рынка недвижимости'
                         ]
    }
    TELEGRAM_CHANNELS = {
        'Недвижимость':
                        {
                            'approved': [
                                         "russianmacro",
                                         "domresearch",
                                         "okoloCB",
                                         "domclick",
                                         "ria_realty",
                                         "realty_rbc",
                                         ],

                            'not approved': [
                                            "Jelezobetonniyzames",
                                            "kvadratnymaster",
                                            "nedvizha",
                                            "belaya_kaska",
                                            "propertyinsider",
                                            "cian_realtor",
                                            "avito_re_pro",
                                            "ipotekahouse",
                                            "filatofff",
                                            "pro_smarent",
                                            "Leonid_Rysev",
                                            "pataninnews",
                                            "rudakov_broker",
                                            ]
                        }

    }
    # Настройки хранения
    OUTPUT_DIR = "outputs_data"
    # OUTPUT_ABS_DIR = os.path.abspath(OUTPUT_DIR)
    OUTPUT_ABS_DIR = Path(__file__).resolve().parent.parent
    OUTPUT_DIR_PROCESSED = os.path.join(OUTPUT_ABS_DIR, OUTPUT_DIR, "processed")
    OUTPUT_DIR_RAW = os.path.join(OUTPUT_ABS_DIR, OUTPUT_DIR, "raw")
    OUTPUT_DIR_TOPICS = os.path.join(OUTPUT_ABS_DIR, OUTPUT_DIR, "topics")
    OUTPUT_DIR_CLUSTERS = os.path.join(OUTPUT_ABS_DIR, OUTPUT_DIR, "clusters")


settings = Settings()
