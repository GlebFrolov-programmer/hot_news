import os
import time
from datetime import datetime, timezone, timedelta, date


from llm.gigachat_client import GigaChatHotNewsGenerator
import pandas as pd
from config.settings import settings

import warnings

from tools.raw_data import get_raw_data

warnings.filterwarnings("ignore")  # Отключает все warnings


start_time = time.time()
# regions = ['Нижегородская область']
# categories = ['Недвижимость']

sources = settings.AVAILABLE_SOURCES
regions = list(settings.REGION_KEYWORDS.keys())[:1]
categories = list(settings.CATEGORIES_SEARCH.keys())[:1]
period = 'Июнь 2025'
to_excel = True
month_begin = date.today().replace(day=1)

for region in regions:
    for category in categories:
        # Шаг 1. Подготовка сырых данных
        # raw_data = get_raw_data(
        #                         sources=sources,
        #                         category=category,
        #                         region=region,
        #                         period=period,
        #                         to_excel=to_excel)
        # raw_data.to_excel(os.path.join(settings.OUTPUT_DIR_RAW, f'RAW_{category}_{region}_{period}_{month_begin}.xlsx'), index=False)

        # data_topics = raw_data.copy()
        data_topics = pd.read_excel(os.path.join(settings.OUTPUT_DIR_RAW, f'RAW_{category}_{region}_{period}_{month_begin}.xlsx'))
        # Шаг 2. Генерация тем из текстов
        credentials = settings.AUTHENTICATION['GIGACHAT_API_AUTH']
        model = "GigaChat"
        llm = GigaChatHotNewsGenerator(credentials=credentials, model=model)

        data_topics['model'] = model
        data_topics['topics'] = data_topics['raw_data'].apply(
            lambda x: llm.generate_topics(x) if pd.notna(x) else None
        )


        data_topics.to_excel(os.path.join(settings.OUTPUT_DIR_TOPICS, f'TOPICS_{category}_{region}_{period}_{month_begin}.xlsx'), index=False)




# full_data_1 = pd.read_excel('/Users/glebfrolov/Documents/hot_news_agent/outputs_data/processed/Google_Недвижимость_Нижегородская область_Июнь 2025_2025-07-01.xlsx')
# full_data_2 = pd.read_excel('/Users/glebfrolov/Documents/hot_news_agent/outputs_data/processed/Tavily_Недвижимость_Нижегородская область_Июнь 2025_2025-07-01.xlsx')
# full_data_3 = pd.read_excel('/Users/glebfrolov/Documents/hot_news_agent/outputs_data/processed/Telegram_Недвижимость_BASE_Июнь 2025_2025-07-01 00:00:00+00:00.xlsx')
# df = pd.concat([full_data_1, full_data_2])
#
# d = fill_raw_data_html(df)
# with pd.ExcelWriter(os.path.join(settings.OUTPUT_DIR_PROCESSED,
#                                 f"{category}_{region}_{period}.xlsx"), engine='openpyxl') as writer:
#     d.to_excel(writer, index=False)
# print(len(df))


end_time = time.time()
print(end_time - start_time)
