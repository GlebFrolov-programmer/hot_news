import ast
import json
import os
import time
from datetime import datetime, timezone, timedelta, date

from tqdm import tqdm

from llm.google_client import GoogleGenAIHotNewsGenerator
# from llm.openrouter_client import OpenrouterHotNewsGenerator
from llm.gigachat_client import GigaChatHotNewsGenerator
import pandas as pd
from config.settings import settings
import warnings

from llm.together_ai_client import TogetherAIHotNewsGenerator
from tools.archiver import create_archives
from tools.email_sender import send_archives_via_gmail
from tools.raw_data import get_raw_data

warnings.filterwarnings("ignore")  # Отключает все warnings

start_time = time.time()
# regions = ['Нижегородская область']
# categories = ['Недвижимость']

sources = settings.AVAILABLE_SOURCES
# sources = ['Telegram', 'Google']
regions = list(settings.REGION_KEYWORDS.keys())
categories = list(settings.CATEGORIES_SEARCH.keys())
period = 'Июль 2025'
to_excel = True
month_begin = date.today().replace(month=8, day=1)
month_begin_utc = datetime.now(timezone.utc).replace(
            month=8, day=1, hour=0, minute=0, second=0, microsecond=0)

# GIGACHAT
# credentials = settings.AUTHENTICATION['GIGACHAT_API_AUTH']
# model = "GigaChat"
# llm = GigaChatHotNewsGenerator(credentials=credentials, model=model)

# TOGETHER_AI
api_key = settings.AUTHENTICATION['TOGETHER_API_KEY']
model = 'Deepseek'
model_version = 'Qwen/Qwen3-32B-FP8'  # Очень долго генерирует, но сама генерация вроде бы норм
llm = TogetherAIHotNewsGenerator(api_key=api_key, model=model, model_version=model_version)

for region in regions:
    start_time_region = time.time()

    for category in categories:
        print(f'***************** {region} ({regions.index(region)+1}/{len(regions)}) / {category} ({categories.index(category)+1}/{len(categories)})  ***************** ')
        # Шаг 1. Подготовка сырых данных
        raw_data = get_raw_data(
                                sources=sources,
                                category=category,
                                region=region,
                                period=period,
                                to_excel=to_excel,
                                month_begin=month_begin,
                                month_begin_utc=month_begin_utc,
                                max_concurrent=5)

        del raw_data['url']
        raw_data.to_excel(os.path.join(settings.OUTPUT_DIR_RAW, f'RAW_{category}_{region}_{period}_{month_begin}.xlsx'), index=False)
    end_time_region = time.time()
    print(f'Время затраченное на регион {region}: {round((end_time_region - start_time_region) / 60, 2)} мин.')

# Архивация всех эксель
create_archives(
    directory=settings.OUTPUT_DIR_RAW,
    extensions=["xlsx", "xls"],
    max_size_mb=15
)


# Отправка всех архивов по почте
GMAIL_EMAIL = settings.AUTHENTICATION['GMAIL']
GMAIL_APP_PASSWORD = settings.AUTHENTICATION['PASS_GMAIL']
RECIPIENT_EMAIL = settings.AUTHENTICATION['MAIL_SBER']
DIRECTORY_PATH = settings.OUTPUT_DIR_RAW

sent_files = send_archives_via_gmail(
        gmail_email=GMAIL_EMAIL,
        gmail_app_password=GMAIL_APP_PASSWORD,
        recipient_email=RECIPIENT_EMAIL,
        directory_path=DIRECTORY_PATH,
        subject_prefix="Архив 2025-08-01",
        body_text="Архив: ",
        file_pattern="archive_*.zip",  # Только zip файлы
        sort_files=True  # Сортировать по номеру
    )

        # raw_data.to_csv(os.path.join(settings.OUTPUT_DIR_RAW, f'RAW_{category}_{region}_{period}_{month_begin}.csv'), index=False, encoding='utf-8', sep=';')

        # Шаг 2. Генерация тем из текстов
        # print('**** ГЕНЕРАЦИЯ ТЕМ ИЗ ТЕКСТОВ ****')
        # data_topics = pd.read_excel(os.path.join(settings.OUTPUT_DIR_RAW, f'RAW_{category}_{region}_{period}_{month_begin}.xlsx'))
        # data_topics['model'] = model
        # tqdm.pandas()
        # data_topics['topics'] = data_topics['raw_data'].progress_apply(
        #     lambda x: llm.generate_topics(region, category, x) if pd.notna(x) else None
        # )
        # data_topics.to_excel(os.path.join(settings.OUTPUT_DIR_TOPICS, f'TOPICS_{category}_{region}_{period}_{month_begin}.xlsx'), index=False)

        # Шаг 3. Кластеризация тем
        # print('**** КЛАСТЕРИЗАЦИЯ ТЕМ ****')
        # data_clusters = pd.read_excel(
        #     os.path.join(settings.OUTPUT_DIR_TOPICS, f'TOPICS_{category}_{region}_{period}_{month_begin}.xlsx'))
        #
        # topics = []
        # for _, row in data_clusters.iterrows():
        #     if row['topics'] != '{}' and row['topics'] != '[]' and pd.notna(row['topics']):
        #         new_topics = ast.literal_eval(row['topics'])
        #         if isinstance(new_topics, list) and len(new_topics) != 0:
        #             new_topics = new_topics[0]
        #         if isinstance(new_topics, dict):
        #             if len(new_topics) != 0 and 'topics' in new_topics.keys():
        #                 for topic in [i for i in [i.strip(' "\'') for i in new_topics['topics']] if
        #                               i != 'нет информации' and len(i) != 0]:
        #                     topics.append(
        #                         {
        #                             'topic': topic,
        #                             'weight': 2 if row['approved'] else 1
        #                         }
        #                     )
        # response = llm.clusterization_topics(str(topics))
        # print(f'RESPONSE: {response}')

        # with open(
        #         os.path.join(settings.OUTPUT_DIR_CLUSTERS, f'CLUSTERS_{category}_{region}_{period}_{month_begin}.json'),
        #         'w', encoding='utf-8') as file:
        #     # Преобразуем словарь в JSON и записываем в файл
        #     json.dump(response, file, ensure_ascii=False, indent=4)

        # Шаг 4. Результат кластеризации
        # print('**** РЕЗУЛЬТАТ КЛАСТЕРИЗАЦИИ ****')
        # file = open(os.path.join(settings.OUTPUT_DIR_CLUSTERS, f'CLUSTERS_{category}_{region}_{period}_{month_begin}.json'), 'r', encoding='utf-8')
        # result_of_clusterization = json.load(file)
        # for cluster in result_of_clusterization:
        #     weight_of_topics = 0
        #     for topic in cluster['topics']:
        #         weight_of_topics += topic['weight']
        #
        #     cluster['weight_cluster'] = weight_of_topics
        #
        # result_of_clusterization.sort(key=lambda x: x["weight_cluster"], reverse=True)
        #
        # for i in result_of_clusterization:
        #     print(
        #         f'WEIGHT: {i["weight_cluster"]} CLUSTER: {i["cluster_name"]} TOPICS({len(i["topics"])}): {i["topics"]}')



end_time = time.time()
print(round((end_time - start_time) / 60, 2))


