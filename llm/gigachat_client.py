import json
import re

from langchain_community.chat_models.gigachat import GigaChat
from langchain.schema import SystemMessage, HumanMessage
import os

from config.settings import settings


class GigaChatHotNewsGenerator:
    def __init__(self, credentials, model):
        """
        Инициализация с использованием LangChain

        :param credentials: Авторизационные данные (логин/пароль или токен)
        """
        self.model = model
        self.credentials = credentials
        self.llm = GigaChat(
            credentials=credentials,
            scope="GIGACHAT_API_PERS",
            verify_ssl_certs=False,
            model=model,
            verbose=True,
            temperature=0.1,
            early_stopping_method="generate"
        )

    def generate_topics(self, message: str, user_message: str = None, system_message: str = None) -> dict:
        """
        Генерация тезисов

        :param user_message: Промт пользователя
        :param system_message: Системное сообщение (роль модели)
        :return: Ответ модели
        """
        if not system_message:
            system_message = """
            Ты профессиональный аналитик, который умеет вычленять из текста самые важные темы, которые состоят из тезисов, инсайдов и другой крайне интересной информации.
    
            Требования:  
            1. Если тезис касается конкретного объекта, в нем ДОЛЖНО быть указано НАЗВАНИЕ ОБЪЕКТА.   
            2. Тезисы должны быть самодостаточными: по каждому можно понять суть без дополнительной информации.  
            3. Максимум 3 тезиса, минимум — 0.  
            4. Тезисы должны быть уникальными по смыслу (без дублирования).  
            5. Только интересная/значимая информация (не тривиальные факты).  
            6. ВЫВОДИ пустой json, если вообще нет подходящих тем или во всем тексте прослеживается какая-то реклама продукта.
            7. В полученных темах ОБЯЗАТЕЛЬНО должно говориться про регион "Нижегородская область" и тема "Недвижимость" (другие регионы и темы не интересуют).
    
            Формат ответа (JSON):
                            ```json
                            {
                                "topics": [  
                                    "<Тезис1>",
                                    "<Тезис2>",
                                    "<Тезис3>"
                                ]
                            }
                             ```
            """

            # print(system_message)

        if not user_message:
            user_message = "Проанализируй текст и выдели тезисы.\n Текст: " + message

        messages = [
            SystemMessage(content=system_message),
            HumanMessage(content=user_message)
        ]

        try:
            response = self.llm(messages)
            correct_response = self.parse_json_obj_from_llm(response.content)
            return correct_response
        except Exception as e:
            raise Exception(f"Ошибка при запросе к {self.model}: {str(e)}")


    # Парсер для JSON-ответов от LLM
    # def parse_json_obj_from_llm(self, text: str) -> dict:
    #     try:
    #         json_string = text.strip('"')
    #         cleaned_json = re.sub(r'```json\n|```', '', json_string)
    #         cleaned_json = cleaned_json.strip()
    #         json_insights_data = json.loads(cleaned_json)
    #
    #         return json_insights_data
    #     except json.JSONDecodeError:
    #         # Если JSON некорректен, пытаемся извлечь JSON0
    #         json_match = re.search(r'\{.*\}', text, re.DOTALL)
    #         if json_match:
    #             return json.loads(json_match.group(0))
    #         raise ValueError("LLM response is not valid JSON")

    def parse_json_obj_from_llm(self, text: str) -> dict:
        """
        Пытается извлечь JSON из ответа LLM.
        Если не получается — возвращает пустой словарь {}.
        """
        if not text:
            return {}

        try:
            # 1. Пробуем распарсить как чистый JSON
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # 2. Чистим markdown-форматирование (```json и ```)
        cleaned_text = re.sub(r'^```(json)?\n|```$', '', text.strip(), flags=re.MULTILINE)
        cleaned_text = cleaned_text.strip()

        # 3. Пробуем распарсить очищенный текст
        try:
            return json.loads(cleaned_text)
        except json.JSONDecodeError:
            pass

        # 4. Пытаемся найти JSON в подстроке
        try:
            json_match = re.search(r'\{[\s\S]*?\}', cleaned_text)
            if json_match:
                return json.loads(json_match.group(0))
        except (json.JSONDecodeError, AttributeError):
            pass

        # 5. Если всё провалилось — возвращаем {} вместо ошибки
        return {}


# Пример использования
if __name__ == "__main__":

    credentials = settings.AUTHENTICATION['GIGACHAT_API_AUTH']
    model = 'GigaChat-Pro'

    # Инициализация генератора
    llm = GigaChatHotNewsGenerator(credentials=credentials, model=model)

    message = """Кабардино-Балкария и Крым лидируют среди российских регионов по доле рассрочки в продажах первичного жилья  , рассказал директор Центра финансовой аналитики Сбербанка Михаил Матовников на конференции Domclick Digital Day. По его данным, по результатам второго квартала 2025 года, топ по доле рассрочки среди регионов с большим объемом продаж возглавляет Кабардино-Балкария с 73% ( 20 п.п. к 2023 году), на втором месте - Крым (69%,   23 п.п.), на третьем - Калининградская область (47%,  18 п.п.). В   Москве  , как отметил Матовников, на рассрочку приходится уже 54% продаж ( 32 п.п.), а на   Санкт-Петербург   - 58% ( 22 п.п.). В целом по России с рассрочкой осуществляется 41% продаж, рассказал аналитик. По его словам, __рассрочка наиболее востребована в элитном сегменте.__ С учетом оценки по двум столицам, Калининградской, Нижегородской, Рязанской, Тульской и Калужской областям доля рассрочки в элитном сегменте по результатам пяти месяцев 2023 года составляет 48% ( 24 п.п. к 2023 году), в бизнес-классе - 30% ( 22 п.п.), а в массовом - 22% ( 20 п.п.).  Подписаться на РИА Недвижимость (https:  t.me ria_realty)    Все наши каналы (https:  t.me addlist qrz_nSYCDL0wYzUy)"""

    try:
        # Генерация тезисов
        response = llm.generate_topics(message)
        print("\nРезультат:")
        print(response)
    except Exception as e:
        print(f"Ошибка: {e}")
