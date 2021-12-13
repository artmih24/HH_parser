# Библиотека для работы с HTTP-запросами. Будем использовать ее для обращения к API HH
import requests
# импорт библиотек
# Пакет для удобной работы с данными в формате json
import json

# Модуль для работы со значением времени
import time

# Модуль для работы с операционной системой. Будем использовать для работы с файлами
import os

from datetime import datetime
from pymongo import MongoClient
from py_currency_converter import convert
import re
from math import *

#получение страницы поиска
def getPage(page=0, reg=1, prof=''):
    """
    Создаем метод для получения страницы со списком вакансий.
    Аргументы:
        page - Индекс страницы, начинается с 0. Значение по умолчанию 0, т.е. первая страница
    """

    # Справочник для параметров GET-запроса
    params = {
        'text': f'NAME:{prof}',  # Текст фильтра. В имени должно быть слово "Аналитик"
        'area': reg,  # Поиск ощуществляется по вакансиям города reg
        'page': page,  # Индекс страницы поиска на HH
        'per_page': 100  # Кол-во вакансий на 1 странице
    }

    req = requests.get('https://api.hh.ru/vacancies', params)  # Посылаем запрос к API  переходит по ссылке и получает данные
    data = req.content.decode()  # Декодируем его ответ, чтобы Кириллица отображалась корректно
    req.close()    # закрывает запрос (соединение)
    return data    # возвращает данные

professions = ['Аналитик', 'Программист', 'Разработчик', 'Инженер', 'Администратор', 'Тестировщик']

# составление названия файла с датой и временем
#             дата и всремя вызова     убирает -          заменяет пробел на   : = пробелы   убираем мс
datetime_string = str(datetime.now()).replace("-", "").replace(" ", "T").replace(":", "").split(".")[0]
filename = f'HH_vacancies_{datetime_string}.json'    # все ято вышк записывается в название файла

# пишем в файл первую [ (квадратную скобку)
f = open(filename, mode='a', encoding='utf8')
f.write('[')
f.close()

for prof in professions:
    for reg in range(1, 31):
        # Считываем первые 2000 вакансий
        print(f'{str(datetime.now())} | Парсинг вакансий "{prof}" HH.ru для региона {reg} начат')
        maxpage = 20   # кол-во страниц
        for page in range(0, maxpage):

            print(f'{str(datetime.now())} | Получено записей: {page * 100}', end='\r')

            # Преобразуем текст ответа запроса в справочник Python
            jsonObj = json.loads(getPage(page=page, reg=reg, prof=prof))

            # Получаем и проходимся по непосредственно списку вакансий
            for v in jsonObj['items']:
                # Обращаемся к API и получаем детальную информацию по конкретной вакансии
                req = requests.get(v['url'])
                data = req.content.decode()
                req.close()

                data_dict = json.loads(data)
                if data_dict['description'] != None:
                    data_dict['description'] = re.sub(r"<.*?>", "", data_dict['description'])

                if data_dict["salary"] != None:
                    # исправление кода валюты российского рубля
                    if data_dict['salary']['currency'] == 'RUR':
                        data_dict['salary']['currency'] = 'RUB'
                    # перевод из белорусских рублей в российские (не поддерживается библиотекой)
                    if data_dict['salary']['currency'] == 'BYR':
                        if data_dict['salary']['from'] != None:
                            data_dict['salary']['from'] = data_dict['salary']['from'] * 29.32
                        if data_dict['salary']['to'] != None:
                            data_dict['salary']['to'] = data_dict['salary']['to'] * 29.32
                        data_dict['salary']['currency'] = 'RUB'
                    # перевод из иностранных валют в российский рубль
                    if data_dict['salary']['currency'] != 'RUB':
                        try:
                            if data_dict['salary']['from'] != None:
                                data_dict['salary']['from'] = convert(base=data_dict['salary']['currency'], amount=data_dict['salary']['from'], to=['RUB'])['RUB']
                        except:
                            data_dict['salary']['from'] = None
                        try:
                            if data_dict['salary']['to'] != None:
                                data_dict['salary']['to'] = convert(base=data_dict['salary']['currency'], amount=data_dict['salary']['to'], to=['RUB'])['RUB']
                        except:
                            data_dict['salary']['from'] = None
                        data_dict['salary']['currency'] = 'RUB'
                    # округление зп
                    if data_dict['salary']['from'] != None:
                        data_dict['salary']['from'] = round(data_dict['salary']['from'])
                    if data_dict['salary']['to'] != None:
                        data_dict['salary']['to'] = round(data_dict['salary']['to'])
                
                data = json.dumps(data_dict)

                # Дописываем вакансии в Json файл
                # Записываем в него ответ запроса и закрываем файл
                f = open(filename, mode='a', encoding='utf8')
                f.write(data)
                f.write(',')
                f.close()

            # Необязательная задержка, но чтобы не нагружать сервисы hh, оставим. 5 сек мы можем подождать
            time.sleep(0.25)
        print(f'{str(datetime.now())} | Парсинг HH.ru для региона {reg} закончен')

# удаление последней запятой в JSON-файле
with open(filename, 'rb+') as filehandle:
    filehandle.seek(-1, os.SEEK_END)   # ищет полсдений символ (установка каретки перед последним символом)
    filehandle.truncate()       # удаление всего что после каретки

# пишем в файл последнюю ] (квадратную скобку)
f = open(filename, mode='a', encoding='utf8')
f.write(']')
f.close()

# импорт содержимого JSON-файла в базу данных
print(f'{str(datetime.now())} | Начат импорт содержимого JSON-файла в базу данных')
db_original = MongoClient('mongodb://127.0.0.1:27017')['KM5_BigData'][filename[:-5]]    # объявление бд
with open(filename, 'r', encoding='utf8') as json_file:                                 # открываем json файл
    json_file_data = json.load(json_file)
db_original.insert_many(json_file_data)                                  # импорт данных из json файла в бд
print(f'{str(datetime.now())} | Импорт содержимого JSON-файла в базу данных завершен!')
# удаление лишних столбцов в базе данных
print(f'{str(datetime.now())} | Начато удаление лишних столбцов в базе данных')
db_original.update_many({}, {'$unset': {'insider_interview': '',
                                        'response_letter_required': '',
                                        'type': '',
                                        'allow_messages': '',
                                        'site': '',
                                        'department': '',
                                        'contacts': '',
                                        'branded_description': '',
                                        'vacancy_constructor_template': '',
                                        'accept_handicapped': '',
                                        'accept_kids': '',
                                        'archived': '',
                                        'response_url': '',
                                        'code': '',
                                        'hidden': '',
                                        'accept_incomplete_resumes': '',
                                        'quick_responses_allowed': '',
                                        'negotiations_url': '',
                                        'suitable_resumes_url': '',
                                        'apply_alternate_url': '',
                                        'has_test': '',
                                        'test': '',
                                        'alternate_url': '',
                                        'accept_temporary': '',
                                        'working_days': '',
                                        'driver_license_types': '',
                                        'created_at': '',
                                        'premium': '',
                                        'billing_type': '',
                                        'relations': '',
                                        'working_time_intervals': '',
                                        'working_time_modes': ''}})
print(f'{str(datetime.now())} | Удаление лишних столбцов в базе данных завершено!')

# подготовка коллекции к A/B-тестированию
print(f'{str(datetime.now())} | Подготовка коллекции к A/B-тестированию')
db_len = db_original.estimated_document_count() # получаем количество данных в БД
db_len_A = ceil(db_len * 0.75)                  # количество предполагаемых данных с флагом А
db_len_B = db_len - db_len_A                    # количество предполагаемых данных с флагом B
db_original.update_many({}, {'$set': {'flag': 'A'}})    # устанавливаем флаг = А для всех записей
db_original.update_many({'$expr': {'$eq': [3, {'$mod': [{'$toInt': '$id'}, 4]}]}}, {'$set': {'flag': 'B'}}) # устанавливаем флаг = В примерно для каждой 4й записи
db_docs_A = db_original.count_documents({'flag': 'A'}) # подсчитываем кол-во записей с флагом А
db_docs_B = db_original.count_documents({'flag': 'B'}) # подсчитываем кол-во записей с флагом В
while (db_docs_B < db_len_B):
    # если записей с флагом В меньше, чем должно быть
	db_original.update_one({'flag': 'A'}, {'$set': {'flag': 'B'}})
	db_docs_A = db_original.count_documents({'flag': 'A'})
	db_docs_B = db_original.count_documents({'flag': 'B'})
while (db_docs_A < db_len_A):
    # если записей с флагом А меньше, чем должно быть
	db_original.update_one({'flag': 'B'}, {'$set': {'flag': 'A'}})
	db_docs_A = db_original.count_documents({'flag': 'A'})
	db_docs_B = db_original.count_documents({'flag': 'B'})
print(f'{str(datetime.now())} | Подготовка коллекции к A/B-тестированию завершена!')

input('Нажмите Enter для закрытия программы . . . ')

