import pandas as pd
import numpy as np
from datetime import date
from datetime import datetime
from datetime import timedelta
import os
import glob
import shutil
# import sys
from threading import Thread
# from configparser import ConfigParser
from ecom_yandex_direct import YandexDirectEcomru
from ecom_db_files import DbEcomru
from data_logging import add_logging


data_folder = './data'
logs_folder = './logs'
delete_files = 1
upl_into_db = 1

print('data_folder: ', data_folder)
print('logs_folder: ', logs_folder)
print('delete_files: ', delete_files)
print('upl_into_db: ', upl_into_db)

# читаем параметры подключения
host = os.environ.get('ECOMRU_PG_HOST', None)
port = os.environ.get('ECOMRU_PG_PORT', None)
ssl_mode = os.environ.get('ECOMRU_PG_SSL_MODE', None)
db_name = os.environ.get('ECOMRU_PG_DB_NAME', None)
user = os.environ.get('ECOMRU_PG_USER', None)
password = os.environ.get('ECOMRU_PG_PASSWORD', None)
target_session_attrs = 'read-write'


# создаем рабочую папку, если еще не создана
if not os.path.isdir(data_folder):
    os.mkdir(data_folder)
# создаем папку для сохранения отчетов
if not os.path.isdir(logs_folder):
    os.mkdir(logs_folder)

# путь для сохранения файлов
path_ = f'{data_folder}/{str(date.today())}/'
if not os.path.isdir(path_):
    os.mkdir(path_)

# создаем экземпляр класса, проверяем соединение с базой
database = DbEcomru(host=host,
                    port=port,
                    ssl_mode=ssl_mode,
                    db_name=db_name,
                    user=user,
                    password=password,
                    target_session_attrs=target_session_attrs)

connection = database.test_db_connection()


# функция для получения отчетов
def thread_func(*args):
    login_ = args[0]
    token_ = args[1]
    report_list_ = args[2]
    date_from_ = args[3]
    date_to_ = args[4]
    direct = YandexDirectEcomru(login=login_, token=token_, use_operator_units='false')
    # direct.get_campaigns()
    for report_type in report_list_:
        # n_units = int(direct.counter[-1]['units'].split('/')[1])
        # if n_units >= 50:
        report_name = report_type.lower() + '-' + str(datetime.now().time().strftime('%H%M%S'))
#         print(report_name)
        report = direct.get_stat_report(report_name=report_name,
                                        report_type=report_type,
                                        date_range_type='CUSTOM_DATE',
                                        include_vat='YES',
                                        format_='TSV',
                                        limit=None,
                                        offset=None,
                                        date_from=date_from_,
                                        date_to=date_to_,
                                        processing_mode='auto',
                                        return_money_in_micros='false',
                                        skip_report_header='false',
                                        skip_column_header='false',
                                        skip_report_summary='true'
                                        )
#         print(report.headers)
        if report is not None:
            if report.status_code == 200:
                add_logging(logs_folder, data=f"{login_}_{report_type}: отчет создан успешно")
                database.save_file(path=path_+f'{login_}',
                                   name=f"{login_}_{report_type.lower()}_{str(datetime.now().time().strftime('%H%M%S'))}.tsv",
                                   content=report.content)
                add_logging(logs_folder, data=f"{login_}_{report_type}: файл отчета сохранен")
            elif report.status_code == 400:
                add_logging(logs_folder, data=f"{login_}_{report_type}: Параметры запроса указаны неверно или достигнут лимит "
                                 f"отчетов в очереди")
            elif report.status_code == 500:
                add_logging(logs_folder, data=f"{login_}_{report_type}: при формировании отчета произошла ошибка")
            elif report.status_code == 502:
                add_logging(logs_folder, data=f"{login_}_{report_type}: время формирования отчета превысило серверное ограничение")
        else:
            add_logging(logs_folder, data=f"{login_}_{report_type}: ошибка соединения с сервером API либо непредвиденная ошибка")
            continue
#         else:
#             add_logging(data=f'{login_}: не достаточно баллов')
#             break
#     direct.get_campaigns()
    pd.DataFrame(direct.counter).to_csv(
        logs_folder+'/'+f"log_{str(date.today())}_{str(datetime.now().time().strftime('%H%M%S'))}_{login_}.csv",
        index=False,
        sep=';')


if connection is not None:
    add_logging(logs_folder, data=str(connection))

    # загружаем таблицу с данными
    db_data = database.get_ya_ads_data()
    # db_data = database.get_table(table_name='ya_ads_data')
    print('Количество записей в таблице статистики ' + str(db_data.shape[0]))
    add_logging(logs_folder, data='Количество записей в таблице статистики ' + str(db_data.shape[0]))

    # извлекаем последнюю дату
    if db_data.shape[0] > 0:
        last_date = db_data['date'].sort_values(ascending=False).values[0]
        add_logging(logs_folder, data='Дата последней записи в таблице статистики ' + str(last_date))
        print('Дата последней записи в таблице статистики ', last_date)
    else:
        last_date = date.today() - timedelta(days=3)

    # загружаем таблицу с аккаунтами
    api_keys = database.get_accounts().drop_duplicates(subset=['key_attribute_value', 'attribute_value'], keep='last')

    # # тестовая таблица с аккаунтами
    # logins = os.environ.get('YA_TEST_USERS', None).split(', ')
    # tokens = os.environ.get('YA_TEST_TOKENS', None).split(', ')
    # api_keys = pd.DataFrame({'login': logins, 'token': tokens})
    add_logging(logs_folder, data='Количество записей в таблице аккаунтов ' + str(api_keys.shape[0]))

    # загружаем типы отчетов
    reports = database.get_data_by_response(sql_resp='select * from ya_ads_report_types')
    report_list = reports['id_report'].tolist()
    # report_list = ['AD_PERFORMANCE_REPORT']
    # report_list = ['SEARCH_QUERY_PERFORMANCE_REPORT']
    # report_list = ['CUSTOM_REPORT']

    # задаем временной интервал
    date_from = str(last_date + timedelta(days=1))
    # date_from = '2022-09-01'
    date_to = str(date.today() - timedelta(days=1))
    # date_to = '2022-12-07'

    print('date_from', date_from)
    print('date_to', date_to)

    # создаем отдельные потоки по каждому аккаунту
    threads = []
    for index, keys in api_keys.iterrows():
        # login = keys[0]
        # token = keys[1]
        login = keys[1]
        token = keys[2]
        threads.append(Thread(target=thread_func, args=(login, token, report_list, date_from, date_to)))

    print(threads)

    # запускаем потоки
    for thread in threads:
        thread.start()

    # останавливаем потоки
    for thread in threads:
        thread.join()

else:
    add_logging(logs_folder, data='Нет подключения к БД')

# проверяем наличие загруженных файлов
files = []
for folder in os.listdir(path_):
    files += (glob.glob(os.path.join(path_ + '/' + folder, "*.tsv")))

if len(files) > 0:
    # создаем датасет на основе загруженных по API данных
    dataset = database.make_dataset(path=path_)
    add_logging(logs_folder, data='Количество сырых строк ' + str(dataset.shape[0]))

    if db_data.shape[0] > 0:
        # фильтрация дубликатов
        db_data_from = db_data[db_data['date'] >= datetime.strptime(date_from, '%Y-%m-%d').date()]
        db_data_from = db_data_from.fillna(np.nan)

        # колонки по которым происходит поиск совпадений
        # cols = dataset.columns.tolist()
        cols = ['report_id', 'adformat', 'adgroupid', 'adgroupname', 'adid',
       'adnetworktype', 'age', 'avgtrafficvolume', 'bounces', 'campaignid',
       'campaignname', 'campaignurlpath', 'campaigntype', 'carriertype',
       'clicks', 'clientlogin', 'cost', 'criterion', 'criterionid',
       'criteriontype', 'ctr', 'date', 'device', 'externalnetworkname',
       'gender', 'impressions', 'incomegrade', 'locationofpresenceid',
       'locationofpresencename', 'matchtype', 'mobileplatform', 'profit',
       'revenue', 'rladjustmentid', 'sessions', 'slot', 'targetingcategory',
       'targetinglocationid', 'targetinglocationname', 'weightedctr',
       'weightedimpressions']

        # print(dataset.shape)
        dataset = dataset.drop_duplicates(subset=cols, keep='first')
        # print(dataset.shape)

        # объединяем датасеты с удалением дубликатов
        into_db = pd.concat([db_data_from, dataset], axis=0).reset_index().drop(['index', 'id'], axis=1).drop_duplicates(
            subset=cols,
            keep=False)
        print('dataset', dataset.shape)
        print('db_data_from', db_data_from.shape)
        print('into_db', into_db.shape)
    else:
        into_db = dataset
        print('into_db', into_db.shape)

    into_db.to_csv(path_ + 'into_db.csv', sep=';', index=False)
    add_logging(logs_folder, data='Готово строк для записи в БД: ' + str(into_db.shape[0]))
    print(into_db)

    if upl_into_db == 1:
        upload = database.upl_to_db(dataset=into_db, table_name='ya_ads_data')
        if upload is not None:
            add_logging(logs_folder, data='Запись в БД выполнена')
        else:
            add_logging(logs_folder, data='Запись в БД не удалась')
    else:
        add_logging(logs_folder, data='Запись в БД отключена')
else:
    print('Нет загруженных файлов для обработки')
    add_logging(logs_folder, data='Нет загруженных файлов для обработки')

if delete_files == 1:
    # удаляем файлы (папку)
    try:
        shutil.rmtree(path_)
        add_logging(logs_folder, data='Файлы удалены')
    except OSError as e:
        print("Error: %s - %s." % (e.filename, e.strerror))
        add_logging(logs_folder, data='Ошибка при удалении файлов')
else:
    add_logging(logs_folder, data='Удаление файлов отменено')