import pandas as pd
import numpy as np
from datetime import date
from datetime import datetime
from datetime import timedelta
import os
import glob
import shutil
# import sys
from ecom_yandex_direct import YandexDirectEcomru
from ecom_db_files import DbEcomru
from data_logging import add_logging


def add_new_user_data(login, token,
                      days=30,
                      upl_into_db=1,
                      delete_files=1,
                      data_folder='./data_new_user',
                      logs_folder='./logs'):
    """
    Проверяет наличие записей в базе и при отсутствии добавляет последнюю статистику за указанный период
    """

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

    # создаем экземпляр класса, проверяем подключение
    database = DbEcomru(host=host,
                        port=port,
                        ssl_mode=ssl_mode,
                        db_name=db_name,
                        user=user,
                        password=password,
                        target_session_attrs=target_session_attrs)

    connection = database.test_db_connection()

    if connection is not None:
        add_logging(logs_folder, data=str(connection))

        # загружаем типы отчетов
        # reports = database.get_data_by_response(sql_resp='select * from ya_ads_report_types')
        # report_list = reports['id_report'].tolist()
        report_list = ['SEARCH_QUERY_PERFORMANCE_REPORT']

        # запрашиваем данные из базы с фильтром по clientlogin
        sql_resp = f"""SELECT * FROM ya_ads_data WHERE clientlogin = '{login.split('@')[0]}'"""
        data = database.get_data_by_response(sql_resp)
        # print(data)

        if data is not None:
            print(f'Количество записей найдено {data.shape[0]}')
            if data.shape[0] == 0:
                date_from = str(date.today() - timedelta(days=days))
                date_to = str(date.today())
                direct = YandexDirectEcomru(login=login, token=token, use_operator_units='false')
                for report_type in report_list:
                    report_name = report_type.lower() + '-' + str(datetime.now().time().strftime('%H%M%S'))
                    report = direct.get_stat_report(report_name=report_name,
                                                    report_type=report_type,
                                                    date_range_type='CUSTOM_DATE',
                                                    include_vat='YES',
                                                    format_='TSV',
                                                    limit=None,
                                                    offset=None,
                                                    date_from=date_from,
                                                    date_to=date_to,
                                                    processing_mode='auto',
                                                    return_money_in_micros='false',
                                                    skip_report_header='false',
                                                    skip_column_header='false',
                                                    skip_report_summary='true'
                                                    )
                    if report is not None:
                        if report.status_code == 200:
                            database.save_file(
                                path=path_ + f'{login}',
                                name=f"{login}_{report_type.lower()}_{str(datetime.now().time().strftime('%H%M%S'))}.tsv",
                                content=report.content)
                            add_logging(logs_folder, data=f"{login}_{report_type}: файл отчета сохранен")
                        elif report.status_code == 400:
                            add_logging(logs_folder,
                                        data=f"{login}_{report_type}: Параметры запроса указаны неверно или достигнут лимит "
                                             f"отчетов в очереди")
                        elif report.status_code == 500:
                            add_logging(logs_folder,
                                        data=f"{login}_{report_type}: при формировании отчета произошла ошибка")
                        elif report.status_code == 502:
                            add_logging(logs_folder,
                                        data=f"{login}_{report_type}: время формирования отчета превысило серверное ограничение")
                    else:
                        add_logging(logs_folder,
                                    data=f"{login}_{report_type}: ошибка соединения с сервером API либо непредвиденная ошибка")
                        print(f'Не удалось загрузить отчет {report_type}')
                        continue
            else:
                print('Данные уже имеются в базе')
                add_logging(logs_folder, data='Данные уже имеются в базе')
        else:
            print('Ошибка при загрузке')
            add_logging(logs_folder, data='Ошибка при загрузке')
    else:
        add_logging(logs_folder, data='Нет подключения к БД')

    # проверяем наличие загруженных файлов
    files = []
    for folder in os.listdir(path_):
        files += (glob.glob(os.path.join(path_ + '/' + folder, "*.tsv")))

    if len(files) > 0:
        # создаем датасет на основе загруженных по API данных
        dataset = database.make_dataset(path=path_)
        print('dataset', dataset.shape)
        if dataset.shape[0] > 0:
            if upl_into_db == 1:
                upload = database.upl_to_db(dataset=dataset, table_name='ya_ads_data')
                if upload is not None:
                    print('Запись в БД выполнена')
                    add_logging(logs_folder, data='Запись в БД выполнена')
                else:
                    print('Запись в БД не удалась')
                    add_logging(logs_folder, data='Запись в БД не удалась')
            else:
                print('Запись в БД отключена')
                add_logging(logs_folder, data='Запись в БД отключена')
        else:
            print('Еще нет статистики по данному аккаунту')
            add_logging(logs_folder, data='Еще нет статистики по данному аккаунту')
    else:
        print('Нет загруженных файлов для обработки')
        add_logging(logs_folder, data='Нет загруженных файлов для обработки')

    if delete_files == 1:
        # удаляем файлы (папку)
        try:
            shutil.rmtree(path_)
            print('Файлы удалены')
            add_logging(logs_folder, data='Файлы удалены')
        except OSError as e:
            print("Error: %s - %s." % (e.filename, e.strerror))
            add_logging(logs_folder, data='Ошибка при удалении файлов')
    else:
        print('Удаление файлов отменено')
        add_logging(logs_folder, data='Удаление файлов отменено')

    return 'OK'




