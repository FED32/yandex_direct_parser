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


def save_file(path, name, content):
    """
    Сохраняет файл
    """
    if not os.path.isdir(path):
        os.mkdir(path)
    dir_ = f"{path}/{name}"
    with open(dir_, 'wb') as file:
        file.write(content)
    # file = open(dir_, 'wb')
    # file.write(content)
    # file.close()
    print('Сохранен ', dir_)


def get_user_reports(login: str,
                     token: str,
                     date_from: str,
                     date_to: str,
                     acc_perf_rep=True,
                     adgr_perf_rep=True,
                     ad_perf_rep=True,
                     camp_perf_rep=True,
                     crit_perf_rep=True,
                     custom_rep=True,
                     reach_and_freq_perf_rep=True,
                     search_query_perf_rep=True,
                     data_folder='./data_user',
                     logs_folder='./logs'):
    """
    Загружает заданные отчеты за заданный период
    """

    # создаем рабочую папку, если еще не создана
    if not os.path.isdir(data_folder):
        os.mkdir(data_folder)
    # создаем папку для сохранения отчетов
    if not os.path.isdir(logs_folder):
        os.mkdir(logs_folder)

    # путь для сохранения файлов
    path_ = data_folder

    # составляем список отчетов
    report_list = []
    if acc_perf_rep is True:
        report_list.append('ACCOUNT_PERFORMANCE_REPORT')
    if adgr_perf_rep is True:
        report_list.append('ADGROUP_PERFORMANCE_REPORT')
    if ad_perf_rep is True:
        report_list.append('AD_PERFORMANCE_REPORT')
    if camp_perf_rep is True:
        report_list.append('CAMPAIGN_PERFORMANCE_REPORT')
    if crit_perf_rep is True:
        report_list.append('CRITERIA_PERFORMANCE_REPORT')
    if custom_rep is True:
        report_list.append('CUSTOM_REPORT')
    if reach_and_freq_perf_rep is True:
        report_list.append('REACH_AND_FREQUENCY_PERFORMANCE_REPORT')
    if search_query_perf_rep is True:
        report_list.append('SEARCH_QUERY_PERFORMANCE_REPORT')

    direct = YandexDirectEcomru(login=login, token=token, use_operator_units='false')

    for report_type in report_list:
        report_name = f"{report_type.lower()}-{str(datetime.now().time().strftime('%H%M%S'))}"
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
                save_file(path=f"{path_}/{login}",
                          name=f"{login}_{report_type.lower()}_{str(datetime.now().time().strftime('%H%M%S'))}.tsv",
                          content=report.content)
                add_logging(logs_folder, data=f"{login}_{report_type}: файл отчета сохранен")
            elif report.status_code == 400:
                add_logging(
                    logs_folder,
                    data=f"{login}_{report_type}: Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
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


def convert_and_upl_user_data(path,
                              upl_into_db=0,
                              host=None,
                              port=None,
                              ssl_mode=None,
                              db_name=None,
                              user=None,
                              password=None,
                              target_session_attrs='read-write'
                              ):
    """
    Обрабатывает загруженные файлы и отправляет в базу данных
    """

    # создаем экземпляр класса, проверяем подключение
    database = DbEcomru(host=host,
                        port=port,
                        ssl_mode=ssl_mode,
                        db_name=db_name,
                        user=user,
                        password=password,
                        target_session_attrs=target_session_attrs)

    connection = database.test_db_connection()
    print(connection)

    if connection is not None:
        add_logging(logs_folder, data=str(connection))
        dataset = database.read_trans_tsv(path)
        for col in dataset.columns:
            # print(col, dtypes[col])
            if dtypes[col] == 'datetime':
                #         if col == 'date':
                dataset[col] = dataset[col].apply(lambda x: datetime.strptime(x, '%Y-%m-%d').date())
            elif dtypes[col] == 'float64':
                dataset[col] = dataset[col].replace('--', np.nan)
                dataset[col] = dataset[col].astype('float64')
            else:
                dataset[col] = dataset[col].astype(dtypes[col])

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




