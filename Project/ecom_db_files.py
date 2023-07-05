from datetime import datetime
import os
import pandas as pd
import numpy as np
import glob
import psycopg2
from sqlalchemy import create_engine


class DbEcomru:
    def __init__(self, host,
                 port,
                 ssl_mode,
                 db_name,
                 user,
                 password,
                 target_session_attrs):

        self.host = host
        self.port = port
        self.ssl_mode = ssl_mode
        self.db_name = db_name
        self.user = user
        self.password = password
        self.target_session_attrs = target_session_attrs

        self.db_access = f"host={self.host} " \
                         f"port={self.port} " \
                         f"sslmode={self.ssl_mode} " \
                         f"dbname={self.db_name} " \
                         f"user={self.user} " \
                         f"password={self.password} " \
                         f"target_session_attrs={self.target_session_attrs}"

    def test_db_connection(self):
        """
        Проверка доступа к БД
        """
        try:
            conn = psycopg2.connect(self.db_access)
            q = conn.cursor()
            q.execute('SELECT version()')
            connection = q.fetchone()
            print(connection)
            conn.close()
            return connection
        except:
            print('Нет подключения к БД')
            return None

    def get_table(self, table_name):
        """
        Загружает таблицу из базы
        """
        query = f"SELECT * FROM {table_name}"
        db_params = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"
        engine = create_engine(db_params)
        try:
            print(f'Загружается таблица {table_name}')
            data = pd.read_sql(query, con=engine)
            print(f'Загружена {table_name}')
            return data
        except:
            print('Произошла непредвиденная ошибка')
            return None

    def get_ya_ads_data(self):
        """
        Загружает таблицу с данными статистики
        """
        # query = """SELECT * FROM ya_ads_data WHERE report_id = 'SEARCH_QUERY_PERFORMANCE_REPORT'"""
        # query = """SELECT * FROM ya_ads_data WHERE report_id = 'CUSTOM_REPORT'"""
        query = """SELECT * FROM ya_ads_data"""
        db_params = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"
        engine = create_engine(db_params)
        try:
            print('Загружается таблица ya_ads_data')
            data = pd.read_sql(query, con=engine)
            print(f'Загружена ya_ads_data')
            return data
        except:
            print('Произошла непредвиденная ошибка')
            return None

    def upl_to_db(self, dataset, table_name):
        """
        Загружает данные в БД
        """
        db_params = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"
        engine = create_engine(db_params)
        try:
            dataset.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            print('Данные записаны в БД')
            return 'ok'
        except:
            print('Произошла непредвиденная ошибка')
            return None

    def get_data_by_response(self, sql_resp):
        """
        Загружает таблицу по SQL-запросу
        """
        db_params = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"
        engine = create_engine(db_params)
        try:
            print('Загружается таблица')
            data = pd.read_sql(sql_resp, con=engine)
            print('Загружена таблица по SQL-запросу')
            return data
        except:
            print('Произошла непредвиденная ошибка, ошибка в запросе или ошибка доступа')
            return None

    def get_accounts(self):
        """
        Загружает из базы аккаунты
        """
        db_params = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"
        engine = create_engine(db_params)
        query = """
                SELECT 
                al.id, 
                asd.attribute_value key_attribute_value, 
                asd2.attribute_value 
                FROM account_service_data asd 
                JOIN account_list al ON asd.account_id = al.id
                JOIN (SELECT al.mp_id, asd.account_id, asd.attribute_id, asd.attribute_value 
                        FROM account_service_data asd
                        JOIN account_list al ON asd.account_id = al.id 
                        WHERE al.mp_id = 16) asd2 ON asd2.mp_id = al.mp_id 
                        AND asd2.account_id= asd.account_id AND asd2.attribute_id <> asd.attribute_id
                WHERE al.mp_id = 16 AND asd.attribute_id = 24 AND al.status_1 = 'Active' 
                GROUP BY asd.attribute_id, asd.attribute_value, asd2.attribute_id, asd2.attribute_value, al.id 
                ORDER BY id
                """
        try:
            print('Загружается таблица')
            data = pd.read_sql(query, con=engine)
            print('Загружена таблица аккаунтов')
            return data
        except:
            print('Произошла непредвиденная ошибка')
            return None

    @staticmethod
    def save_file(path, name, content):
        """
        Сохраняет файл
        """
        if not os.path.isdir(path):
            os.mkdir(path)
        dir_ = path + '/' + name
        file = open(dir_, 'wb')
        file.write(content)
        file.close()
        print('Сохранен', dir_)

    @staticmethod
    def read_trans_tsv(file, login):
        """
        Обрабатывает загруженные данные
        """
        columns = {'AdFormat': 'adformat', 'AdGroupId': 'adgroupid', 'AdGroupName': 'adgroupname', 'AdId': 'adid',
                   'AdNetworkType': 'adnetworktype', 'Age': 'age', 'AvgClickPosition': 'avgclickposition',
                   'AvgCpc': 'avgcpc', 'AvgCpm': 'avgcpm', 'AvgEffectiveBid': 'avgeffectivebid',
                   'AvgImpressionFrequency': 'avgimpressionfrequency', 'AvgImpressionPosition': 'avgimpressionposition',
                   'AvgPageviews': 'avgpageviews', 'AvgTrafficVolume': 'avgtrafficvolume',
                   'BounceRate': 'bouncerate', 'Bounces': 'bounces',
                   'CampaignId': 'campaignid', 'CampaignName': 'campaignname', 'CampaignUrlPath': 'campaignurlpath',
                   'CampaignType': 'campaigntype', 'CarrierType': 'carriertype', 'Clicks': 'clicks',
                   'ClickType': 'clicktype', 'ClientLogin': 'clientlogin', 'ConversionRate': 'conversionrate',
                   'Conversions': 'conversions', 'Cost': 'cost', 'CostPerConversion': 'costperconversion',
                   'Criteria': 'criteria', 'CriteriaId': 'criteriaid', 'CriteriaType': 'criteriatype',
                   'Criterion': 'criterion', 'CriterionId': 'criterionid', 'CriterionType': 'criteriontype',
                   'Ctr': 'ctr',
                   'Date': 'date', 'Device': 'device',
                   'ExternalNetworkName': 'externalnetworkname',
                   'Gender': 'gender', 'GoalsRoi': 'goalsroi',
                   'ImpressionReach': 'impressionreach', 'Impressions': 'impressions',
                   'ImpressionShare': 'impressionshare',
                   'IncomeGrade': 'incomegrade',
                   'LocationOfPresenceId': 'locationofpresenceid', 'LocationOfPresenceName': 'locationofpresencename',
                   'MatchedKeyword': 'matchedkeyword', 'MatchType': 'matchtype', 'MobilePlatform': 'mobileplatform',
                   'Month': 'month',
                   'Placement': 'placement', 'Profit': 'profit',
                   'Quarter': 'quarter', 'Query': 'query',
                   'Revenue': 'revenue', 'RlAdjustmentId': 'rladjustmentid',
                   'Sessions': 'sessions', 'Slot': 'slot',
                   'TargetingCategory': 'targetingcategory', 'TargetingLocationId': 'targetinglocationid',
                   'TargetingLocationName': 'targetinglocationname',
                   'Week': 'week', 'WeightedCtr': 'weightedctr', 'WeightedImpressions': 'weightedimpressions',
                   'Year': 'year'}
        data = pd.read_csv(file, sep='\t', header=1, skipfooter=0,
                           # engine='python'
                           )
        name = str(pd.read_csv(file, sep='\t', header=0, nrows=0).columns[0])
        report_id = (name.split('-')[0]).upper()
        data.rename(columns=columns, inplace=True)
        data['report_id'] = report_id

        data['login'] = login

        return data

    def make_dataset(self, path):
        """
        Собирает датасет
        """
        dtypes = {'adformat': 'object', 'adgroupid': 'object', 'adgroupname': 'object', 'adid': 'object',
                  'adnetworktype': 'object', 'age': 'object', 'avgclickposition': 'float64', 'avgcpc': 'float64',
                  'avgcpm': 'float64', 'avgeffectivebid': 'float64', 'avgimpressionfrequency': 'float64',
                  'avgimpressionposition': 'float64', 'avgpageviews': 'float64', 'avgtrafficvolume': 'float64',
                  'bouncerate': 'float64', 'bounces': 'float64',
                  'campaignid': 'object', 'campaignname': 'object', 'campaignurlpath': 'object',
                  'campaigntype': 'object', 'carriertype': 'object', 'clicks': 'float64', 'clicktype': 'object',
                  'clientlogin': 'object', 'conversionrate': 'float64', 'conversions': 'float64', 'cost': 'float64',
                  'costperconversion': 'float64', 'criteria': 'object', 'criteriaid': 'object',
                  'criteriatype': 'object', 'criterion': 'object', 'criterionid': 'object', 'criteriontype': 'object',
                  'ctr': 'float64',
                  'date': 'datetime', 'device': 'object',
                  'externalnetworkname': 'object',
                  'gender': 'object', 'goalsroi': 'float64',
                  'impressionreach': 'float64', 'impressions': 'float64', 'impressionshare': 'object',
                  'incomegrade': 'object',
                  'locationofpresenceid': 'object', 'locationofpresencename': 'object',
                  'matchedkeyword': 'object', 'matchtype': 'object', 'mobileplatform': 'object', 'month': 'datetime',
                  'placement': 'object', 'profit': 'float64',
                  'quarter': 'datetime', 'query': 'object',
                  'revenue': 'float64', 'rladjustmentid': 'object', 'report_id': 'object',
                  'sessions': 'float64', 'slot': 'object',
                  'targetingcategory': 'object', 'targetinglocationid': 'object', 'targetinglocationname': 'object',
                  'week': 'datetime', 'weightedctr': 'float64', 'weightedimpressions': 'float64',
                  'year': 'datetime',
                  'login': 'object'}

        rep_data = []
        for folder in os.listdir(path):
            files = (glob.glob(os.path.join(path + '/' + folder, "*.tsv")))
            for file in files:
                rep_data.append(self.read_trans_tsv(file, login=folder))

        dataset = pd.concat(rep_data, axis=0).reset_index().drop('index', axis=1)

        for col in dataset.columns:
            # print(col, dtypes[col])
            if dtypes[col] == 'datetime':
                #         if col == 'date':
                dataset[col] = dataset[col].apply(lambda x: datetime.strptime(x, '%Y-%m-%d').date())
            elif dtypes[col] == 'float64':
                dataset[col] = dataset[col].replace('--', np.nan)
                dataset[col] = dataset[col].astype('float64')
            else:
                dataset[col] = dataset[col].replace('--', np.nan)
                dataset[col] = dataset[col].astype(dtypes[col])
        return dataset

    def add_new_access_data(self,
                            client_id: int,
                            name: str,
                            login: str,
                            token: str,
                            ya_direct_mp_id=16,
                            ya_direct_login_attribute_id=24,
                            ya_direct_token_attribute_id=25,
                            status='Active'):
        """
        Добавляет в базу новые данные для доступа к yandex direct для пользователя
        """
        add_to_acc_list_query = f"INSERT INTO account_list (mp_id, client_id, status_1, name) " \
                                f"VALUES ({ya_direct_mp_id}, {client_id}, '{status}', '{name}')"

        id_query = f"SELECT id FROM account_list " \
                   f"WHERE client_id = {client_id} AND mp_id = {ya_direct_mp_id} AND name = '{name}'"

        try:
            conn = psycopg2.connect(self.db_access)
            q = conn.cursor()
            q.execute(add_to_acc_list_query)
            conn.commit()
            status = q.statusmessage
            q.close()
            conn.close()
        except:
            print('Нет подключения к БД, или нет доступа на выполнение операции')
            return None

        if status is not None:
            try:
                conn = psycopg2.connect(self.db_access)
                q = conn.cursor()
                q.execute(id_query)
                result = q.fetchall()
                print(result)
                conn.close()
            except:
                print('Нет подключения к БД, или нет доступа на выполнение операции')
                return None

            if result is not None:
                id_ = result[0][0]

                add_to_acc_service_data_query = f"INSERT INTO account_service_data (account_id, attribute_id, " \
                                                f"attribute_value) " \
                                                f"VALUES ({id_}, {ya_direct_login_attribute_id}, '{login}'), " \
                                                f"({id_}, {ya_direct_token_attribute_id}, '{token}')"
                try:
                    conn = psycopg2.connect(self.db_access)
                    q = conn.cursor()
                    q.execute(add_to_acc_service_data_query)
                    conn.commit()
                    # status = q.statusmessage
                    q.close()
                    conn.close()
                    return 'OK'
                except:
                    print('Нет подключения к БД, или нет доступа на выполнение операции')
                    return None

    def get_last_date(self):
        """
        Возвращает последнюю дату в таблице статистики
        """
        db_params = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"
        engine = create_engine(db_params)
        query = "SELECT max(date) FROM ya_ads_data"
        try:
            data = pd.read_sql(query, con=engine)
            return data.values[0][0]
        except:
            print('Произошла непредвиденная ошибка')
            return None

    def find_duplicates(self):
        """
        Возвращает индексы дубликатов строк
        """
        query = "SELECT * FROM ya_ads_data ORDER BY date desc"
        data = self.get_data_by_response(query)

        if data is not None:

            cols = list(data.drop('id', axis=1).columns)

            dupl = pd.DataFrame(data.id).join(pd.DataFrame(data.duplicated(subset=cols, keep='last')))
            dupl.columns = ['id', 'duplicate']

            return dupl[dupl.duplicate == True].id.values.tolist()

        else:
            return None

    def delete_duplicates(self, id_list: list):
        """
        Удаляет строки в соответствии со списком индексов
        """
        query = f"DELETE FROM ya_ads_data WHERE id IN {tuple(id_list)}"

        try:
            conn = psycopg2.connect(self.db_access)
            q = conn.cursor()
            q.execute(query)
            conn.commit()
            status = q.statusmessage
            q.close()
            conn.close()
            print(status)
            return status
        except:
            print('Нет подключения к БД, или нет доступа на выполнение операции')
            return None






