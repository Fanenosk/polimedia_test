"""
Модуль реализует родительский класс для всех модулей подключения к PostgreSQL.
1. Конструктор переопределяется в дочерних классах под параметры подключения.
2. Функция 'select' обеспечивает декоратор для дочерних классов, реализующий выполнение всех селект запросов.
3. Функция 'insert' обеспечивает декаратор для дочерних классов, реализуеющий выполнение всех записывающих
   или изменяющих запросов с данными и таблицами.
4. Функция 'many_insert' обеспечивает декаратор для дочерних классов, реализуеющий выполнение всех записывающих
   запросов. На подачу идёт список кортежей данных, а сам sql формируется по типу "insert into table(cols) values %s".
"""


import psycopg2
from contextlib import closing
from psycopg2.extras import DictCursor
from psycopg2.extras import execute_values


class DBPostgresql:
    def __init__(self):
        self.dbname = 'polimedia_test'
        self.user = 'test_user'
        self.password = 'test'
        self.host = 'localhost'

    def select_with_headers(function):
        def wrapper(self, *args):
            sql = function(self, *args)
            # Вызывается контекстный менеджер, который создаёт
            # сооединение с БД и курсор. В случае падения запроса или чего либо
            # соединение с курсором будет автоматически закрыто.
            try:
                with closing(psycopg2.connect(dbname=self.dbname,
                                              user=self.user,
                                              password=self.password,
                                              host=self.host)) as conn:
                    with conn.cursor() as cursor:
                        try:
                            conn.autocommit = True
                            cursor.execute(sql)
                            result = cursor.fetchall()
                            return result, [desc.name for desc in cursor.description]
                        finally:
                            conn.close()
            except psycopg2.Error as error:
                print("Ошибка чтения из БД", function.__name__, error)
            return []
        return wrapper

    def insert(function):
        def wrapper(self, **kwargs):
            sql = function(self, **kwargs)
            # Вызывается контекстный менеджер, который создаёт
            # сооединение с БД и курсор. В случае падения запроса или чего либо
            # соединение с курсором будет автоматически закрыто.
            # autocommit служит для предотвращения долгих тразакций, т.е.
            # предотвращения ошибок, блокировки процесса.
            try:
                with closing(psycopg2.connect(dbname=self.dbname,
                                              user=self.user,
                                              password=self.password,
                                              host=self.host)) as conn:
                    with conn.cursor() as cursor:
                        try:
                            cursor.execute(sql)
                            conn.commit()
                        finally:
                            conn.close()
            except psycopg2.Error as error:
                print("Ошибка записи в бд", function.__name__, error)
        return wrapper

    def many_insert(function):
        def wrapper(self, **kwargs):
            sql, data = function(self, **kwargs)
            # Вызывается контекстный менеджер, который создаёт
            # сооединение с БД и курсор. В случае падения запроса или чего либо
            # соединение с курсором будет автоматически закрыто.
            # autocommit служит для предотвращения долгих тразакций, т.е.
            # предотвращения ошибок, блокировки процесса.
            try:
                with closing(psycopg2.connect(dbname=self.dbname,
                                              user=self.user,
                                              password=self.password,
                                              host=self.host)) as conn:
                    with conn.cursor() as cursor:
                        try:
                            conn.autocommit = True
                            execute_values(cursor, sql, data)
                        finally:
                            conn.close()
            except psycopg2.Error as error:
                print("Ошибка записи в бд", function.__name__, error)
        return wrapper

    @insert
    def create_table(self, name_table, columns):
        return "create table if not exists {name_table} ({columns});".format(name_table=name_table,
                                                                             columns=columns)

    @many_insert
    def insert_to_table(self, name_table, columns, data):
        return "insert into {name_table} ({columns}) values %s;".format(name_table=name_table,
                                                                        columns=columns), data

    @select_with_headers
    def get_agr_values(self):
        return "select * from (select 'week' as period_type, " \
               "to_char(date_trunc('week', prodazhi2.sale_date), 'YYYY.MM.DD') as start_date ," \
               "to_char(date_trunc('week', prodazhi2.sale_date) + interval '1 week', 'YYYY.MM.DD') as end_date," \
               "p2.fio as salesman_fio," \
               "p3.fio as chif_fio," \
               "sum(prodazhi2.quantity) as sales_count," \
               "sum(prodazhi2.final_price) as sales_sum," \
               "prodazhi2.item_id as max_overcharge_item," \
               "max((round(prodazhi2.final_price/prodazhi2.quantity/prodazhi2.price::numeric,2)-1)*100) as max_overcharge_percent " \
               "from " \
               "(select * from (select * from prodazhi p " \
               "join tovary t on (p.item_id = t.id) " \
               "where t.is_actual = 1 " \
               "union all select * from prodazhi p " \
               "join uslugi u on (p.item_id = u.id) " \
               "where u.is_actual = 1) as prodazhi1) as prodazhi2," \
               "prodavtsy p2," \
               "otdely o " \
               "join prodavtsy p3 on (o.dep_chif_id = p3.id) " \
               "where date_trunc('week', prodazhi2.sale_date) >= prodazhi2.sdate " \
               "and date_trunc('week', prodazhi2.sale_date) <= prodazhi2.edate " \
               "and prodazhi2.salesman_id = p2.id " \
               "and p2.department_id = o.department_id " \
               "group by prodazhi2.sale_date, p2.fio, p3.fio, prodazhi2.item_id " \
               "union all select 'month' as period_type, " \
               "to_char(date_trunc('month', prodazhi2.sale_date), 'YYYY.MM.DD') as start_date , " \
               "to_char(date_trunc('month', prodazhi2.sale_date) + interval '1 month', 'YYYY.MM.DD') as end_date, " \
               "p2.fio as salesman_fio, " \
               "p3.fio as chif_fio, " \
               "sum(prodazhi2.quantity) as sales_count," \
               "sum(prodazhi2.final_price) as sales_sum," \
               "prodazhi2.item_id as max_overcharge_item," \
               "max((round(prodazhi2.final_price/prodazhi2.quantity/prodazhi2.price::numeric,2)-1)*100) as max_overcharge_percent " \
               "from " \
               "(select * from (select * from prodazhi p " \
               "join tovary t on (p.item_id = t.id) " \
               "where t.is_actual = 1 " \
               "union all select * from prodazhi p " \
               "join uslugi u on (p.item_id = u.id) " \
               "where u.is_actual = 1) as prodazhi1) as prodazhi2," \
               "prodavtsy p2," \
               "otdely o " \
               "join prodavtsy p3 on (o.dep_chif_id = p3.id) " \
               "where date_trunc('month', prodazhi2.sale_date) >= prodazhi2.sdate " \
               "and date_trunc('month', prodazhi2.sale_date) <= prodazhi2.edate " \
               "and prodazhi2.salesman_id = p2.id " \
               "and p2.department_id = o.department_id " \
               "group by prodazhi2.sale_date, p2.fio, p3.fio, prodazhi2.item_id) as result_sql " \
               "order by result_sql.salesman_fio, result_sql.start_date"

