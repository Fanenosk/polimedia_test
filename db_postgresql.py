"""
Модуль реализует родительский класс для всех модулей подключения к PostgreSQL.
1. Конструктор переопределяется в дочерних классах под параметры подключения.
2. Функция 'select_with_headers' обеспечивает декоратор для дочерних классов,
   реализующий выполнение всех селект запросов и возвращающий названия колоннок.
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

    def м(function):
        def wrapper(self, *args):
            sql = function(self, *args)
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
        return "select * from " \
               "(select DISTINCT on (table_prodazhi.start_date, table_prodazhi.end_date, " \
               "table_prodazhi.salesman_id) 'month' as period_type," \
               "table_prodazhi.start_date, table_prodazhi.end_date, prodavtsy.fio as salesman_fio , " \
               "shef.fio as chif_fio , table_aggr.sales_count," \
               "table_aggr.sales_sum, table_prodazhi.item_id,table_prodazhi.max_overcharge_percent from (" \
               "(select to_char(date_trunc('month', p.sale_date), 'YYYY.MM.DD') as start_date, " \
               "to_char(date_trunc('month', p.sale_date) + interval '1 month','YYYY.MM.DD') as end_date, *, " \
               "p.final_price/p.quantity - t.price as max_overcharge," \
               "(round(p.final_price/p.quantity/t.price::numeric,2)-1)*100 as max_overcharge_percent from prodazhi p " \
               "join tovary t on (p.item_id = t.id) " \
               "where p.item_id = t.id " \
               "and t.is_actual = 1 " \
               "and p.sale_date >= t.sdate " \
               "and p.sale_date <= t.edate " \
               "union all select to_char(date_trunc('month', p.sale_date),'YYYY.MM.DD') as start_date, " \
               "to_char(date_trunc('month', p.sale_date) + interval '1 month','YYYY.MM.DD') as end_date,*, " \
               "p.final_price/p.quantity - u.price as max_overcharge," \
               "(round(p.final_price/p.quantity/u.price::numeric,2)-1)*100 as max_overcharge_percent from prodazhi p " \
               "join uslugi u on (p.item_id = u.id) " \
               "where p.item_id = u.id " \
               "and u.is_actual = 1 " \
               "and p.sale_date >= u.sdate " \
               "and p.sale_date <= u.edate) as table_prodazhi " \
               "join (select to_char(date_trunc('month', p2.sale_date),'YYYY.MM.DD') as start_date, " \
               "to_char(date_trunc('month', p2.sale_date) + interval '1 month','YYYY.MM.DD') as end_date," \
               "salesman_id,sum(p2.quantity) as sales_count,sum(p2.final_price) as sales_sum from prodazhi p2 " \
               "group by date_trunc('month', p2.sale_date), p2.salesman_id) as table_aggr " \
               "on (table_aggr.salesman_id = table_prodazhi.salesman_id) " \
               "and (table_aggr.start_date = table_prodazhi.start_date) " \
               "and (table_aggr.end_date = table_prodazhi.end_date))," \
               "prodavtsy " \
               "join otdely on (prodavtsy.department_id  = otdely.department_id) " \
               "join prodavtsy shef on (otdely.dep_chif_id = shef.id) " \
               "where table_prodazhi.salesman_id = prodavtsy.id " \
               "order by table_prodazhi.start_date, table_prodazhi.end_date, table_prodazhi.salesman_id, " \
               "table_prodazhi.max_overcharge desc) as result_table " \
               "union all " \
               "select * from " \
               "(select DISTINCT on (table_prodazhi.start_date, table_prodazhi.end_date, table_prodazhi.salesman_id) " \
               "'week' as period_type, " \
               "table_prodazhi.start_date, table_prodazhi.end_date, prodavtsy.fio as salesman_fio , " \
               "shef.fio as chif_fio , table_aggr.sales_count, " \
               "table_aggr.sales_sum, table_prodazhi.item_id,table_prodazhi.max_overcharge_percent from (" \
               "(select to_char(date_trunc('week', p.sale_date), 'YYYY.MM.DD') as start_date, " \
               "to_char(date_trunc('week', p.sale_date) + interval '1 week','YYYY.MM.DD') as end_date, *, " \
               "p.final_price/p.quantity - t.price as max_overcharge," \
               "(round(p.final_price/p.quantity/t.price::numeric,2)-1)*100 as max_overcharge_percent from prodazhi p " \
               "join tovary t on (p.item_id = t.id) " \
               "where p.item_id = t.id " \
               "and t.is_actual = 1 " \
               "and p.sale_date >= t.sdate " \
               "and p.sale_date <= t.edate " \
               "union all select to_char(date_trunc('week', p.sale_date),'YYYY.MM.DD') as start_date, " \
               "to_char(date_trunc('week', p.sale_date) + interval '1 week','YYYY.MM.DD') as end_date,*, " \
               "p.final_price/p.quantity - u.price as max_overcharge, " \
               "(round(p.final_price/p.quantity/u.price::numeric,2)-1)*100 as max_overcharge_percent from prodazhi p " \
               "join uslugi u on (p.item_id = u.id) " \
               "where p.item_id = u.id and u.is_actual = 1 " \
               "and p.sale_date >= u.sdate " \
               "and p.sale_date <= u.edate) as table_prodazhi " \
               "join (select to_char(date_trunc('week', p2.sale_date),'YYYY.MM.DD') as start_date, " \
               "to_char(date_trunc('week', p2.sale_date) + interval '1 week','YYYY.MM.DD') as end_date, salesman_id," \
               "sum(p2.quantity) as sales_count,sum(p2.final_price) as sales_sum from prodazhi p2 " \
               "group by date_trunc('week', p2.sale_date), p2.salesman_id) as table_aggr " \
               "on (table_aggr.salesman_id = table_prodazhi.salesman_id) " \
               "and (table_aggr.start_date = table_prodazhi.start_date) " \
               "and (table_aggr.end_date = table_prodazhi.end_date)),prodavtsy " \
               "join otdely on (prodavtsy.department_id  = otdely.department_id) " \
               "join prodavtsy shef on (otdely.dep_chif_id = shef.id) " \
               "where table_prodazhi.salesman_id = prodavtsy.id " \
               "order by table_prodazhi.start_date, table_prodazhi.end_date, table_prodazhi.salesman_id, " \
               "table_prodazhi.max_overcharge desc) as result_table " \
               "order by salesman_fio, start_date"

