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

    def select(function):
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
                    with conn.cursor(cursor_factory=DictCursor) as cursor:
                        try:
                            conn.autocommit = True
                            cursor.execute(sql)
                            result = cursor.fetchall()
                            return result
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

