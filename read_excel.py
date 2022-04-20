from db_postgresql import DBPostgresql
import transliterate
import pandas as pd
import numpy as np
import datetime


def eval_type(values: list, headers: np.ndarray) -> dict:
    type_dict = {pd._libs.tslibs.timestamps.Timestamp: 'timestamp',
                 datetime.datetime: 'timestamp',
                 np.int64: 'int',
                 str: 'text'}
    headers_type = {}
    for head in headers:
        headers_type.update({head: type_dict[type(values[head])]})
    return headers_type


def parse_excel_into_db(path_file: str):
    sheet_names = pd.ExcelFile(path_file).sheet_names
    db = DBPostgresql()
    for name in sheet_names:
        df = pd.read_excel(path_file, sheet_name=name)
        headers = df.columns.ravel()
        headers_type = eval_type(df.iloc[0], headers)
        name_table = transliterate.translit(name, reversed=True)
        db.create_table(name_table=name_table,
                        columns=','.join(list(map(lambda key, item:
                                                  key + ' ' + item, headers_type, headers_type.values())))
                        )
        db.insert_to_table(name_table=name_table,
                           columns=','.join(headers),
                           data=(list(df.itertuples(index=False, name=None)))
                           )


if __name__ == "__main__":
    parse_excel_into_db('Исходные данные.xlsx')

