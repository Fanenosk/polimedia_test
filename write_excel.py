import pandas as pd
from db_postgresql import DBPostgresql


def write_to_excel(path_file):
    db = DBPostgresql()
    data_output, headers = db.get_agr_values()
    pd.DataFrame(data_output).to_excel(path_file, header=headers, index=False)


if __name__ == "__main__":
    write_to_excel("Выходные данные.xlsx")

