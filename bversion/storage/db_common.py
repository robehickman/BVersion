import sqlite3

def init_db(db_file_path):
    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    con = sqlite3.connect(db_file_path)
    con.row_factory = dict_factory

    cur = con.cursor()

    return con, cur
