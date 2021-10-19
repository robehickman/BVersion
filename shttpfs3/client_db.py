import sqlite3

def prep_result(res):
    for item in res:
        item['last_mod'] = float(item['last_mod'])
        item['created']  = float(item['created'])
    return res

class client_db:
    def __init__(self, db_file_path):

        def dict_factory(cursor, row):
            d = {}
            for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
            return d

        con = sqlite3.connect(db_file_path)
        con.row_factory = dict_factory

        cur = con.cursor()

        self.con = con
        self.cur = cur

        # ==========================
        self.cur.execute( """
            create table if not exists meta (
                id int,
                have_revision  Text,
                root           Text,
                format_version Text
            )
            """)

        # ----------
        self.cur.execute( """
            create table if not exists files (
                path     Text unique,
                last_mod Text,
                created  Text,
                server_file_hash Text
            )
            """)

        # ----------
        self.cur.execute( """
            create table if not exists journal (
                comand Text,
                arg1   Text,
                arg2   Text
            )
            """)

        self.con.commit()
        

# ===================================================
# Handling of file manifest
# ===================================================
    def get_manifest(self):
        res = self.cur.execute("select * from files")
        return prep_result(res.fetchall())

    # --------------
    def get_file_info(self, path):
        res = self.cur.execute("select * from files where path = ?", (path,))
        res = prep_result(res.fetchall())
        return None if len(res) == 0 else res[0]

    # --------------
    def add_to_manifest(self, file_info):
        self.cur.execute("""
            insert into files (
                path,
                last_modeifed,
                created,
                server_file_hash
            ) values (
                ?,
                ?,
                ?,
                ?
            ) """, (
                file_info['path'],
                file_info['last_modified'],
                file_info['created'],
                file_info['server_file_hash']))
            