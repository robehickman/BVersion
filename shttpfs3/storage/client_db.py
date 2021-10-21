import sqlite3, json

#=====================================================
def prep_manifest_result(res):
    for item in res:
        item['last_mod'] = float(item['last_mod'])
        item['created']  = float(item['created'])
    return res

#=====================================================
class client_db:
    def __init__(self, db_file_path):

        print(db_file_path)

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
                data Text
            )
            """)

        self.con.commit()


    # --------------
    def commit(self):
        self.con.commit()
        
# ===================================================
# Handling of journal for jornaling filesystem
# ===================================================
    def get_fs_journal(self):
        res = self.cur.execute("select data, rowid from journal order by rowid")
        journal = []
        for item in res.fetchall():
            item['data'] = json.loads(item['data'])
            journal.append(item)

        return journal

    # --------------
    def write_fs_journal(self, data):

        self.cur.execute("""
            insert into journal
                (data)
                values
                (?)
            """, (json.dumps(data),))

        print(self.get_fs_journal())

    # --------------
    def delete_from_fs_journal(self, j_item):
        self.cur.execute("delete from journal where rowid = ?", (j_item['rowid'],))

    # --------------
    def clear_fs_journal(self):
        self.cur.execute("delete from journal")

# ===================================================
# Handling of file manifest
# ===================================================
    def get_manifest(self):
        res = self.cur.execute("select * from files")
        return prep_manifest_result(res.fetchall())

    # --------------
    def get_single_file_from_manifest(self, path):
        res = self.cur.execute("select * from files where path = ?", (path,))
        res = prep_manifest_result(res.fetchall())
        return None if len(res) == 0 else res[0]

    # --------------
    def add_file_to_manifest(self, file_info):
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

    # --------------
    def remove_file_from_manifest(self, path):
        self.cur.execute("delete from files where path = ?", (path,))
 