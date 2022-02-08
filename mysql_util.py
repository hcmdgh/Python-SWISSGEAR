import pymysql
import pymysql.cursors
from pymysql.connections import Connection
import typing
from pymysql.err import IntegrityError


class EntryNotFoundError(RuntimeError):
    """
    没有查询到条目时抛出的异常。
    """
    pass


class MultipleEntriesFoundError(RuntimeError):
    """
    查询到多个条目时（如果期望只有一个条目）抛出的异常。
    """
    pass


class TableNotExistError(RuntimeError):
    """
    表不存在时抛出的异常。
    """
    pass


class MySQLConnection:
    def __init__(self, *,
                 host: str,
                 user: str,
                 password: str,
                 port: int = 3306,
                 database: str,
                 charset: str = 'utf8mb4',
                 autocommit: bool = True):
        self.conn = pymysql.connect(
            host=host,
            user=user,
            password=password,
            port=port,
            database=database,
            charset=charset,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=autocommit,
        )
        self.cursor = self.conn.cursor()

    def get_table(self, table_name: str, primary_key: str = 'id') -> 'MySQLTable':
        return MySQLTable(conn=self.conn, table_name=table_name, primary_key=primary_key)

    def commit(self):
        self.conn.commit()

    def __del__(self):
        self.cursor.__exit__()
        self.conn.__exit__()


class MySQLTable:
    def __init__(self, conn: Connection, table_name: str, primary_key: str = 'id'):
        self.conn = conn
        self.cursor = conn.cursor()
        self.table_name = table_name
        self.primary_key = primary_key

    def commit(self):
        self.conn.commit()

    def drop(self):
        self.cursor.execute(f'DROP TABLE IF EXISTS {self.table_name}')

    def clear(self):
        self.cursor.execute(f'TRUNCATE TABLE {self.table_name}')

    def count(self) -> int:
        try:
            self.cursor.execute(f'SELECT COUNT(*) FROM {self.table_name}')
        except pymysql.err.ProgrammingError:
            raise TableNotExistError
        else:
            return self.cursor.fetchone()['COUNT(*)']

    def insert_one(self, entry: dict):
        field_names = list(entry.keys())
        values = list(entry.values())
        part1 = ', '.join(field_names)
        part2 = ', '.join(['%s'] * len(field_names))
        sql = f'INSERT INTO {self.table_name} ({part1}) VALUES ({part2})'

        self.cursor.execute(sql, values)

    def save_one(self, entry: dict):
        id_ = entry[self.primary_key]
        self.delete_by_id(id_)
        self.insert_one(entry)

    def update_one(self, entry: dict):
        id_ = entry[self.primary_key]
        try:
            exist_entry = self.get_by_id(id_)
        except EntryNotFoundError:
            exist_entry = dict()

        new_entry = exist_entry
        for key, value in entry.items():
            if value:
                new_entry[key] = value

        self.save_one(new_entry)

    def get_all(self, page_size: int = 1000, **conditions) -> typing.Iterable[dict]:
        last_id = -1

        if not conditions:
            sql = f'SELECT * FROM {self.table_name} WHERE {self.primary_key} > %s ORDER BY {self.primary_key} LIMIT %s'
        else:
            sql = f'SELECT * FROM {self.table_name} WHERE {" AND ".join([f"{key} = %s" for key in conditions.keys()])} AND {self.primary_key} > %s ORDER BY {self.primary_key} LIMIT %s'

        while True:
            self.cursor.execute(sql, list(conditions.values()) + [last_id, page_size])
            entries = self.cursor.fetchall()
            if not entries:
                break
            for entry in entries:
                last_id = entry[self.primary_key]
                yield entry

    def get_by_id(self, id_: int) -> dict:
        sql = f'SELECT * FROM {self.table_name} WHERE {self.primary_key} = %s'
        self.cursor.execute(sql, [id_])
        entries = self.cursor.fetchall()
        if not entries:
            raise EntryNotFoundError
        elif len(entries) > 1:
            raise MultipleEntriesFoundError
        else:
            return entries[0]

    def delete_by_id(self, id_: int):
        sql = f'DELETE FROM {self.table_name} WHERE {self.primary_key} = %s'
        self.cursor.execute(sql, [id_])
