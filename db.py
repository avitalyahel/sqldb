import os
import sqlite3
from typing import Iterator, Iterable

from db_dumpers import DUMPERS, dump_file_fmt, Dumper
from db_schema import TABLE_SCHEMAS, TableSchema
from generic import AttrDict, OrderedAttrDict
from verbosity import verbose, set_verbosity

g_conn = None
g_db_path = ''
g_table_columns = AttrDict()  # {tname: TableColumns()}


def name() -> str:
    return os.path.basename(g_db_path)


class TableColumns(object):

    def __init__(self, *args, sep='|'):
        self._sep = sep
        self._cols = args

    def __repr__(self):
        return self._sep.join(self.names)

    @property
    def sep(self):
        return self._sep

    @sep.setter
    def sep(self, value):
        self._sep = value

    @property
    def names(self):
        return self._extract(1)

    def _extract(self, index):
        return (col[index] for col in self._cols)


def connect(path: str):
    global g_conn

    if g_conn is None:
        global g_db_path
        g_db_path = os.path.expanduser(path)
        g_conn = sqlite3.connect(g_db_path, check_same_thread=False)
        verbose(2, 'connected to', g_db_path)


def disconnect():
    global g_conn

    if g_conn is not None:
        g_conn.commit()
        g_conn.close()
        verbose(2, 'closed connection to:', g_db_path)
        g_conn = None


def init(path: str = '', drop: bool = False):
    connect(path)

    for tname in TABLE_SCHEMAS.keys():
        if drop or not load_table_info(tname):
            _drop_create_table(tname)
            load_table_info(tname)


def fini():
    for tname in TABLE_SCHEMAS.keys():
        if tname in g_table_columns:
            del g_table_columns[tname]

    disconnect()


def load_table_info(tname):
    if tname not in g_table_columns:
        cols = g_conn.cursor().execute('PRAGMA table_info("{}")'.format(tname)).fetchall()

        if cols:
            g_table_columns[tname] = TableColumns(*cols)
            verbose(2, 'loaded info of table:', tname)

        else:
            return None

    return g_table_columns[tname]


def _drop_create_table(tname):
    cur = g_conn.cursor()
    cur.execute('DROP TABLE IF EXISTS ' + tname)
    cur.execute('CREATE TABLE {} ({})'.format(tname, str(TABLE_SCHEMAS[tname])))
    verbose(2, 'initialized table:', tname)


def create(table, **kwargs):
    key = TABLE_SCHEMAS[table].__key__
    value = kwargs[key]
    _assert_not_existing(table, **{key: value})

    record = TABLE_SCHEMAS[table].new(**kwargs)
    sql = 'INSERT INTO {} ({}) VALUES ({})'.format(table, *record.for_insert())
    verbose(2, sql)
    g_conn.cursor().execute(sql)
    g_conn.commit()
    verbose(1, 'created', table[:-1], repr(record))
    return record


def update(table, **kwargs):
    key = TABLE_SCHEMAS[table].__key__
    value = kwargs[key]
    _assert_existing(table, **{key: value})

    record = TABLE_SCHEMAS[table].new(**kwargs)
    sql = 'UPDATE {} SET {} WHERE {}=\'{}\''.format(table, record.for_update(**kwargs), key, value)
    g_conn.cursor().execute(sql)
    g_conn.commit()
    verbose(2, 'updated', table[:-1], repr(record))


def read(table, **kv) -> TableSchema:
    assert len(kv) == 1, 'expected single key-value pair'
    sql = 'SELECT * FROM {} WHERE {}=\'{}\''.format(table, *list(kv.items())[0])
    verbose(2, 'reading:', sql)
    values = g_conn.cursor().execute(sql).fetchone()

    if not values:
        raise NameError('missing from {}: {}={}'.format(table, *list(kv.items())[0]))

    record = _new_schema(table, values)
    verbose(2, 'read', table[:-1], repr(record))
    return record


def existing(table, **kv) -> bool:
    assert len(kv) == 1, 'expected single key-value pair'

    key, value = list(kv.items())[0]
    sql = 'SELECT 1 FROM {} WHERE {}=\'{}\' LIMIT 1'.format(table, key, value)
    values = g_conn.cursor().execute(sql).fetchone()
    exists = values is not None and len(values) > 0
    verbose(2, key, value, 'does' if exists else 'does not', 'exist')
    return exists


def write(table, **kwargs):
    key = TABLE_SCHEMAS[table].__key__
    value = kwargs[key]

    if existing(table, **{key: value}):
        update(table, **kwargs)

    else:
        create(table, **kwargs)


def _assert_existing(table, **kv):
    if not existing(table, **kv):
        raise NameError('missing from {}: {}={}'.format(table, *list(kv.items())[0]))


def _assert_not_existing(table, **kv):
    if existing(table, **kv):
        raise NameError('already exists in {}: {}={}'.format(table, *list(kv.items())[0]))


def delete(table, lenient=False, **kv):
    if not lenient:
        _assert_existing(table, **kv)

    col, value = list(kv.items())[0]
    sql = 'DELETE FROM {} WHERE {} {} \'{}\''.format(table, col, 'LIKE' if '%' in value else '=', value)
    g_conn.cursor().execute(sql)
    g_conn.commit()
    verbose(1, '[v]', sql)


def list_table(table, **where) -> Iterator:
    return (_new_schema(table, row) for row in rows(table, **where))


def select(table: str, *columns, **where) -> Iterable:  # yield row
    sql = 'SELECT {} FROM {}'.format(','.join(columns) if columns else '*', table)

    if where:
        sql += ' WHERE ' + TABLE_SCHEMAS[table].new(**where).for_where(**where)

    for row in _select(sql):
        yield row


def _select(sql) -> Iterable:  # yield row
    verbose(3, sql)
    cursor = g_conn.cursor().execute(sql)

    row = cursor.fetchone()

    while row:
        yield row

        row = cursor.fetchone()


def select_join(left: str, right: str, on: str) -> Iterable:  # yield row
    sql = 'SELECT * FROM ' + left + ' LEFT JOIN ' + right + ' ON ' + '{}.{} = {}.{}'.format(left, on, right, on)

    for row in _select(sql):
        yield row


def select_objects(table: str, *columns, **where) -> Iterable:  # (OrderedAttrDict, )
    return (OrderedAttrDict(zip((k for k in TABLE_SCHEMAS[table].keys() if not columns or k in columns), row))
            for row in select(table, *columns, **where))


def select_join_objects(left: str, right: str, on: str) -> Iterable:  # (OrderedAttrDict, )
    return (OrderedAttrDict(zip(list(TABLE_SCHEMAS[left].keys())[:-1] + list(TABLE_SCHEMAS[right].keys())[:-1], row))
            for row in select_join(left, right, on))


def rows(table: str, sep: str = '', **where) -> Iterable:
    return (sep.join(row) if sep else row
            for row in select(table, **where))


def _new_schema(table, values) -> TableSchema:
    return TABLE_SCHEMAS[table].new(**dict(zip(g_table_columns[table].names, values)))


def _new_dump_file(out: str, cwd: str = '', table: str = '') -> Dumper:
    fmt = dump_file_fmt(out)
    return DUMPERS[fmt](name=dump_file_path(out, cwd, table))


def dump_file_path(out: str, cwd: str = '', table: str = '') -> str:
    fmt = dump_file_fmt(out)
    return os.path.join(cwd, out.replace(fmt, table + '.' + fmt))


def _close_dump_files(files: [Dumper]) -> [str]:
    closed = []

    for file in files:
        file.close()
        closed.append(file.name)

    return closed


def dump(*outs, cwd: str = '') -> [str]:
    assert outs, 'expected one or more out files, formats: ' + ', '.join(DUMPERS.keys())
    dumped = []
    tables = list(TABLE_SCHEMAS.keys())

    for table in tables:
        dump_files = [_new_dump_file(out, cwd, table) for out in outs]

        for row in select_objects(table):
            [file.dump(TABLE_SCHEMAS[table].new(**row)) for file in dump_files]

        dumped.extend(_close_dump_files(dump_files))

    return dumped


if __name__ == '__main__':
    set_verbosity(3)
    init(path='/tmp/test.db', drop=True)
    sep = '\n\t\t'

    assert create('Table1', Field1='abc', Field2=1, Field3=0.5)

    assert create('Table2', Field1='def', Field2=2, Field3=1.5)

    assert read('Table1', Field1='abc')

    assert read('Table2', Field3=1.5)

    assert create('Table1', Field1='tmp')

    delete('Table1', Field1='tmp')

    try:
        read('Table1', Field1='tmp')

    except NameError as exc:
        if 'missing' not in str(exc):
            raise

    assert existing('Table1', Field2=1)
    assert not existing('Table2', Field3=2.5)

    assert create('Table1', Field1='xyz', Field2=1, Field3=11.11)
    assert len(list(select('Table1', Field2=1))) == 2

    assert len(dump('test.yaml', 'test.json', 'test.csv', cwd='/tmp')) == 3 * 2

    assert create('Table2', Field1='hjf', Field3=0.5)
    assert create('Table2', Field1='lmn', Field3=11.11)
    print('\n'.join(str(r) for r in select('Table1')))
    print('\n'.join(str(r) for r in select('Table2')))
    print('\n'.join(str(r) for r in select_join(left='Table1', right='Table2', on='Field3')))
