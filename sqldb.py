import os
import sqlite3
from typing import Iterator, Iterable

import MySQLdb

import sqldb_schema
from generic import AttrDict, OrderedAttrDict
from sqldb_dumpers import DUMPERS, dump_file_fmt, Dumper
from sqldb_schema import TableSchema, update_table_schemas, get_table_schemas, get_table_schema, where_op_value, _quoted
from verbosity import verbose, set_verbosity

m_conn = sqlite3.Connection('')
m_db_path = ''
m_table_columns = AttrDict()  # {tname: TableColumns()}


def name() -> str:
    return os.path.basename(m_db_path)


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


def connect(name: str, driver: str = '', username: str = '', password: str = ''):
    global m_conn
    global m_db_path

    if not driver or driver == 'sqlite3':
        m_db_path = os.path.expanduser(name)
        m_conn = sqlite3.connect(m_db_path, check_same_thread=False)
        verbose(2, 'connected to', m_db_path)

    elif driver == 'MySQLdb':
        name, host = name.split('@', 1) if '@' in name else (name, 'localhost')
        m_conn = MySQLdb.connect(host=host, database=name, user=username, password=password)
        verbose(2, f'connected to {name}@{host}')

    else:
        raise KeyError(f'unsupported driver: {driver}, only one of: sqlite3, MySQLdb')

    m_cursor = m_conn.cursor()


def disconnect():
    global m_conn

    m_conn.commit()
    m_conn.close()
    verbose(2, 'closed connection:', repr(m_conn))
    m_conn = sqlite3.Connection('')


def init(name: str = '', driver: str = '', username: str = '', password: str = '', drop: bool = False):
    connect(name=name, driver=driver, username=username, password=password)

    for tname, fields in get_table_schemas().items():
        if fields and drop:
            _drop_create_table(tname)

        load_table_info(tname)


def fini():
    for tname in get_table_schemas().keys():
        if tname in m_table_columns:
            del m_table_columns[tname]

    disconnect()


def load_table_info(tname: str):
    if tname not in m_table_columns:
        driver = str(m_conn).split('.')[0][1:]

        if driver == 'sqlite3':
            cols = m_conn.cursor().execute(f'PRAGMA table_info("{tname}")').fetchall()

        elif driver == '_mysql':
            try:
                cursor = m_conn.cursor()
                cursor.execute(f'SHOW COLUMNS FROM {tname}')
                cols = []
                primary = ''

                for i, col in enumerate(cursor.fetchall()):
                    cols.append(tuple([i] + list(_mysql_types_to_sqlite3(col))))

                    if col[3] == 'PRI':
                        primary = col[0]

                if primary:
                    cols.append((len(cols), '__key__', primary))

                cols = tuple(cols)

            except MySQLdb._exceptions.ProgrammingError as exc:
                if exc.args[0] != 1146:  # Table '{db}.{table}' doesn't exist
                    raise

                return None

        else:
            raise TypeError(f'unsupported Db driver: {driver}')

        if cols:
            m_table_columns[tname] = TableColumns(*cols)
            verbose(2, 'loaded info of table:', tname)

            if driver != 'sqlite3' and (
                    tname not in sqldb_schema.m_table_schemas or not sqldb_schema.m_table_schemas[tname]):
                sqldb_schema.m_table_schemas[tname] = sqldb_schema.TableSchema(
                    *list(tuple(col[1:3]) for col in cols)
                )

        else:
            return None

    return m_table_columns[tname]


def _mysql_types_to_sqlite3(col: tuple) -> tuple:
    _col = list(col)

    if 'int' in _col[1]:
        _col[1] = 'INT'

    elif _col[1].startswith('varchar') or _col[1] == 'datetime' or _col[1] == 'text':
        _col[1] = 'TEXT'

    elif _col[1] == 'double':
        _col[1] = 'REAL'

    return tuple(_col)


def _drop_create_table(tname):
    cursor = m_conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS ' + tname)
    cursor.execute('CREATE TABLE {} ({})'.format(tname, str(get_table_schema(tname))))
    verbose(2, 'initialized table:', tname)


def create(table, **kwargs) -> TableSchema:
    if '__key__' in get_table_schema(table):
        key = get_table_schema(table).__key__
        value = kwargs[key]
        _assert_not_existing(table, **{key: value})

    record = get_table_schema(table).new(**kwargs)
    sql = 'INSERT INTO {} ({}) VALUES ({})'.format(table, *record.for_insert())
    verbose(2, sql)
    cursor = m_conn.cursor()
    cursor.execute(sql)
    m_conn.commit()
    verbose(1, 'created', table[:-1], repr(record))
    return record


def update(table, **kwargs):
    schema = get_table_schema(table)

    if '__key__' in schema:
        key = schema.__key__
        value = kwargs[key]
        _assert_existing(table, **{key: value})
        del kwargs[key]
        where = f'{key}{where_op_value(value)}'
        _set = ','.join('='.join([k, _quoted(v)]) for k, v in kwargs.items())

    else:
        record = get_table_schema(table).new(**kwargs)
        where = record.for_where(**kwargs)
        _set = record.for_update(**kwargs)

    sql = 'UPDATE {} SET {} WHERE {}'.format(table, _set, where)
    cursor = m_conn.cursor()
    cursor.execute(sql)
    m_conn.commit()
    verbose(2, 'updated', table[:-1], sql)


def read(table, **kv) -> TableSchema:
    assert len(kv) == 1, 'expected single key-value pair'
    sql = 'SELECT * FROM {} WHERE {}=\'{}\''.format(table, *list(kv.items())[0])
    verbose(2, 'reading:', sql)
    cursor = m_conn.cursor()
    cursor.execute(sql)
    values = cursor.fetchone()

    if not values:
        raise NameError('missing from {}: {}={}'.format(table, *list(kv.items())[0]))

    record = _new_schema(table, values)
    verbose(2, 'read', table[:-1], repr(record))
    return record


def existing(table, unbounded=False, **where) -> bool:
    schema = get_table_schema(table)

    if '__key__' in schema:
        assert len(where) == 1, 'expected single key-value pair'

    if unbounded:
        where_sql = ' AND '.join(f'{k}{where_op_value(str(v))}'
                                 for k, v in where.items() if v)
    else:
        where_sql = schema.new(**where).for_where(**where)

    sql = 'SELECT 1 FROM {} WHERE {} LIMIT 1'.format(table, where_sql)

    try:
        cursor = m_conn.cursor()
        cursor.execute(sql)
        values = cursor.fetchone()

    except sqlite3.OperationalError as exc:
        values = None

        if str(exc) != f'no such table: {table}':
            raise exc

    exists = values is not None and len(values) > 0
    verbose(2, ' '.join(f'{k}={v}' for k, v in where.items()), 'does' if exists else 'does not', 'exist')

    return exists


def write(table, **kwargs):
    key = get_table_schema(table).__key__
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


def delete(table, lenient=False, **where):
    if not lenient:
        _assert_existing(table, **where)

    sql = f'DELETE FROM {table} WHERE ' + get_table_schema(table).new(**where).for_where(**where)
    cursor = m_conn.cursor()
    cursor.execute(sql)
    m_conn.commit()
    verbose(1, '[v]', sql)


def list_table(table, **where) -> Iterator:
    return (_new_schema(table, row) for row in rows(table, **where))


def select(table: str, *columns, **where) -> Iterable:  # yield row
    sql = 'SELECT {} FROM {}'.format(','.join(columns) if columns else '*', table)

    if where:
        sql += ' WHERE ' + get_table_schema(table).new(**where).for_where(**where)

    for row in _select(sql):
        yield row


def _select(sql) -> Iterable:  # yield row
    verbose(3, sql)

    cursor = m_conn.cursor()
    cursor.execute(sql)
    row = cursor.fetchone()

    while row:
        yield row

        row = cursor.fetchone()


def select_join(left: str, right: str, on: str) -> Iterable:  # yield row
    sql = 'SELECT * FROM ' + left + ' LEFT JOIN ' + right + ' ON ' + '{}.{} = {}.{}'.format(left, on, right, on)

    for row in _select(sql):
        yield row


def select_objects(table: str, *columns, **where) -> Iterable:  # (OrderedAttrDict, )
    return (OrderedAttrDict(zip((f for f in get_table_schema(table).keys() if not columns or f in columns), row))
            for row in select(table, *columns, **where))


def select_join_objects(left: str, right: str, on: str) -> Iterable:  # (OrderedAttrDict, )
    return (
        OrderedAttrDict(zip(list(get_table_schema(left).keys())[:-1] + list(get_table_schema(right).keys())[:-1], row))
        for row in select_join(left, right, on)
    )


def rows(table: str, sep: str = '', **where) -> Iterable:
    return (sep.join(row) if sep else row
            for row in select(table, **where))


def _new_schema(table, values) -> TableSchema:
    return get_table_schema(table).new(**dict(zip(m_table_columns[table].names, values)))


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
    tables = list(get_table_schemas().keys())

    for table in tables:
        dump_files = [_new_dump_file(out, cwd, table) for out in outs]

        for row in select_objects(table):
            [file.dump(get_table_schema(table).new(**row)) for file in dump_files]

        dumped.extend(_close_dump_files(dump_files))

    return dumped


if __name__ == '__main__':
    set_verbosity(3)

    update_table_schemas(OrderedAttrDict(
        ('Table1', TableSchema(
            ('Field1', 'TEXT'),
            ('Field2', 'INT'),
            ('Field3', 'REAL'),
            ('__key__', 'Field1'),
        )),
        ('Table2', TableSchema(
            ('Field1', 'TEXT'),
            ('Field2', 'INT'),
            ('Field3', 'REAL'),
            ('__key__', 'Field1'),
        )),
        ('si_classifications', None),
    ))

    init(name='dev_movado_db@dev-pontaperta-aurora.pontaperta-app.com', driver='MySQLdb',
         username='movado', password='Yq0ycb0AzS')

    classes = list(select_objects('si_classifications'))
    assert classes

    assert existing('si_classifications', id=classes[0].id)

    assert read('si_classifications', id=classes[0].id)

    fini()

    init(name='/tmp/test.db', drop=True)

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

    assert len(dump('test.yaml', 'test.json', 'test.csv', cwd='/tmp')) == 3 * 3

    assert create('Table2', Field1='hjf', Field3=0.5)
    assert create('Table2', Field1='lmn', Field3=11.11)
    print(sep.join(str(r) for r in select('Table1')))
    print(sep.join(str(r) for r in select('Table2')))
    print(sep.join(str(r) for r in select_join(left='Table1', right='Table2', on='Field3')))

    fini()
