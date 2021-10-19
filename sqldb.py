import logging
import os
import sqlite3
from typing import Iterator, Iterable

import MySQLdb
import MySQLdb._exceptions
import yaml

import sqldb_schema
from generic import AttrDict, OrderedAttrDict
from sqldb_dumpers import DUMPERS, dump_file_fmt, Dumper
from sqldb_schema import TableSchema, get_table_schemas, get_table_schema, table_keys_dict
from sqldb_schema import where_op_value, _quoted

m_logger = logging.getLogger(__name__)

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
        m_logger.info('connected to ' + m_db_path)

    elif driver == 'MySQLdb':
        name, host = name.split('@', 1) if '@' in name else (name, 'localhost')
        m_conn = MySQLdb.connect(host=host, database=name, user=username, password=password, charset='utf8')
        m_logger.info(f'connected to {name}@{host}')

    else:
        raise KeyError(f'unsupported driver: {driver}, only one of: sqlite3, MySQLdb')


def disconnect():
    global m_conn

    m_conn.commit()
    m_conn.close()
    m_logger.debug('closed connection: ' + repr(m_conn))
    m_conn = sqlite3.Connection('')


def init(name: str = '', driver: str = '', username: str = '', password: str = '',
         drop: bool = False, verify: bool = True):
    connect(name=name, driver=driver, username=username, password=password)

    for tname, fields in get_table_schemas().items():
        if fields and drop:
            _drop_create_table(tname)

        load_table_info(tname, verify=verify and not fields)

    m_logger.debug(yaml.dump(get_table_schemas(), default_flow_style=True, width=999))


def fini():
    for tname in get_table_schemas().keys():
        if tname in m_table_columns:
            del m_table_columns[tname]

    disconnect()


def load_table_info(tname: str, verify: bool = True):
    if tname not in m_table_columns:
        driver = str(m_conn).split('.')[0][1:]

        if driver == 'sqlite3':
            cols = m_conn.cursor().execute(f'PRAGMA table_info("{tname}")').fetchall()

        elif driver == '_mysql':
            cols = []

            try:
                cursor = m_conn.cursor()
                cursor.execute(f'SHOW COLUMNS FROM {tname}')
                primary = ''

                for i, col in enumerate(cursor.fetchall()):
                    cols.append(tuple([i] + list(_mysql_types_to_sqlite3(col))))

                    if col[3] == 'PRI':
                        primary = col[0]

                if primary:
                    cols.append((len(cols), '__key__', primary))

                cols = tuple(cols)

            except MySQLdb._exceptions.ProgrammingError as exc:
                if exc.args[0] != 1146:  # not Table '{db}.{table}' doesn't exist
                    raise

                elif verify:
                    raise KeyError('failed getting info for table:', tname)

                else:
                    return None

        else:
            raise TypeError(f'unsupported Db driver: {driver}')

        if cols:
            m_table_columns[tname] = TableColumns(*cols)
            m_logger.debug('loaded info of table: ' + tname)

            if tname not in sqldb_schema.m_table_schemas or not sqldb_schema.m_table_schemas[tname]:
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
    m_logger.info('initialized table: ' + tname)


def create(table, **kwargs) -> TableSchema:
    schema = get_table_schema(table)

    try:
        keys = table_keys_dict(table, kwargs, schema)
        assert not existing(table, **keys), f"{keys} already exists at {table}"

    except KeyError:
        pass

    record = schema.new(**kwargs)
    sql = 'INSERT INTO {} ({}) VALUES ({})'.format(table, *record.for_insert())
    m_logger.debug(sql)
    cursor = m_conn.cursor()
    cursor.execute(sql)
    m_conn.commit()
    m_logger.info(f'created at {table} {repr(record)}')

    return record


def update(table, **kwargs):
    schema = get_table_schema(table)
    keys = table_keys_dict(table, kwargs, schema)

    if not existing(table, **keys):
        raise NameError(f"table {table} is missing {keys}")

    record = schema.new(**kwargs)
    where = schema.new(**keys).for_where(**keys)
    _set = record.for_update(**kwargs)

    sql = f'UPDATE {table} SET {_set} WHERE {where}'
    cursor = m_conn.cursor()
    cursor.execute(sql)
    m_conn.commit()
    m_logger.debug(f'updated at {table} {sql}')


def read(table, **kv) -> TableSchema:
    where = get_table_schema(table).new(**kv).for_where(**kv)
    sql = f"SELECT * FROM {table} WHERE {where}"
    m_logger.debug('reading: ' + sql)

    try:
        values = next(_select(sql))

    except StopIteration:
        raise NameError('missing from {}: {}={} "{}"'.format(table, *list(kv.items())[0], sql))

    record = _new_schema(table, values)
    m_logger.debug(f'read from {table} {repr(record)}')

    return record


def existing(table, by_schema=True, **where) -> bool:
    if by_schema:
        by_schema = get_table_schema(table)
        where_sql = by_schema.new(**where).for_where(**where)

    else:
        where_sql = ' AND '.join(f'{k}{where_op_value(str(v))}'
                                 for k, v in where.items() if v)

    sql = f"SELECT 1 FROM {table} WHERE {where_sql} LIMIT 1"

    try:
        values = next(_select(sql))

    except StopIteration:
        values = None

    except sqlite3.OperationalError as exc:
        values = None

        if str(exc) != f'no such table: {table}':
            raise exc

    exists = values is not None and len(values) > 0
    m_logger.debug(' '.join(f'{k}={v}' for k, v in where.items()) + ' does' if exists else ' does not' + ' exist')

    return exists


def write(table, **kwargs):
    try:
        update(table, **kwargs)

    except NameError:
        create(table, **kwargs)

    except KeyError as exc:
        if exc.args[0] != '__key__':
            raise

        delete(table, lenient=True, **kwargs)
        create(table, **kwargs)


def delete(table, lenient=False, by_schema=True, **where):
    if by_schema:
        by_schema = get_table_schema(table)
        for_where = by_schema.new(**where).for_where(**where)

    else:
        for_where = ' '.join(f"{k}={_quoted(v)}" for k, v in where.items())

    sql = f'DELETE FROM {table}'

    if not lenient and where:
        assert existing(table, **where), f"table {table} is missing {for_where}"

    if where:
        sql += ' WHERE ' + for_where

    cursor = m_conn.cursor()
    cursor.execute(sql)
    m_conn.commit()
    m_logger.debug('Done ' + sql)


def list_table(table, **where) -> Iterator:
    return (_new_schema(table, row) for row in rows(table, **where))


def select(table: str, *columns, by_schema=True, **where) -> Iterable:  # yield row
    sql = f"SELECT {','.join(columns) if columns else '*'} FROM {table}"

    if 'order_by' in where:
        order_by = ' ORDER BY ' + where['order_by']
        del where['order_by']

    else:
        order_by = ''

    if where:
        sql += ' WHERE '

        if by_schema:
            sql += get_table_schema(table).new(**where).for_where(**where)

        else:
            sql += ' '.join(f"{k}={_quoted(v)}" for k, v in where.items())

    sql += order_by

    for row in _select(sql):
        yield row


def _select(sql) -> Iterable:  # yield row
    m_logger.debug(sql)
    m_conn.commit()
    cursor = m_conn.cursor()

    try:
        cursor.execute(sql)

    except Exception as exc:
        raise type(exc)(str(exc) + f' "{sql}"')

    row = cursor.fetchone()

    while row:
        yield row

        row = cursor.fetchone()


def select_join(left: str, right: str, on: str) -> Iterable:  # yield row
    sql = 'SELECT * FROM ' + left + ' LEFT JOIN ' + right + ' ON ' + '{}.{} = {}.{}'.format(left, on, right, on)

    for row in _select(sql):
        yield row


def select_objects(table: str, *columns, **where) -> Iterable:  # (OrderedAttrDict, )
    return (OrderedAttrDict(zip(list(f for f in get_table_schema(table).keys() if not columns or f in columns), row))
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
