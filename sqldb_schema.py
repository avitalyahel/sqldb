from generic import OrderedAttrDict


class TableSchema(OrderedAttrDict):

    def __str__(self):
        return ','.join('{} {}'.format(k, v) for k, v in self.items() if k != '__key__')

    def __repr__(self):
        if '__key__' in self:
            return '{}: {}, {}'.format(
                self.__key__, self[self.__key__],
                ', '.join(': '.join([k, v]) for k, v in self.items() if k not in [self.__key__, '__key__'] and v))

        else:
            return ', '.join(': '.join([k, v]) for k, v in self.items() if v)

    def new(self, **kwargs):
        result = TableSchema((k, PYTYPES[v]() if v in PYTYPES else v) for k, v in self.items())
        result.update(dict((k, _empty(v)) for k, v in kwargs.items() if k in result))
        return result

    def for_insert(self):
        cols, vals = zip(*[(k, _quoted(v)) for k, v in self.items() if k != '__key__'])
        return ','.join(cols), ','.join(vals)

    def for_update(self, **kwargs):
        return ', '.join(' = '.join([k, _quoted(v)]) for k, v in self.items() if (v or k in kwargs) and k != '__key__')

    def for_where(self, **kwargs):
        return ' AND '.join(f'{k}{where_op_value(v)}'
                            for k, v in self.items()
                            if (v or k in kwargs) and k != '__key__')


def where_op_value(value: str) -> str:
    if value and value[0] in '><':
        op = f' {value[0]}'
        value = value[1:].strip()

    elif '%' in value:
        op = ' LIKE '

    else:
        op = ' = '

    return f'{op}{_quoted(value)}'


def _quoted(val):
    return "'{}'".format(val.replace("'", "''")) if isinstance(val, str) else _empty(val)


def _empty(val):
    return '' if val is None else str(val)


PYTYPES = dict(
    INT=int,
    TEXT=str,
    REAL=float,
    BLOB=bytes,
    NULL=type(None),
)

m_table_schemas = OrderedAttrDict()


def update_table_schemas(schemas: OrderedAttrDict):
    global m_table_schemas
    m_table_schemas.update(schemas)


def get_table_schemas() -> OrderedAttrDict:
    global m_table_schemas
    return m_table_schemas


def get_table_schema(table: str) -> OrderedAttrDict:
    return get_table_schemas()[table]
