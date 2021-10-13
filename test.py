import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname).1s: %(message)s  <%(filename)s:%(lineno)d>',
)

from sqldb import *
from sqldb_schema import *

if __name__ == '__main__':
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
        ('Table3', TableSchema(
            ('Field1', 'TEXT'),
            ('Field2', 'INT'),
            ('Field3', 'REAL'),
            ('__key__', 'Field1,Field2'),
        )),
    ))

    init(name='/tmp/test.db', drop=True)

    sep = '\n\t\t'

    assert create('Table1', Field1='abc', Field2=1, Field3=0.5)
    assert read('Table1', Field1='abc').Field3 == '0.5'

    assert create('Table2', Field1='def', Field2=2, Field3=1.5)

    assert read('Table1', Field1='abc')

    assert read('Table2', Field3=1.5)

    update('Table1', Field1='abc', Field3=1.5)
    assert read('Table1', Field1='abc').Field3 == '1.5'

    try:
        update('Table1', Field1='abcd', Field3=2.5)

    except NameError as exc:
        if 'missing' not in str(exc):
            raise

    write('Table2', Field1='ghi', Field2='1', Field3='2.3')
    assert read('Table2', Field1='ghi').Field2 == '1'
    write('Table2', Field1='ghi', Field2='2', Field3='2.3')
    assert read('Table2', Field1='ghi').Field2 == '2'

    assert create('Table1', Field1='tmp').Field1 == 'tmp'

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
    print(sep.strip('\n') + sep.join(str(r) for r in select('Table1')))
    print(sep.strip('\n') + sep.join(str(r) for r in select('Table2')))
    print(sep.strip('\n') + sep.join(str(r) for r in select_join(left='Table1', right='Table2', on='Field3')))

    assert create('Table3', Field1='hij', Field2=1, Field3=1.1).Field3 == '1.1'
    update('Table3', Field1='hij', Field2=1, Field3=2.2)
    assert read('Table3', Field1='hij', Field2=1).Field3 == '2.2'
    write('Table3', Field1='hij', Field2=2, Field3=3.3)
    assert read('Table3', Field1='hij', Field2=2).Field3 == '3.3'
    assert len(list(select('Table3', Field1='hij'))) == 2

    fini()
