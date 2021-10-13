import json
import logging
from collections import OrderedDict

import yaml

import generic

m_logger = logging.getLogger(__name__)


class Dumper:

    def __init__(self, name: str = ''):
        self._name = name
        self._file = None

    @property
    def name(self) -> str:
        return self._name

    def dump(self, obj: OrderedDict):
        if not self._file:
            self._file = open(self._name, 'w+')

        self.write(obj)

    def write(self, obj: OrderedDict):
        self._file.write(str(obj) + '\n')

    def close(self):
        if self._file:
            self._file.close()
            m_logger.info('Closed ' + self._file.name)

    def public(self, obj: OrderedDict) -> OrderedDict:
        return OrderedDict((k, v) for k, v in obj.items() if not k.startswith('_'))


class JsonDumper(Dumper):

    def dump(self, obj: OrderedDict):
        if not self._file:
            self._file = open(self._name, 'w+')
            self._file.write('[\n')

        super(JsonDumper, self).dump(obj)

    def write(self, obj: OrderedDict):
        json.dump(self.public(obj), self._file, indent=2)
        self._file.write(',\n')

    def close(self):
        if self._file:
            self._file.write('{}]')

        super(JsonDumper, self).close()


class YamlDumper(Dumper):
    def write(self, obj: OrderedDict):
        key = obj['__key__']
        d = dict(self.public(obj))

        for k, v in d.items():
            if v and '[' in v and ']' in v:
                d[k] = eval(v.replace("''", "\'"))

        yaml.dump({d[key]: d}, self._file, default_flow_style=False, width=999),


class CsvDumper(Dumper):

    def __init__(self, name: str = ''):
        super(CsvDumper, self).__init__(name)
        self._header = True

    def write(self, obj: OrderedDict):
        generic.dump_dicts_to_csv(self._file, [self.public(obj)], header=self._header)
        self._header = False


DUMPERS = dict(
    yaml=YamlDumper,
    csv=CsvDumper,
    json=JsonDumper,
)


def dump_file_fmt(out: str) -> str:
    if '.' not in out:
        raise TypeError('expected out file name with format extension, got: ' + out)

    fmt = out.split('.')[1]

    if fmt not in DUMPERS:
        raise TypeError('unexpected file format: {}, expected: {}'.format(fmt, list(DUMPERS.keys())))

    return fmt
