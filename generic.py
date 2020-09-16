import csv
from collections import OrderedDict
from typing import Iterable
from typing.io import TextIO


class AttrDict(dict):

    def __getattr__(self, item):
        return self[item]

    def __setattr__(self, key, value):
        self[key] = value


class OrderedAttrDict(OrderedDict):

    def __init__(self, *args, **kwargs):
        super(OrderedAttrDict, self).__init__(**kwargs)
        self._add_key_values(args)

    def _add_key_values(self, args):
        for i, arg in enumerate(args):
            if isinstance(arg, tuple) and len(arg) == 2:
                self[arg[0]] = arg[1]

            elif isinstance(arg, Iterable):
                self._add_key_values(arg)

            else:
                raise TypeError('unexpected arg {}: {}, requires tuple of two'.format(i + 1, arg))

    def __repr__(self) -> str:
        return '{' + ', '.join("'{}': '{}'".format(*kv) for kv in self.items()) + '}'

    def __getattr__(self, item):
        return self[item]

    def __setattr__(self, key, value):
        self[key] = value


def dump_dicts_to_csv(f: TextIO, dicts: Iterable, header: bool = True):
    for d in dicts:
        w = csv.DictWriter(f, d)

        if header:
            w.writeheader()
            header = False

        w.writerow(d)
