# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import unittest
import tempfile

from itertools import product
from os.path import join

from sphinxsearch import Engine
from sphinxsearch.session import SessionFactory
from sphinxsearch.query import Query
from sphinxsearch.tests.utils import (
    get_api, get_servers, get_indexers, get_valid_indexes
)


class Test(unittest.TestCase):

    def test_queries(self):
        import pdb
        # pdb.set_trace()

        index_set = get_valid_indexes()

        for index in index_set:
            qs = Query(index)
            qss = Query(index.get_name())

    def test(self):
        sphinxapi = get_api()
        server_cls_list = get_servers()
        indexer_cls_list = get_indexers()
        index_set_list = [get_valid_indexes()]
        index_set_list += []

        arg_source = product(server_cls_list,
                             indexer_cls_list,
                             index_set_list)

        writed = False

        engine = None

        for server_cls, indexer_cls, index_set in arg_source:
            engine_kwargs = dict(api=sphinxapi, server=server_cls(host='localhost', port=6543),
                                 indexer=indexer_cls(), indexes=index_set)

            engine_kwargs_keys = list(engine_kwargs.keys())
            rng_list = range(len(engine_kwargs_keys))

            for pop_args in [engine_kwargs_keys[:n] for n in rng_list]:
                if not engine or not pop_args:
                    new_engine_kwargs = engine_kwargs.copy()
                    index_set = new_engine_kwargs.pop('indexes')
                    engine = Engine(**new_engine_kwargs)

                    for index_class in index_set[:1]:
                        engine.add_index(index_class)

                    engine.extend_indexes(index_set)

                else:
                    rplc_kwargs = dict([(k, engine_kwargs[k]) for k in pop_args])
                    engine = engine.replace(**rplc_kwargs)

                assert set(engine.indexes) == set(index_set), engine.indexes

                Session = SessionFactory(engine)

                conf = engine.get_conf()
                if not writed:
                    engine.save('sphinx.conf')

    def test_pgsql(self):
        pass


class RealTest(unittest.TestCase):
    def save_engine(self, engine):
        tmp = tempfile.gettempdir()
        config_path = join(tmp, 'sphinx.conf')
        engine.save(config_path)


if __name__ == '__main__':
    unittest.main()
