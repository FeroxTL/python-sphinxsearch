# -*- coding: utf-8 -*-
from __future__ import unicode_literals, print_function

import unittest

from tempfile import gettempdir
from os.path import join

from sphinxsearch.models import (
    Index, Int, String, Bool, TimeStamp, MVA, Float, PgsqlSource
)
from sphinxsearch.tests._base_tests_settings import (
    TEST_ENGINE_SCHEMA_SETTINGS_LIST, TEST_ENGINE_SETTINGS
)


HOST = 'localhost'
PORT = '4321'
TMP_ROOT = join(gettempdir())
MAX_MATCHES = 10000
LOG_DIR = join(TMP_ROOT, 'logs')


def get_indexer():
    from sphinxsearch.engine.indexer import Indexer

    indexer = Indexer()
    indexer.mem_limit = '32M'

    return indexer


def get_server():
    from sphinxsearch import SearchServer

    my_server = SearchServer(host=HOST, port=PORT)

    my_server.read_timeout = 5
    my_server.client_timeout = 300
    my_server.max_children = 0
    my_server.pid_file = join(TMP_ROOT, 'searchd.pid')
    my_server.max_matches = MAX_MATCHES
    my_server.log = join(LOG_DIR, 'searchd.log')
    my_server.workers = 'prefork'
    my_server.preopen_indexes = True
    my_server.seamless_rotate = True

    my_server.set_option('max_filter_values', 8192)

    return my_server


def get_engine(api, server, indexer, models):
    from sphinxsearch.engine import Engine

    engine = Engine()
    engine.api = api
    engine.server = server
    engine.indexer = indexer
    engine.set_conf('sphinx.conf')

    return engine


class AbstractProductsIndex(Index):
    __abstract__ = True
    __source__ = PgsqlSource(host=HOST,
                             port=5432, db='nazya_db',
                             user='nazya', password='pass')

    path = '/var/www/nazya/nazya/var/sphinx/index_data/data_anyshop_products'
    docinfo = 'extern'
    mlock = 0
    morphology = 'stem_enru'
    min_word_len = 2

    charset_type = 'utf-8'
    charset_table = ('0..9, A..Z->a..z, _, a..z, U+410..U+42F->U+430..U+44F, '
                     'U+430..U+44F')
    min_infix_len = 2
    enable_star = 1
    query_info = 'SELECT * FROM "base_nazyaproduct" WHERE id=$id'


class AnyshopProducts(AbstractProductsIndex):
    __abstract__ = True

    nazyacategory_id = Int()
    type = Int()
    seller_id = Int()

    tree_id = Int()
    lft = Int()
    rght = Int()

    name = String()
    orig_name = String()
    item_id = String()
    thumbs = String()
    images = String()
    nazyacategory__item_id = String()

    post_fee = Float()
    current_price = Float()
    orig_price = Float()

    in_stock = Bool()

    modified_at = TimeStamp()
    created_at = TimeStamp()

    property_values_ids = MVA(
        Int,
        query='SELECT "base_nazyaproduct_property_values"."nazyaproduct_id"')


class RakutenProducts(AnyshopProducts):
    pass


class BaseTests(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        import sphinxapi

        self.server = get_server()
        self.api = sphinxapi
        self.indexer = get_indexer()

    @property
    def engine(self):
        engine = get_engine(self.api, self.server, self.indexer, set())
        conf_file_path = join(TMP_ROOT, 'sphinx.conf')
        engine.set_conf(conf_file_path)
        return engine

    @property
    def engine_with_schema(self):
        engine = self.engine
        engine.add_index(RakutenProducts)
        engine.extend_indexes((AnyshopProducts, AbstractProductsIndex))

        return engine

    @property
    def local_engine(self):
        engine = self.engine
        engine.set_conf('sphinx.conf')
        engine.save()
        return engine

    def test_index_name(self):
        self.assertEqual(
            (RakutenProducts.__sourcename__,),
            RakutenProducts.get_index_names())

    def test_server_start(self):
        engine = self.local_engine

        self.assertEqual(
            engine.commands.status(),
            'searchd --config sphinx.conf --status')

        self.assertEqual(
            engine.commands.start(),
            'searchd --config sphinx.conf --start')

        LOGDEBUGS = [
            'searchd --config sphinx.conf --start',
            'searchd --config sphinx.conf --start --logdebug',
            'searchd --config sphinx.conf --start --logdebugv',
            'searchd --config sphinx.conf --start --logdebugvv',
        ]

        for i in range(0, 4):
            self.assertEqual(
                engine.commands.start(logdebug=i),
                LOGDEBUGS[i])

        self.assertEqual(
            engine.commands.start(index=RakutenProducts),
            'searchd --config sphinx.conf --start --index '
            '{}'.format(RakutenProducts.__sourcename__))

        self.assertEqual(
            engine.commands.start(index='main'),
            'searchd --config sphinx.conf --start --index main')

        self.assertEqual(
            engine.commands.start(port=1234),
            'searchd --config sphinx.conf --start --port 1234')

        self.assertEqual(
            engine.commands.start(listen='localhost:4321:mysql41'),
            'searchd --config sphinx.conf --start --listen '
            'localhost:4321:mysql41')

        self.assertRaises(
            TypeError,
            lambda: engine.commands.start(listen='localhost:4321:mysql41',
                                          port=1234))

    def test_server_stop(self):
        engine = self.local_engine

        self.assertEqual(
            engine.commands.stop(),
            'searchd --config sphinx.conf --stop')

        self.assertEqual(
            engine.commands.stop(block=True),
            'searchd --config sphinx.conf --stop --stopwait')

        self.assertEqual(
            engine.commands.stop(block=True, pidfile='/tmp/custom.pid'),
            'searchd --config sphinx.conf --stop --stopwait'
            ' --pidfile /tmp/custom.pid')

    def test_server_restart(self):
        engine = self.local_engine

        self.assertEqual(
            engine.commands.restart(),
            'searchd --config sphinx.conf --stop --stopwait ; searchd '
            '--config sphinx.conf --start')

        self.assertEqual(
            engine.commands.restart(pidfile='/tmp/custom.pid'),
            'searchd --config sphinx.conf --stop --stopwait --pidfile '
            '/tmp/custom.pid ; searchd --config sphinx.conf --start '
            '--pidfile /tmp/custom.pid')

        self.assertEqual(
            engine.commands.restart(pidfile='/tmp/custom.pid',
                                    new_pidfile='/tmp/custom_new.pid',
                                    logdebug=3),
            'searchd --config sphinx.conf --stop --stopwait --pidfile '
            '/tmp/custom.pid ; searchd --config sphinx.conf --start '
            '--logdebugvv --pidfile /tmp/custom_new.pid'
            )

    def test_indexer(self):
        engine = self.local_engine

        self.assertEqual(
            engine.commands.reindex(RakutenProducts),
            'indexer --config sphinx.conf {} --rotate'.format(
                RakutenProducts.__sourcename__))

        self.assertEqual(
            engine.commands.reindex(all=True, sighup_each=True),
            'indexer --config sphinx.conf --all --sighup-each --rotate'
            )

        self.assertEqual(
            engine.commands.buildstops(RakutenProducts,
                                       outputfile='/tmp/stops.txt',
                                       limit=100),
            'indexer --config sphinx.conf {} '
            '--buildstops /tmp/stops.txt 100'.format(
                RakutenProducts.__sourcename__))

        self.assertEqual(
            engine.commands.buildstops(RakutenProducts,
                                       outputfile='/tmp/stops.txt',
                                       limit=100,
                                       freqs=True),
            'indexer --config sphinx.conf {} '
            '--buildstops /tmp/stops.txt 100 --buildfreqs'.format(
                RakutenProducts.__sourcename__)
            )

    def test_indexer_merge(self):
        engine = self.local_engine

        self.assertEqual(
            engine.commands.merge(RakutenProducts, deleted=0),
            'indexer --config sphinx.conf --merge '
            '{} --merge-dst-range deleted 0 0 '
            '--rotate'.format(RakutenProducts.__sourcename__))

    def test_conf(self):
        engine = self.local_engine

        self.assertEqual(
            engine.create_config(),
            TEST_ENGINE_SETTINGS)

        engine.save()

        engine.set_conf('sphinx.conf')
        self.assertEquals(engine.commands.get_conf(), 'sphinx.conf')

        self.assertEqual(
            engine.commands.buildstops(RakutenProducts,
                                       'arena_products',
                                       outputfile='tmp/bar.txt',
                                       limit=1000,
                                       freqs=True),
            'indexer --config sphinx.conf {} '
            'arena_products --buildstops tmp/bar.txt 1000 '
            '--buildfreqs'.format(RakutenProducts.__sourcename__))

    def test_session(self):
        engine = self.local_engine
        session = engine.get_session()

        import pdb
        pdb.set_trace()

    def test_engine_indexes(self):
        engine = self.engine_with_schema

        config = engine.create_config()
        parts = [part.format(RakutenProducts.__sourcename__)
                 for part in TEST_ENGINE_SCHEMA_SETTINGS_LIST]

        for part in parts:
            self.assertTrue(part in config)


if __name__ == '__main__':
    unittest.main()
