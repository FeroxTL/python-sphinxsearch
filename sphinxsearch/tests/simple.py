# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import unittest
import os
import shutil

from sphinxsearch.tests.base_tests import (
    get_server, get_indexer, get_engine,
    SPHINX_ROOT, LOG_DIR, INDEX_DIR
)


try:
    from settings_local import RakutenProducts
except ImportError:
    from sphinxsearch.tests.base_tests import RakutenProducts


class SimpleTests(unittest.TestCase):
    def setUp(self):
        self.server = get_server()
        self.api = __import__('sphinxapi')
        self.indexer = get_indexer()

    @property
    def engine(self):
        engine = get_engine(self.api, self.server, self.indexer, set())
        conf_file_path = 'sphinx.conf'
        engine.set_conf(conf_file_path)
        return engine

    def test(self):
        # clear sphinx directory
        if os.path.exists(SPHINX_ROOT):
            shutil.rmtree(SPHINX_ROOT)

        # create needed directories
        for path in [SPHINX_ROOT, LOG_DIR, INDEX_DIR]:
            os.mkdir(path)

        engine = self.engine
        engine.add_index(RakutenProducts)
        engine.save()

        # reindex
        self.assertEqual(engine.commands.reindex(RakutenProducts).call(), 0)

        # start sphinx
        self.assertEqual(engine.commands.start().call(), 0)

        from sphinxsearch.query import Query

        session = engine.get_session()
        qs = Query(RakutenProducts, session)

        print(qs[3:])

        # stop sphinx
        self.assertEqual(engine.commands.stop().call(), 0)


if __name__ == '__main__':
    unittest.main()
