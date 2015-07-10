# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import unittest

from sphinxsearch.tests.base_tests import BaseTests
from sphinxsearch.tests.simple import SimpleTests
from sphinxsearch.tests.server import ServerTests

if __name__ == '__main__':
    # Base tests
    suite = unittest.TestLoader().loadTestsFromTestCase(BaseTests)
    unittest.TextTestRunner().run(suite)

    # Simple tests
    suite = unittest.TestLoader().loadTestsFromTestCase(SimpleTests)
    unittest.TextTestRunner().run(suite)

    # Server tests
    suite = unittest.TestLoader().loadTestsFromTestCase(ServerTests)
    unittest.TextTestRunner().run(suite)
