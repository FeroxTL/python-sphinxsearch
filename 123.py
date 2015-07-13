# -*- coding: utf-8 -*-
import six
from random import randint


REPR_OUTPUT_SIZE = 2


class QueryBackend(object):
    def __init__(self):
        super(QueryBackend, self).__init__()
        self.filters = {}
        self.offset = 0
        self.limit = 10000

    def add_filter(self, attr_name, filter_op):
        self.filters[attr_name] = filter_op

    def set_limits(self, offset, limit):
        self.offset = offset
        self.limit = limit


def clone_method(method):
    def wrapper(self):
        self._clear_cache()
        return self
    return wrapper


class Query(object):
    def __init__(self):
        super(Query, self).__init__()
        self.query = QueryBackend()
        self._result_cache = None

    def _clear_cache(self):
        self._result_cache = None

    def _populate(self):
        if self._result_cache is None:
            self._result_cache = [randint(1, 10) for x in range(randint(5, 15))]

    @clone_method
    def filter(self, *args, **kwargs):
        self.query.add_filter('a', 'b')

    def __len__(self, *args, **kwargs):
        self._populate()
        return len(self._result_cache)

    def __repr__(self):
        data = list(self[:REPR_OUTPUT_SIZE + 1])
        if len(data) > REPR_OUTPUT_SIZE:
            data[-1] = "...(remaining elements truncated)..."
        return repr(data)

    def __getitem__(self, k):
        """
        Retrieves an item or slice from the set of results.
        """
        if not isinstance(k, (slice,) + six.integer_types):
            raise TypeError
        assert ((not isinstance(k, slice) and (k >= 0)) or
                (isinstance(k, slice) and (k.start is None or k.start >= 0) and
                 (k.stop is None or k.stop >= 0))), \
            "Negative indexing is not supported."

        if self._result_cache is not None:
            return self._result_cache[k]

        qs = self
        if isinstance(k, slice):
            if k.start is not None:
                start = int(k.start)
            else:
                start = None
            if k.stop is not None:
                stop = int(k.stop)
            else:
                stop = None
            qs.query.set_limits(start, stop)
            return list(qs)[::k.step] if k.step else qs

        qs.query.set_limits(k, k + 1)
        return list(qs)[0]


if __name__ == '__main__':
    q = Query()
    print(len(q))
    print(q.filter())
    print(len(q))

    print('---------')

    q = q.filter()
    print(list(q))
    print(q)
    print(len(q))
    print(q[2])
