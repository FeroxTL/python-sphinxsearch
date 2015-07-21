# -*- coding: utf-8 -*-
from __future__ import absolute_import

import select
import socket
from struct import pack, unpack

from . import const


def clone_method(method):
    def wrapper(self, *args, **kwargs):
        return self
    return wrapper


class SphinxClient(object):
    def __init__(self, host='localhost', port=9312, path=None, timeout=1.0):
        self._host = host
        self._port = port
        self._socket = None
        self._path = path
        self._timeout = timeout
        self._offset = 0
        self._limit = 100
        self._maxmatches = 1000
        self._mode = const.SPH_MATCH_ALL
        self._fieldweights = {}
        self._indexweights = {}
        self._reset()

    def __del__(self):
        self._disconnect()

    def _disconnect(self):
        if self._socket:
            self._socket.close()
        self._socket = None

    def _connect(self):
        if self._socket:
            # we have a socket, but is it still alive?
            sr, sw, _ = select.select([self._socket], [self._socket], [], 0)

            # this is how alive socket should look
            if len(sr) == 0 and len(sw) == 1:
                return self._socket

            # oops, looks like it was closed, lets reopen
            self._disconnect()

        try:
            if self._path:
                af = socket.AF_UNIX
                addr = self._path
                desc = self._path
            else:
                af = socket.AF_INET
                addr = (self._host, self._port)
                desc = '%s;%s' % addr
            sock = socket.socket(af, socket.SOCK_STREAM)
            sock.settimeout(self._timeout)
            sock.connect(addr)
        except socket.error, msg:
            if sock:
                sock.close()
            raise RuntimeError('connection to %s failed (%s)' % (desc, msg))

        v = unpack('>L', sock.recv(4))
        if v < 1:
            sock.close()
            raise RuntimeError('expected searchd protocol version, got %s' % v)

        # all ok, send my version
        sock.send(pack('>L', 1))
        return sock

    def _send(self, sock, req):
        total = 0
        while True:
            sent = sock.send(req[total:])
            if sent <= 0:
                break

            total = total + sent

        return total

    def _reset(self):
        self._overrides = {}
        self._filters = []
        self._anchor = {}
        self._groupby = ''
        self._groupfunc = const.SPH_GROUPBY_DAY
        self._groupsort = '@group desc'
        self._groupdistinct = ''

    @clone_method
    def set_limits(self, offset, limit=None, maxmatches=None, cutoff=None):
        """
        Set offset and count into result set, and optionally set max-matches
        and cutoff limits.
        """
        self._offset = offset
        if limit:
            self._limit = limit
        if maxmatches:
            self._maxmatches = maxmatches
        if cutoff:
            self._cutoff = cutoff

    @clone_method
    def set_options(self, match_mode=None, ranking=None, sort_mode=None,
                    id_range=None):
        if match_mode:
            if match_mode not in const.ALL_MATCH_MODES:
                raise Exception('Unknown match mode')
            self._mode = match_mode

        if ranking and len(ranking) == 2:
            if ranking[0] not in const.ALL_RANK_MODES:
                raise Exception('Unknown rank mode')
            self._ranker, self._rankexpr = ranking

        if sort_mode and len(sort_mode) == 2:
            if sort_mode[0] not in const.ALL_SORT_MODES:
                raise Exception('Unknown sort mode')
            self._sort, self._sortby = sort_mode

        if id_range and len(id_range) == 2:
            if not id_range[0] <= id_range[1]:
                raise Exception('Min id must be greater max id')
            self._min_id, self._max_id = id_range

    @clone_method
    def weights(self, field_weights, index_weights):
        if field_weights:
            self._fieldweights = field_weights

        if index_weights:
            self._indexweights = index_weights

    @clone_method
    def filter(self, **kwargs):
        exclude = bool(kwargs.pop('exclude'))
        for key, value in kwargs.items():
            self._filters.append({
                'type': const.SPH_FILTER_VALUES,
                'attr': key,
                'exclude': bool(exclude),
                'values': value,
            })

    @clone_method
    def extra(self, select=None):
        pass


if __name__ == '__main__':
    cl = SphinxClient()
    cl._connect()
    cl.set_limits(5).filter().extra().weights()
    print('OK')
