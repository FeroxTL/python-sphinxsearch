# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import select
import socket
from six import string_types, integer_types, iteritems
from struct import pack, unpack, unpack_from, calcsize
from re import sub
from collections import namedtuple

# PY3
try:
    xrange
except NameError:
    xrange = range

# TODO: rewrite response parsing to to cStringIO
try:
    from StringIO import StringIO
except ImportError:
    from io import BytesIO as StringIO


from . import const


__all__ = ['escape', 'SphinxClient']


Attr = namedtuple('Attr', ['name', 'type'])


def format_req(val, fmt='>L', len_fmt='>L'):
    if isinstance(val, integer_types):
        return pack(fmt, val)
    if isinstance(val, string_types):
        val = val.encode('utf-8')
        return pack(fmt, len(val)) + val
    if isinstance(val, (dict,)):
        return format_req(len(val), len_fmt) + b''.join([
            format_req(key, fmt) + format_req(value, fmt)
            for key, value in iteritems(val)])
    if isinstance(val, (list,)):
        return format_req(len(val), len_fmt) + b''.join([
            format_req(key, fmt) for key in val])
    raise Exception('Unknown val format')


class SResponse(StringIO):
    def read_int(self, fmt='>L'):
        return unpack(fmt, self.read(calcsize(fmt)))[0]

    def read_str(self, fmt='>L'):
        length = self.read_int(fmt)
        return self.read(length).decode('utf-8')

    def write_fmt(self, val):
        self.write(format_req(val))


def clone_method(method):
    def wrapper(self, *args, **kwargs):
        method(self, *args, **kwargs)
        return self
    return wrapper


def escape(string):
    return sub(r"([=\(\)|\-!@~\"&/\\\^\$\=])", r"\\\1", string)


class SphinxClient(object):
    def __init__(self, host='localhost', port=9312, path=None, timeout=1.0):
        self._host = host
        self._port = port
        self._path = path
        self._socket = None
        self._timeout = timeout
        self._offset = 0
        self._limit = 100
        self._maxmatches = 1000
        self._mode = const.SPH_MATCH_ALL
        self._fieldweights = {}
        self._indexweights = {}
        self._retrycount = 0
        self._retrydelay = 0
        self._maxquerytime = 0
        self._reset()

    def __del__(self):
        self._disconnect()

    def _reset(self):
        self._overrides = {}
        self._filters = []
        self._anchor = {}
        self._groupby = ''
        self._groupfunc = const.SPH_GROUPBY_DAY
        self._groupsort = '@group desc'
        self._groupdistinct = ''
        self._ranker = const.SPH_RANK_PROXIMITY_BM25
        self._sort = const.SPH_SORT_RELEVANCE
        self._sortby = ''
        self._select = '*'
        self._reqs = []
        self._min_id = 0
        self._max_id = 0
        self._cutoff = 0
        self._rankexpr = ''

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
        except socket.error as e:
            if sock:
                sock.close()
            raise RuntimeError('connection to %s failed (%s)' % (desc, e))

        v = unpack('>L', sock.recv(4))
        if v < (1,):
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

    def _get_response(self, sock, client_ver):
        (status, ver, length) = unpack('>2HL', sock.recv(8))
        response = b''
        left = length
        while left > 0:
            chunk = sock.recv(left)
            if chunk:
                response += chunk
                left -= len(chunk)
            else:
                break

        if not self._socket:
            sock.close()

        # check response
        read = len(response)
        if not response or read != length:
            if length:
                raise Exception(
                    'failed to read searchd response (status=%s, ver=%s, '
                    'len=%s, read=%s)' % (status, ver, length, read))
            else:
                raise Exception('received zero-sized searchd response')

        # check status
        if status == const.SEARCHD_WARNING:
            wend = 4 + unpack('>L', response[0:4])[0]
            # TODO: logger
            print(response[4:wend])
            return response[wend:]

        if status == const.SEARCHD_ERROR:
            raise Exception('searchd error: ' + response[4:].decode('utf-8'))

        if status == const.SEARCHD_RETRY:
            raise Exception(
                'temporary searchd error: ' + response[4:].decode('utf-8'))

        if status != const.SEARCHD_OK:
            raise Exception('unknown status code %d' % status)

        # check version
        if ver < client_ver:
            raise Exception(
                'searchd command v.%d.%d older than client\'s v.%d.%d, '
                'some options might not work' % (
                    ver >> 8,
                    ver & 0xff,
                    client_ver >> 8,
                    client_ver & 0xff
                ))

        return response

    def __populate_results(self):
        sock = self._connect()

        req = b''.join(self._reqs)
        length = len(req) + 8

        self._send(sock, pack('>HHLLL', const.SEARCHD_COMMAND_SEARCH,
                   const.VER_COMMAND_SEARCH, length, 0, len(self._reqs)) + req)

        return self._get_response(sock, const.VER_COMMAND_SEARCH)

    def _populate(self):
        response = SResponse(self.__populate_results())
        response.seek(0)

        nreqs = len(self._reqs)

        results = []
        for i in range(0, nreqs):
            result = {}
            results.append(result)

            status = response.read_int()

            result['status'] = status
            if status != const.SEARCHD_OK:
                message = response.read_str()

                if status == const.SEARCHD_WARNING:
                    result['warning'] = message
                else:
                    result['error'] = message
                    continue

            # read schema
            fields = []
            attrs = []

            for x in range(response.read_int()):
                fields.append(response.read_str())

            result['fields'] = fields

            nattrs = response.read_int()
            for x in range(nattrs):
                attrs.append(Attr(response.read_str(), response.read_int()))

            result['attrs'] = attrs

            # read match count
            count = response.read_int()
            id64 = response.read_int()

            # read matches
            result['matches'] = []
            for x in range(count):
                if id64:
                    doc = response.read_int('>Q')
                    weight = response.read_int()
                else:
                    doc = response.read_int()
                    weight = response.read_int()

                match = {'id': doc, 'weight': weight, 'attrs': {}}
                for attr in attrs:
                    if attr.type == const.SPH_ATTR_FLOAT:
                        match['attrs'][attr.name] = response.read_int('>F')
                    elif attr.type == const.SPH_ATTR_BIGINT:
                        match['attrs'][attr.name] = response.read_int('>Q')
                    elif attr.type == const.SPH_ATTR_STRING:
                        match['attrs'][attr.name] = response.read_str()
                    elif attr.type == const.SPH_ATTR_MULTI:
                        match['attrs'][attr.name] = []
                        nvals = response.read_int()
                        for n in range(0, nvals):
                            match['attrs'][attr.name].append(response.read_int())
                    elif attr.type == const.SPH_ATTR_MULTI64:
                        match['attrs'][attr.name] = []
                        nvals = response.read_int()
                        nvals = nvals / 2
                        for n in range(0, nvals):
                            match['attrs'][attr.name].append(response.read_int('>Q'))
                    else:
                        match['attrs'][attr.name] = response.read_int()

                result['matches'].append(match)

            result['total'] = response.read_int()
            result['total_found'] = response.read_int()
            result['time'] = '%.3f' % (response.read_int() / 1000.0)

            result['words'] = []
            for x in range(response.read_int()):
                word = response.read_str()
                docs = response.read_int()
                hits = response.read_int()

                result['words'].append({'word': word, 'docs': docs, 'hits': hits})

        self._reqs = []
        response.close()
        return results

    def __query_base(self):
        req = []

        req.append(pack('>4L', self._offset, self._limit, self._mode,
                        self._ranker))
        if self._ranker == const.SPH_RANK_EXPR:
            req.append(format_req(len(self._rankexpr)))
            req.append(format_req(self._rankexpr))

        return b''.join(req)

    def __query_sort(self):
        return format_req(self._sort) + format_req(self._sortby)

    def __query_query(self, query):
        return format_req(escape(query))

    def __query_weights(self):
        """DEPRECATED"""
        # req.append(pack('>L', len(self._weights)))
        # for w in self._weights:
        #     req.append(pack('>L', w))
        return pack('>L', 0)

    def __query_index(self, index):
        try:
            return format_req(index)
        except UnicodeDecodeError:
            raise RuntimeError('Index must be string')

    def __query_id_range(self):
        # >L is id64 range marker
        return pack('>L2Q', 1, self._min_id, self._max_id)

    def __query_filters(self):
        req = []

        req.append(format_req(len(self._filters)))
        for f in self._filters:
            req.append(format_req(f['attr']))
            filtertype = f['type']
            req.append(pack('>L', filtertype))
            if filtertype == const.SPH_FILTER_VALUES:
                req.append(format_req(f['values'], fmt='>q'))
            elif filtertype == const.SPH_FILTER_RANGE:
                req.append(pack('>2q', f['min'], f['max']))
            elif filtertype == const.SPH_FILTER_FLOATRANGE:
                req.append(pack('>2f', f['min'], f['max']))
            req.append(format_req(f['exclude']))

        return b''.join(req)

    def __query_other(self):
        req = []
        req.append(pack('>L', self._groupfunc))
        req.append(format_req(self._groupby))
        req.append(pack('>L', self._maxmatches))
        req.append(format_req(self._groupsort))
        req.append(pack('>3L', self._cutoff, self._retrycount,
                        self._retrydelay))
        req.append(format_req(self._groupdistinct))
        return b''.join(req)

    def __query_anchor(self):
        """
        Anchor point
        """
        if len(self._anchor) == 0:
            return format_req(0)
        else:
            attrlat, attrlong = self._anchor['attrlat'], self._anchor['attrlong']
            latitude, longitude = self._anchor['lat'], self._anchor['long']
            # TODO: fix
            req = []
            req.append(pack('>L', 1))
            req.append(pack('>L', len(attrlat)) + attrlat)
            req.append(pack('>L', len(attrlong)) + attrlong)
            req.append(pack('>f', latitude) + pack('>f', longitude))
            return b''.join(req)

    def __query_overrides(self):
        """
        Attribute overrides
        """
        req = []
        req.append(pack('>L', len(self._overrides)))
        for v in self._overrides.values():
            req.extend((pack('>L', len(v['name'])), v['name']))
            req.append(pack('>LL', v['type'], len(v['values'])))
            for id, value in v['values'].iteritems():
                req.append(pack('>Q', id))
                if v['type'] == const.SPH_ATTR_FLOAT:
                    req.append(pack('>f', value))
                elif v['type'] == const.SPH_ATTR_BIGINT:
                    req.append(pack('>q', value))
                else:
                    req.append(pack('>l', value))

        return b''.join(req)

    @clone_method
    def query(self, query='', index='*', comment=''):
        req = [
            self.__query_base(),
            self.__query_sort(),
            self.__query_query(query),
            self.__query_weights(),
            self.__query_index(index),
            self.__query_id_range(),
            self.__query_filters(),
            self.__query_other(),
            self.__query_anchor(),
            format_req(self._indexweights),  # per-index weights
            format_req(self._maxquerytime),  # max query time
            format_req(self._fieldweights),  # per-field weights
            format_req(comment),
            self.__query_overrides(),
            format_req(self._select),  # select-list
        ]

        self._reqs.append(b''.join(req))

    def status(self):
        # connect, send query, get response
        sock = self._connect()

        self._send(
            sock,
            pack('>2HLL', const.SEARCHD_COMMAND_STATUS,
                 const.VER_COMMAND_STATUS, 4, 1)
        )
        response = SResponse(
            self._get_response(sock, const.VER_COMMAND_STATUS))

        res = {}

        response.read(8)  # TODO: get this magic number
        response_len = response.len
        while response_len > response.tell():
            k = response.read_str()
            v = response.read_str()
            res[k] = v

        return res

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
        exclude = bool(kwargs.pop('exclude', False))
        for key, value in kwargs.items():
            self._filters.append({
                'attr': key,
                'values': value,
                'type': const.SPH_FILTER_VALUES,
                'exclude': bool(exclude),
            })

    @clone_method
    def extra(self, select=None):
        if select:
            self._select = select


if __name__ == '__main__':
    cl = SphinxClient()
    rez = cl.set_limits(1).filter(salary_from=[123]).query()._populate()
    print(rez)
    if len(rez) > 0 and 'total_found' in rez[0]:
        print(rez[0]['total_found'])
    # from pprint import pprint
    # pprint(cl.status())
    print('OK')
