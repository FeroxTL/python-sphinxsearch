# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

import os

from six import string_types
from os.path import join

from ..query.filters import Range
from ..utils.cmdtools import (
    cmd_flag, cmd_decorator, check_options, cmd_named_kwarg, cmd_named_arg,
    requires_kwarg
)


class Executable(unicode):
    """
    Makes string executable with os execution
    """
    def call(self):
        return os.system(self)


def executable(f):
    def wrapper(*args, **kwds):
        return Executable(f(*args, **kwds))
    return wrapper


def index_to_str(*index):
    if len(index) == 1:
        index = index[0]
        if isinstance(index, string_types):
            return unicode(index)
        else:
            return ' '.join(index.get_index_names())
    elif len(index) == 2:
        return ' '.join(index)
    else:
        raise ValueError(
            'index must be two strings or have get_index_names method ')


def cmd_loglevel_option(func):
    def wrapper(*args, **kwargs):
        level = int(kwargs.pop('logdebug', 0) or 0)

        if not level:
            logdebug_str = ''
        elif 1 <= level <= 3:
            level_str = (level - 1) * 'v'
            logdebug_str = '--logdebug%s' % level_str
        else:
            raise TypeError('invalid logdebug level %s' % level)

        cmd_splitted = list(func(*args, **kwargs))
        if logdebug_str:
            cmd_splitted.append(logdebug_str)
        return cmd_splitted
    return wrapper


def cmd_dst_range_popper(func):
    def wrapper(*args, **kwargs):
        if kwargs:
            dst_attr, dst_range = kwargs.popitem()
            if isinstance(dst_range, Range):
                start = int(dst_range.start)
                end = int(dst_range.end)
            else:
                start = int(dst_range)
                end = int(dst_range)
            dst_range_str = '%s %s %s' % (dst_attr, start, end)
        else:
            dst_range_str = ''

        cmd_splitted = func(*args, **kwargs)
        if dst_range_str:
            cmd_splitted.append('--merge-dst-range')
            cmd_splitted.append(dst_range_str)
        return cmd_splitted
    return wrapper


def indexer_cmd_wrapper(func):
    func = cmd_flag('dump', '--dump-rows', default=False)(func)
    func = cmd_flag('sql', '--print-queries', default=False)(func)
    func = cmd_flag('debug', '--verbose', default=False)(func)
    func = cmd_flag('noprogress', '--noprogress', default=False)(func)
    func = cmd_flag('quiet', '--quiet', default=False)(func)
    func = cmd_decorator(func)
    return func


def server_cmd_wrapper(func):
    func = cmd_decorator(func)
    return func


def indextool_cmd_wrapper(func):
    func = cmd_decorator(func)
    func = cmd_flag('optimize_rt_klists', '--optimize-rt-klists', default=False)(func)
    func = cmd_flag('strip_path', '--strip-path', default=False)(func)
    func = cmd_flag('strip_path', '--strip-path', default=False)(func)
    func = cmd_flag('checkconfig', '--checkconfig', default=False)(func)
    func = cmd_flag('quiet', '--quiet', default=False)(func)
    return func


class CommandBuilder(object):
    """
    >>> commands = CommandBuilder()
    """
    def __init__(self, prefix=''):
        self.prefix = prefix

    @property
    def prefix(self):
        return self._prefix

    @prefix.setter
    def prefix(self, prefix):
        """
        >>> commands.prefix = u'/usr/local/'
        """
        self._prefix = prefix
        self.search_cmd = join(prefix, 'search')
        self.searchd_cmd = join(prefix, 'searchd')
        self.indexer_cmd = join(prefix, 'indexer')
        self.spelldump_cmd = join(prefix, 'spelldump')
        self.indextool_cmd = join(prefix, 'indextool')
        self.wordbreaker_cmd = join(prefix, 'wordbreaker')

    @property
    @executable
    def search(self):
        return '%s --config %s' % (self.search_cmd, self.config_path)

    @property
    @executable
    def searchd(self):
        return '%s --config %s' % (self.searchd_cmd, self.config_path)

    @property
    @executable
    def indexer(self):
        return '%s --config %s' % (self.indexer_cmd, self.config_path)

    @property
    @executable
    def spelldump(self):
        return '%s --config %s' % (self.spelldump_cmd, self.config_path)

    @property
    @executable
    def indextool(self):
        return '%s --config %s' % (self.indextool_cmd, self.config_path)

    @property
    @executable
    def wordbreaker(self):
        return '%s --config %s' % (self.wordbreaker_cmd, self.config_path)

    def set_conf(self, config_path):
        self.config_path = config_path

    def get_conf(self):
        return self.config_path

    @executable
    @indexer_cmd_wrapper
    @cmd_flag('rotate', '--rotate', default=True)
    @cmd_flag('sighup_each', '--sighup-each', default=False)
    @cmd_flag('nohup', '--nohup', default=False)
    def reindex(self, *indexes, **kwargs):
        """
        >>> commands.reindex(Index, rotate=False)
        >>> commands.reindex('main', 'delta', deleted=0)
        >>> commands.reindex(all=True, sighup_each=True, dump='/tmp/dump.sql')
        """
        cmd_splitted = [self.indexer]

        all = kwargs.pop('all', False)

        check_options(kwargs)

        if all:
            cmd_splitted.append('--all')
        else:
            cmd_splitted.extend(map(index_to_str, indexes))

        return cmd_splitted

    @executable
    @indexer_cmd_wrapper
    @cmd_flag('rotate', '--rotate', default=True)
    @cmd_flag('keep_attrs', '--keep-default=attrs', default=False)
    @cmd_flag('killlists', '--merge-default=killlists', default=False)
    @cmd_flag('nohup', '--nohup', default=False)
    @cmd_dst_range_popper
    def merge(self, *index):
        """
        >>> commands.merge(Index, rotate=False)
        >>> commands.merge('main', 'delta', deleted=0)
        >>> commands.merge(Main, Delta, deleted=Range(23, 556), rotate=True)
        """
        cmd_splitted = [self.indexer, '--merge']

        index_str = index_to_str(*index)
        cmd_splitted.append(index_str)

        return cmd_splitted

    @executable
    @indexer_cmd_wrapper
    @cmd_flag('freqs', '--buildfreqs', default=False)
    @requires_kwarg('outputfile')
    @requires_kwarg('limit')
    def buildstops(self, *indexes, **kwargs):
        """
        >>> commands.buildstops(Index, 'main', limit=500, freqs=True, outputfile='/tmp/stops.txt')
        >>> commands.buildstops('main', 'delta', limit=5000, sql=True)
        >>> commands.buildstops(Main, Delta, limit=9000, outputfile='/tmp/stops.txt')
        """
        outputfile = kwargs.pop('outputfile')
        limit = kwargs.pop('limit')

        check_options(kwargs)

        cmd_splitted = [self.indexer]
        cmd_splitted.extend(map(index_to_str, indexes))
        cmd_splitted.append('--buildstops')

        cmd_splitted.append(unicode(outputfile))
        cmd_splitted.append(unicode(int(limit)))
        return cmd_splitted

    @executable
    @server_cmd_wrapper
    @cmd_named_kwarg('pidfile', '--pidfile')
    def status(self):
        return [self.searchd, '--status']

    @executable
    @server_cmd_wrapper
    @cmd_named_kwarg('pidfile', '--pidfile')
    @cmd_flag('block', '--stopwait', default=False)
    def stop(self):
        return [self.searchd, '--stop']

    @executable
    @server_cmd_wrapper
    @cmd_named_kwarg('index', '--index', apply=index_to_str)
    @cmd_named_kwarg('pidfile', '--pidfile')
    @cmd_named_kwarg('listen', '--listen', conflicts=('host', 'port'))
    @cmd_named_kwarg('port', '--port', conflicts=('listen',))
    @cmd_named_kwarg('host', '--host', conflicts=('listen',))
    # debug options
    @cmd_flag('iostats', '--iostats', default=False)
    @cmd_flag('cpustats', '--cpustats', default=False)
    @cmd_flag('console', '--console', default=False)
    @cmd_flag('safetrace', '--safetrace', default=False)
    @cmd_flag('replay_flags', '--replay-default=flags', default=False)
    # windows options
    @cmd_flag('install', '--install', default=False, conflicts=['nodetach'])
    @cmd_flag('delete', '--delete', default=False, conflicts=['nodetach'])
    @cmd_flag('servicename', '--servicename', default=False, conflicts=['nodetach'])
    @cmd_flag('ntservice', '--ntservice', default=False, conflicts=['nodetach'])
    #linux only option
    @cmd_flag('nodetach', '--nodetach', default=False, conflicts=['install',
                                                                  'delete',
                                                                  'servicename',
                                                                  'ntservice'])
    @cmd_loglevel_option
    def start(self, **kwargs):
        return [self.searchd, '--start']

    @executable
    @server_cmd_wrapper
    def restart(self, *args, **kwargs):
        pidfile = kwargs.pop('pidfile', None)
        new_pidfile = kwargs.pop('new_pidfile', pidfile)

        stop_kwargs = dict(block=True)
        start_kwargs = kwargs

        if pidfile:
            stop_kwargs['pidfile'] = pidfile
        if new_pidfile:
            start_kwargs['pidfile'] = new_pidfile

        stop_cmd = self.stop(**stop_kwargs)
        start_cmd = self.start(*args, **start_kwargs)
        return [stop_cmd, ';', start_cmd]

    @indextool_cmd_wrapper
    @cmd_named_arg('file')
    def dumpconfig(self):
        return [self.indextool, '--dumpconfig']

    @indextool_cmd_wrapper
    @cmd_named_arg('file')
    def dumpheader(self):
        return [self.indextool, '--dumpheader']

    @indextool_cmd_wrapper
    @cmd_named_arg('index', apply=index_to_str)
    def build_infixes(self):
        return [self.indextool, '--build-infixes']

    @indextool_cmd_wrapper
    @cmd_named_arg('index', apply=index_to_str)
    @cmd_flag('rotate', '--rotate', default=True)
    def check(self):
        return [self.indextool, '--check']

    @indextool_cmd_wrapper
    @cmd_named_arg('index', apply=index_to_str)
    def dumpdict(self):
        return [self.indextool, '--dumpdict']

    @indextool_cmd_wrapper
    @cmd_named_arg('index', apply=index_to_str)
    def dumpdocids(self):
        return [self.indextool, '--dumpdocids']

    @indextool_cmd_wrapper
    @cmd_named_arg('index', apply=index_to_str)
    @cmd_named_kwarg('wordid', '--wordid', conflicts=['keyword'])
    @cmd_named_arg('keyword', conflicts=['wordid'])
    def dumphitlist(self):
        return [self.indextool, '--dumphitlist']

    @indextool_cmd_wrapper
    @cmd_named_arg('index', apply=index_to_str)
    @cmd_named_arg('optfile')
    def fold(self):
        return [self.indextool, '--fold']

    @indextool_cmd_wrapper
    @cmd_named_arg('index', apply=index_to_str)
    def htmlstrip(self):
        return [self.indextool, '--htmlstrip']

    @indextool_cmd_wrapper
    @cmd_named_arg('index', apply=index_to_str)
    def morph(self):
        return [self.indextool, '--morph']
