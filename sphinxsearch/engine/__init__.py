# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

from collections import OrderedDict

from .server import SearchServer
from .indexer import Indexer
from .commands import CommandBuilder
from ..utils.const import CONFIG_INDENT

__all__ = ['Engine', 'SearchServer', 'Indexer']


class Engine(object):
    def __init__(self):
        self._api = None
        self._server = None
        self._indexer = None
        self.conf_file = None
        self.commands = CommandBuilder()
        self.indexes = dict()

    @property
    def api(self):
        return self._api

    @api.setter
    def api(self, api):
        self._api = api
        if self._server is not None:
            self._server.set_api(api)

    @property
    def server(self):
        return self._server

    @server.setter
    def server(self, server):
        self._server = server
        if self._api is not None:
            self._server.set_api(self.api)

    @property
    def indexer(self):
        return self._indexer

    @indexer.setter
    def indexer(self, indexer):
        self._indexer = indexer

    def add_index(self, index):
        self.indexes[index.get_index_name()] = index

    def extend_indexes(self, indexes):
        self.indexes.update({index.get_index_name(): index
                             for index in indexes})

    def set_conf(self, conf_file):
        self.conf_file = conf_file
        self.commands.set_conf(conf_file)

    def get_conf(self):
        return self.conf_file

    def create_config(self):
        blocks_dict = OrderedDict(
            self.server.get_options().items() +
            self.indexer.get_options().items() +
            self.get_models_dict().items()
        )

        str_list = []

        for block_name, block_attrs in blocks_dict.items():
            block_body_list = ['%s = %s' % (ak, av) for ak, av in
                               block_attrs.items()]
            block_body = ('\n%s' % CONFIG_INDENT).join([''] + block_body_list)
            block = """%s\n{%s\n}\n""" % (block_name, block_body)
            str_list.append(block)

        return '\n'.join(str_list)

    def save(self):
        if self.conf_file is None:
            raise RuntimeError('Engine must provide conf_file')

        config_str = self.create_config()
        with open(self.conf_file, 'w') as f:
            f.write(config_str)

    def write(self, config_path):
        with open(config_path, 'w') as f:
            s = self.get_conf().encode('utf-8')
            f.write(s)

    def get_models_dict(self):
        indexes_blocks = {}

        for index in self.indexes.values():
            if index._meta.abstract:
                continue
            index_option_dicts = index.get_option_dicts(self)
            indexes_blocks.update(index_option_dicts)

        return indexes_blocks

    def replace(self, **kwargs):
        new_inst = self.__class__(
            api=kwargs.get('api', self.api),
            server=kwargs.get('server', self.server),
            indexer=kwargs.get('indexer', self.indexer))
        new_inst.extend_indexes(kwargs.get('indexes', self.indexes))
        return new_inst

    def get_session(self, **kwargs):
        return self.server.get_session(**kwargs)
