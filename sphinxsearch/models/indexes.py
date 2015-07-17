# -*- coding: utf-8 -*-
from __future__ import absolute_import

import re
from abc import ABCMeta
from six import with_metaclass
from collections import OrderedDict

from .attrs import AbstractAttr


INDEX_NAME_RE = re.compile(ur'^[\w]+$')


class Options(object):
    """
    _meta options for class
    """
    REQUIRED_OPTIONS = [
        'abstract',
        'source',  # database connection
        'sql_query',
        'name',  # index name
        'delta',  # if index is delta
    ]
    OPTIONS = [
        'enable_star',
        'path',
        'docinfo',
        'mlock',
        'morphology',
        'min_word_len',
        'charset_type',
        'charset_table',
        'min_infix_len',
        'query_info',
    ]
    # This options are ignored in conf output
    INTERNAL_OPTIONS = [
        'abstract',
        'source',
        'name',
        'delta',
        'sql_query',
    ]

    def __init__(self, cls, index_meta=None, **kwargs):
        super(Options, self).__init__()
        OPTIONS = self.REQUIRED_OPTIONS + self.OPTIONS
        for attr in OPTIONS:
            default = getattr(index_meta, attr, None)
            setattr(self, attr, default)

        for k, v in kwargs.items():
            setattr(self, k, v)

        self.abstract = self.abstract or False
        self.delta = self.delta or False

        if not self.abstract:
            # Checking index name
            if self.name is None:
                pattern = '{}_delta' if self.delta else '{}'
                self.name = pattern.format(cls.get_source_name())

            # Checking required fields
            for attr in self.REQUIRED_OPTIONS:
                if getattr(self, attr) is None:
                    raise Exception(
                        'Could not create class "{}": "{}" is required'.format(
                            cls.__name__, attr))

        # TODO: logger
        if index_meta:
            for attr in index_meta.__dict__.keys():
                if attr not in OPTIONS and not attr.startswith('__'):
                    print(
                        'Warning! "{}" Meta option in "{}" class'
                        ' is not supported.'.format(attr, cls.__name__))

    def get_option(self, name):
        option = getattr(self, name)
        if option is None:
            return ''
        elif option is bool:
            return int(option)
        else:
            return option

    def get_option_dicts(self):
        result = OrderedDict()

        for name in self.REQUIRED_OPTIONS + self.OPTIONS:
            if name in self.INTERNAL_OPTIONS:
                continue

            option = self.get_option(name)
            if option:
                result[name] = option

        return result


class IndexMeta(ABCMeta):
    def __new__(cls, cls_name, cls_parents, cls_dict):
        src_cls = ABCMeta.__new__(cls, cls_name, cls_parents, cls_dict)

        if cls_name.split('.')[-1] == 'NewBase':
            return src_cls

        try:
            abstract = getattr(cls_dict['Meta'], 'abstract', None)
        except KeyError:
            abstract = False

        src_cls._meta = Options(
            cls=src_cls,
            index_meta=cls_dict.get('Meta', getattr(src_cls, 'Meta')),
            abstract=abstract)

        cls_attr_names = [nm for nm in dir(src_cls) if not nm.startswith('__')]

        cls_dict = OrderedDict([(name, getattr(src_cls, name)) for name in
                                cls_attr_names])

        if not src_cls._meta.abstract:
            cls.validate(src_cls)

            source_attrs_dict = OrderedDict()

            for name, attr in cls_dict.items():
                if isinstance(attr, AbstractAttr):
                    source_attrs_dict[name] = attr

            src_cls.__attrs__ = source_attrs_dict

        return src_cls

    @staticmethod
    def validate(src_cls):
        if src_cls._meta.name:
            assert re.match(INDEX_NAME_RE, src_cls._meta.name)


class Index(with_metaclass(IndexMeta, object)):
    class Meta:
        abstract = True
        delta = False

    @classmethod
    def get_option_dicts(cls, engine):
        if cls._meta.abstract:
            raise NotImplementedError('Cannot get conf for abstract index')

        source_type = cls._meta.source.source_type

        source_options = cls._meta.source.get_source_options()

        source_options.update(OrderedDict({
            'sql_query': cls._meta.get_option('sql_query')
        }))
        index_options = cls._meta.get_option_dicts()

        for name, attr in cls.__attrs__.items():
            key, value = attr.get_option(name, source_type)
            source_options[key] = value

        return cls._meta.source.get_option_dicts(cls, source_options, index_options)

    @classmethod
    def get_source_name(cls):
        name = cls.__name__
        module_name = cls.__module__.split('.')[-1]
        return ('%s_%s' % (module_name, name)).lower()

    @classmethod
    def get_index_names(cls):
        names = (cls._meta.name,)
        if cls._meta.delta:
            delta_index_name = '{0}_delta : {0}'.format(cls._meta._name)
            names = names + (delta_index_name,)
        return names

    @classmethod
    def get_index_name(cls):
        return cls._meta.name
