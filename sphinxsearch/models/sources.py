# -*- coding: utf-8 -*-
from __future__ import absolute_import

from abc import ABCMeta

from six import with_metaclass

from .attrs import AbstractAttr
from .types import AbstractIndexType
from ..utils import is_abstract, set_abstract


class IndexBase(object):
    __abstract__ = True


class IndexMeta(ABCMeta):
    def __new__(cls, cls_name, cls_parents, cls_dict):
        src_cls = ABCMeta.__new__(cls, cls_name, cls_parents, cls_dict)

        if cls_name.split('.')[-1] == 'NewBase':
            return src_cls

        is_abc = cls_dict.pop('__abstract__', False)

        set_abstract(src_cls, is_abc)

        cls_attr_names = [nm for nm in dir(src_cls) if not nm.startswith('__')]

        cls_dict = dict([(name, getattr(src_cls, name)) for name in
                         cls_attr_names])

        if not is_abstract(src_cls):
            cls.validate(src_cls)
            source_attrs_dict = {}

            for name, attr in cls_dict.items():
                if isinstance(attr, AbstractAttr):
                    source_attrs_dict[name] = attr

            src_cls.__attrs__ = source_attrs_dict

        source_name = cls_dict.get('__sourcename__') or\
            cls.get_source_name(src_cls)
        src_cls.__sourcename__ = source_name

        return src_cls

    @classmethod
    def get_source_name(cls, index_cls):
        name = index_cls.__name__
        module_name = index_cls.__module__.split('.')[-1]
        return ('%s_%s' % (module_name, name)).lower()

    @staticmethod
    def validate(src_cls):
        assert hasattr(src_cls, '__source__'), src_cls
        assert isinstance(
            src_cls.__source__, AbstractIndexType), src_cls.__source__
        pass


class Index(with_metaclass(IndexMeta, IndexBase)):
    __delta__ = False
    __abstract__ = True

    @classmethod
    def get_option_dicts(cls, engine):
        if is_abstract(cls):
            raise NotImplementedError('Cannot get conf for abstract index')

        source_type = cls.__source__.source_type

        attr_conf_options = {}

        for name, attr in cls.__attrs__.items():
            key, value = attr.get_option(name, source_type)
            attr_conf_options[key] = value

        return cls.__source__.get_option_dicts(cls, attr_conf_options)

    @classmethod
    def get_index_names(cls):
        names = (cls.__sourcename__,)
        if cls.__delta__:
            delta_index_name = '{}_delta : {}'.format(cls.__sourcename__)
            names = names + (delta_index_name,)
        return names
