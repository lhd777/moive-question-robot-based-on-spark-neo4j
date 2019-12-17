"""Manages caches of calculated partitions."""

from __future__ import (division, absolute_import, print_function,
                        unicode_literals)

import logging
import pickle
import time
import zlib

log = logging.getLogger(__name__)


class CacheManager(object):
    def __init__(self, max_mem=1.0, serializer=None, deserializer=None,
                 checksum=None):
        self.max_mem = max_mem
        self.serializer = serializer if serializer else pickle.dumps
        self.deserializer = deserializer if deserializer else pickle.loads
        self.checksum = checksum if checksum else zlib.crc32

        self.cache_obj = {}
        self.cache_cnt = 0

    def incr_cache_cnt(self):
        self.cache_cnt += 1
        return self.cache_cnt

    def add(self, ident, obj):
        self.cache_obj[ident] = {
            'id': self.incr_cache_cnt(),
            'mem_obj': obj,
            'disk_location': None,
            'checksum': None,
        }

    def get(self, ident):
        if ident not in self.cache_obj:
            return None
        else:
            return self.cache_obj[ident]['mem_obj']

    def has(self, ident):
        return (
            ident in self.cache_obj and (
                self.cache_obj[ident]['mem_obj'] is not None or
                self.cache_obj[ident]['disk_location'] is not None
            )
        )

    def stored_idents(self):
        return [k
                for k, v in self.cache_obj.items()
                if (v['mem_obj'] is not None or
                    v['disk_location'] is not None)]

    def clone_contains(self, filter_id):
        cm = CacheManager(self.max_mem,
                          self.serializer, self.deserializer,
                          self.checksum)
        cm.cache_obj = {i: c
                        for i, c in self.cache_obj.items()
                        if filter_id(i)}
        return cm