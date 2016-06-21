'''
Created on Jun 20, 2016

@author: abhinav
'''
import collections

class LRUCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = collections.OrderedDict()

    def get(self, key, default=None):
        try:
            value = self.cache.pop(key)
            self.cache[key] = value
            return value
        except KeyError:
            return default

    def set(self, key, value):
        try:
            self.cache.pop(key)
        except KeyError:
            while(len(self.cache) >= self.capacity):
                self.cache.popitem(last=False)
        self.cache[key] = value