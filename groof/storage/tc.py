# Copyright (c) 2009 Elisha Cook <ecook@justastudio.com>
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

"""
File storage implementation backed by tokyo cabinet (requires pytc).
"""


import os, os.path
from tokyocabinet import btree, hash
from abstract import IFileStorage, IPrefixMatchingStorage, IDuplicateKeyStorage, IStorageGroup


class TokyoCabinetStorage(IFileStorage, IPrefixMatchingStorage):
    
    def __setitem__(self, k, v):
        self._db[k] = v
        
        
    def __getitem__(self, k):
        try:
            return self._db[k]
        except KeyError:
            raise KeyError, k
            
            
    def __delitem__(self, k):
        try:
            self._db.out(k)
        except KeyError:
            raise KeyError, k
            
            
    def __contains__(self, k):
        return k in self._db
            
            
    def __len__(self):
        return len(self._db)
            
            
    def clear(self):
        self._db.vanish()
        
        
    def close(self):
        self._db.close()
        
        
    def flush(self):
        self._db.flush()
        
        
    def copy(self, path):
        self._db.copy(path)
        
        
    def match_prefix(self, prefix, limit=-1):
        return self._db.fwmkeys(prefix, limit)
        
        
        
class BTreeStorage(TokyoCabinetStorage, IDuplicateKeyStorage):
    
    def __init__(self):
        self._db = btree.BTree()
        self._db.tune(0,0,0,0,0,btree.BDBTLARGE|btree.BDBTTCBS)
        
        
    def open(self, path, mode):
        if mode == 'r':
            flags = btree.BDBOREADER
        elif mode == 'rw':
            flags = btree.BDBOREADER | btree.BDBOWRITER | btree.BDBOCREAT
        else:
            raise ValueError, "Expected mode to be 'r' or 'rw'"
            
        self._db.open(path, flags)
        
        
    def setdup(self, k, v):
        self._db.putdup(k,v)
        
        
    def getdup(self, k):
        try:
            return self._db.getdup(k)
        except KeyError:
            raise KeyError, k
            
            
    def deldup(self, k):
        try:
            return self._db.outdup(k)
        except KeyError:
            raise KeyError, k
        
        
        
class HashTableStorage(TokyoCabinetStorage):
    
    def __init__(self):
        self._db = hash.Hash()
        self._db.tune(0,0,0,hash.HDBTLARGE|hash.HDBTTCBS)
        
        
    def open(self, path, mode):
        if mode == 'r':
            flags = hash.HDBOREADER
        elif mode == 'rw':
            flags = hash.HDBOREADER | hash.HDBOWRITER | hash.HDBOCREAT
        else:
            raise ValueError, "Expected mode to be 'r' or 'rw'"
            
        self._db.open(path, flags)
        
        
        
class TokyoCabinetStorageGroup(IStorageGroup):
    
    def __init__(self, basedir):
        if not os.path.exists(basedir):
            os.makedirs(basedir)
            
        storage_defs = [
            ('node',HashTableStorage),
            ('left',HashTableStorage),
            ('right',HashTableStorage)
        ]
        
        for n,T in storage_defs:
            i = T()
            i.open(os.path.join(basedir, n), 'rw')
            setattr(self, n, i)
        
        
        
    def get_index(self, name):
        if not os.path.exists(os.path.join(basedir, 'indices')):
            os.mkdir(os.path.join(basedir, 'indices'))
        index = BTreeStorage()
        index.open(os.path.join(basedir, 'indices', name),'rw')
        return index