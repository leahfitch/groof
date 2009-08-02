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
from tokyocabinet import btree
from abstract import (
    IFileStorage, IPrefixMatchingStorage, IDuplicateKeyStorage, IIterableStorage,
    ITransactionalStorage, TransactionalStorageGroup
)


class TokyoCabinetStorage(IFileStorage, IPrefixMatchingStorage, ITransactionalStorage):
    
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
        self._db.sync()
        
        
    def copy(self, path):
        self._db.copy(path)
        
        
    def match_prefix(self, prefix, limit=-1):
        return self._db.fwmkeys(prefix, limit)
        
        
    def start_txn(self):
        self._db.tranbegin()
        
        
    def abort_txn(self):
        self._db.tranabort()
        
        
    def commit_txn(self):
        self._db.trancommit()
        
        
        
class BTreeStorage(TokyoCabinetStorage, IDuplicateKeyStorage, IIterableStorage):
    
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
            
            
    def __iter__(self):
        if len(self._db) == 0:
            return
            
        c = self._db.cursor()
        
        c.first()
        k = c.key()
        
        try:
            while k:
                yield k
                c.next()
                k = c.key()
        except KeyError:
            pass
            
            
    def iter_records(self):
        if len(self._db) == 0:
            return
            
        c = self._db.cursor()
        
        c.first()
        r = c.rec()
        try:
            while 1:
                yield r
                c.next()
                r = c.rec()
        except KeyError:
            pass
        
        
        
class TokyoCabinetStorageGroup(TransactionalStorageGroup):
    
    def __init__(self, basedir):
        if not os.path.exists(basedir):
            os.makedirs(basedir)
        
        for n in self.storage_attrs:
            i = BTreeStorage()
            i.open(os.path.join(basedir, n), 'rw')
            setattr(self, n, i)
        
        self.index_dir = os.path.join(basedir, 'indices')
        
        if not os.path.exists(self.index_dir):
            os.makedirs(self.index_dir)
            
        self.indices = {}
        
        
        
    def get_index(self, name):
        if name not in self.indices:
            self.indices[name] = BTreeStorage()
            self.indices[name].open(os.path.join(self.index_dir, name),'rw')
        return self.indices[name]
        