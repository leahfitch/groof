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


import pytc
from abstract import (
    AbortTxn, ITransaction, IFileStorage, IDuplicateKeyStorage, IReadOnlyCursor
)



class TokyoCabinetStorage(IFileStorage, IDuplicateKeyStorage):
    
    
    def __init__(self):
        self._db = pytc.BDB()
        self._txn = TokyoCabinetTransaction(self._db)
        
        
    def __setitem__(self, k, v):
        self._db[k] = v
        
        
    def __getitem__(self, k):
        try:
            return self._db[k]
        except KeyError:
            raise KeyError, k
            
            
    def __delitem__(self, k):
        try:
            del self._db[k]
        except KeyError:
            raise KeyError, k
            
            
    def __contains__(self, k):
        return k in self._db
            
            
    def setdup(self, k, v):
        self._db.putdup(k,v)
        
        
    def getdup(self, k):
        try:
            return self._db.getlist(k)
        except KeyError:
            raise KeyError, k
            
            
    def deldup(self, k):
        try:
            self._db.outlist(k)
        except KeyError:
            raise KeyError, k
            
            
    def __len__(self):
        return len(self._db)
            
            
    def cursor(self):
        return TokyoCabinetCursor(self._db)
        
        
    def txn(self):
        return self._txn
        
        
    def in_txn(self):
        return self._txn.active
        
        
    def open(self, path, mode):
        if mode == 'r':
            flags = pytc.BDBOREADER
        elif mode == 'rw':
            flags = pytc.BDBOREADER | pytc.BDBOWRITER | pytc.BDBOCREAT
        else:
            raise ValueError, "Expected mode to be 'r' or 'rw'"
            
        self._db.open(path, flags)
        
        
    def close(self):
        self._db.close()



class TokyoCabinetTransaction(ITransaction):
    
    
    def __init__(self, _db):
        self._db = _db
        self.active = False
        
        
    def __enter__(self):
        self._db.tranbegin()
        self.active = True
        
        
    def __exit__(self, exc_type, exc_val, tb):
        try:
            if exc_type is None:
                self._db.trancommit()
                self._db.sync()
            else:
                self._db.tranabort()
                if exc_type == AbortTxn:
                    return True
        finally:
            self.active = False



class TokyoCabinetCursor(IReadOnlyCursor):
    
    def __init__(self, _db):
        self._c = _db.curnew()
        
        
    def first(self):
        try:
            self._c.first()
        except KeyError:
            raise RuntimeError, "Can't set a cursor since there are no records"
        
        
    def last(self):
        try:
            self._c.last()
        except KeyError:
            raise RuntimeError, "Can't set a cursor since there are no records"
        
        
    def moveto(self, k):
        try:
            self._c.jump(k)
        except KeyError:
            raise RuntimeError, "Can't set a cursor since there are no records"
    
    
    def next(self):
        self._c.next()
        
        
    def previous(self):
        self._c.prev()
        
        
    def current(self):
        return self._c.rec()
        