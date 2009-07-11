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

from datetime import date
import json
from uuid import uuid4 as uuid
import threading


class TrackedAttrsMixin(object):
    """Factored out methods for managing attributes fo vertices and arcs"""
    
    def __getitem__(self, k):
        return self.attrs[k]
    
    
    def __setitem__(self, k, v):
        self.db.assert_serializable(v)
        
        if k in self.attrs:
            if v == self.attrs[k]:
                return
        
        self.attrs[k] = v
        self.db.im_dirty(self)
        
        
    def __delitem__(self, k):
        if k in self.attrs:
            del self.attrs[k]
            self.db.im_dirty(self)



class Arc(TrackedAttrsMixin):
    """A tuple of (vp, e, vs)
    
    All arcs have a label. In addition, arcs can have arbitrary attributes.
    """
    
    
    def __init__(self, db, p, e, s, attrs={}):
        self.db = db
        self.p = p
        self.e = e
        if isinstance(s, Vertex):
            self._successor = s
            self.s = s.id
        else:
            self.s = s
            self._successor = None
        self.attrs = attrs
        
        
    def predecessor(self):
        return self.p
    
    
    def edge(self):
        return self.e
        
        
    def successor(self):
        if self._successor == None:
            self._successor = self.db[self.s]
        return self._successor
        
        
    def __str__(self):
        return "Arc(%s %s %s)" % (self.p.id, self.e, self.s)



class Vertex(TrackedAttrsMixin):
    
    def __init__(self, db, id, attrs={}, arcs=[]):
        self.db = db
        self.id = id
        self.attrs = attrs
        self.arcs = arcs
    
    
    def add_arc(self, e, s, attrs={}):
        for a in self.arcs:
            if a.e == e and a.s == s:
                return a
        a = Arc(self.db, self, e, s, attrs)
        self.arcs.append(a)
        self.db.im_dirty(self)
    
    
    def get_arcs(self, e=None):
        if e is None:
            return self.arcs
        return [a for a in self.arcs if a.e == e]
    
    
    def remove_arc(self, a):
        try:
            self.arcs.remove(a)
            self.db.im_dirty(self)
        except IndexError:
            pass
        
        
    def __str__(self):
        return "Vertex(%s)" % self.id



class Transaction(object):
    
    def __init__(self, db):
        self.db = db
        self.active = False
        
        
    def __enter__(self):
        self.active = True
        
        
    def __exit__(self, exc_type, exc_val, tb):
        try:
            if exc_type is not None:
                self.db.dirty = []
            else:
                self.db.flush()
        finally:
            self.active = False



class Graph(object):
    
    
    def __init__(self, storage):
        self.storage = storage
        self.local = threading.local()
        self.local.dirty = []
        self.local.removed = []
        
        
    def create(self, id=None):
        self.assert_in_txn()
        if id is None:
            id = self.create_id()
        if id in self.storage:
            raise ValueError, "A vertex with id %s already exists" % id
        v = Vertex(self, id, {}, [])
        self.im_dirty(v)
        return v
    
    
    def __getitem__(self, id):
        data = self.storage[id]
        attrs, arcs = json.loads(data)
        v = Vertex(self, id, attrs)
        v.arcs = [Arc(self, v, e, s, attrs) for e,s,attrs in arcs]
        return v
    
    
    def __delitem__(self, id):
        self.assert_in_txn()
        if id not in self.local.removed and id in self.storage:
            self.local.removed.append(id)
    
    
    def __len__(self):
        return len(self.storage)
    
    
    def txn(self):
        if not hasattr(self.local, 'txn'):
            self.local.txn = Transaction(self)
        return self.local.txn
    
    
    def flush(self):
        if len(self.local.dirty) > 0 or len(self.local.removed) > 0:
            with self.storage.txn():
                for id in self.local.removed:
                    del self.storage[id]
                for v in self.local.dirty:
                    self.storage[v.id] = json.dumps((v.attrs, [(a.e,a.s,a.attrs) for a in v.arcs]))
        
        
    def create_id(self):
        return uuid().hex
        
        
    def im_dirty(self, item):
        self.assert_in_txn()
        if isinstance(item, Arc):
            item = item.p
        if item not in self.local.dirty:
            self.local.dirty.append(item)
        
        
    def assert_serializable(self, v):
        if not isinstance(v, (int, float, str, unicode, list, tuple, dict)):
            raise ValueError, "%s is not serializable" % v
        
        
    def assert_in_txn(self):
        assert self.txn().active, "Operation not permitted outside a transaction"