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


OUTGOING = 0
INCOMING = 1


class Arc(object):
    """A tuple of (vp, e, vs)
    
    All arcs have a label and direction. In addition, arcs can have arbitrary attributes.
    """
    
    
    dirlabels = {
        OUTGOING: '->',
        INCOMING: '<-'
    }
    
    
    def __init__(self, db, start, label, end, direction=OUTGOING):
        self.db = db
        self.start = start
        self.label = label
        if isinstance(end, Vertex):
            self._end = end
            self.end_id = end.id
        else:
            self.end_id = end
            self._end = None
        self.direction = direction
        
        
    def get_end(self):
        if self._end == None:
            self._end = self.db[self.end_id]
        return self._end
    end = property(get_end)
        
        
    def inverse(self):
        return Arc(self.db, self.end, self.label, self.start, self.direction ^ 1)
        
        
    def __eq__(self, other):
        return hash(self) == hash(other)
        
        
    def __hash__(self):
        return hash(str(self))
        
        
    def __str__(self):
        return "Arc(%s, %s, %s, %s,)" % (self.start.id, self.label, self.end_id, self.dirlabels[self.direction])
        
        
    def __repr__(self):
        return str(self)



class Vertex(object):
    
    def __init__(self, db, id_, attrs=None, arcs=None):
        self.db = db
        self.id = id_
        self.attrs = attrs if attrs is not None else {}
        self.arcs = arcs if arcs is not None else []
        
        
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
        
        
    def __contains__(self, k):
        return k in self.attrs
        
        
    def items(self):
        return self.attrs.items()
        
        
    def add_arc(self, label, end, direction=OUTGOING):
        a = Arc(self.db, self, label, end, direction)
        if a not in self.arcs:
            self.arcs.append(a)
            self.db.im_dirty(self)
            end.add_inverse_arc(a)
        
        
    def add_inverse_arc(self, a):
        a = a.inverse()
        if a not in self.arcs:
            self.arcs.append(a)
            self.db.im_dirty(self)
    
    
    def remove_arc(self, a):
        try:
            self.arcs.remove(a)
            self.db.im_dirty(self)
        except IndexError:
            pass
        else:
            a.end.remove_inverse_arc(a)
        
        
    def remove_inverse_arc(self, a):
        a = a.inverse()
        try:
            self.arcs.remove(a)
            self.db.im_dirty(self)
        except IndexError:
            pass
        
        
    def get_arcs(self, label=None, direction=None):
        if direction is None:
            return [a for a in self.arcs if label is None or a.label == label]
        else:
            return [a for a in self.arcs if label is None or a.label == label and a.direction == direction]
        
        
    def delete(self):
        del self.db[self]
        
        
    def __eq__(self, other):
        return hash(self) == hash(other)
        
        
    def __hash__(self):
        return hash(str(self))
        
        
    def __str__(self):
        return "Vertex(%s)" % self.id
        
        
    def __repr__(self):
        return str(self)



class Transaction(object):
    
    def __init__(self, db):
        self.db = db
        self.active = False
        
        
    def __enter__(self):
        self.active = True
        
        
    def __exit__(self, exc_type, exc_val, tb):
        try:
            if exc_type is not None:
                self.db.local.dirty = []
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
        
        
    def create(self, id=None, attrs=None):
        self.assert_in_txn()
        if id is None:
            id = self.create_id()
        if id in self.storage:
            raise ValueError, "A vertex with id %s already exists" % id
        v = Vertex(self, id, attrs)
        self.im_dirty(v)
        return v
    
    
    def __getitem__(self, id):
        data = self.storage[id]
        attrs, arcs = json.loads(data)
        v = Vertex(self, id, attrs)
        v.arcs = [Arc(self, v, l, e, d) for l,e,d in arcs]
        return v
    
    
    def __delitem__(self, v):
        self.assert_in_txn()
        if v not in self.local.removed and v.id in self.storage:
            self.local.removed.append(v)
    
    
    def __len__(self):
        return len(self.storage)
    
    
    def txn(self):
        if not hasattr(self.local, 'txn'):
            self.local.txn = Transaction(self)
        return self.local.txn
    
    
    def flush(self):
        if len(self.local.dirty) > 0 or len(self.local.removed) > 0:
            with self.storage.txn():
                for v in self.local.removed:
                    for a in v.get_arcs(None, OUTGOING):
                        a.end.remove_inverse_arc(a)
                    del self.storage[v.id]
                for v in self.local.dirty:
                    self.storage[v.id] = json.dumps((v.attrs, [(a.label,a.end_id,a.direction) for a in v.arcs]))
        
        
    def create_id(self):
        return uuid().hex
        
        
    def im_dirty(self, item):
        self.assert_in_txn()
        if isinstance(item, Arc):
            vertices = [item.start, item.end]
        else:
            vertices = [item]
            
        for v in vertices:
            if v not in self.local.dirty:
                self.local.dirty.append(v)
        
        
    def assert_serializable(self, v):
        if not isinstance(v, (int, float, str, unicode, list, tuple, dict)):
            raise ValueError, "%s is not serializable" % v
        
        
    def assert_in_txn(self):
        assert self.txn().active, "Operation not permitted outside a transaction"