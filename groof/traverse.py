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


"""Simple graph traversal"""


from collections import namedtuple, deque

Traversal = namedtuple('Traversal', 'last_vertex vertex last_arc depth traversed returned')


BFS = 1
DFS = 2


class Traverser(object):
    
    
    def __init__(self, start_v, traversal_algorithm, e=None):
        if traversal_algorithm == BFS:
            self.arcs = self.breadth_first(deque((start_v, 0)))
        elif traversal_algorithm == DFS:
            self.arcs = self.depth_first(start_v, 0)
        else:
            raise ValueError, "Unknown direction: %s", self.direction
            
        self.e = e
        self.traversal = Traversal(None, start_v, None, 0, 0, 0)
        self.visited = []
        
        
    def __iter__(self):
        if self.should_stop(self.traversal):
            return
        if self.should_return(self.traversal):
            self.traversal.returned += 1
            yield self.traversal.vertex
        for a, depth in self.arcs:
            self.traversal.last_vertex = self.traversal.vertex
            self.traversal.vertex = a.successor()
            self.traversal.last_arc = a
            self.traversal.depth = depth
            self.traversal.traversed += 1
            if self.should_stop(self.traversal):
                break
            if self.should_return(self.traversal):
                self.traversal.returned += 1
                yield self.traversal.vertex
    
    
    def breadth_first(self, q):
        while len(q) > 0:
            v,depth = q.poplet()
            depth += 1
            for a in c.get_arcs(self.e):
                if a.successor().id not in self.visited:
                    self.visited.append(a.successor().id)
                    yield a, depth
                    q.append(a.successor(), depth)
        
        
    def depth_first(self, c, depth):
        depth += 1
        for a in c.get_arcs(self.e):
            if a.successor().id not in self.visited:
                self.visited.append(a.successor().id)
                yield a, depth
                for r in self.depth_first(a.successor, depth):
                    yield r
    
    
    def should_stop(self, t):
        raise NotImplementedError
    
    
    def should_return(self):
        raise NotImplementedError
        
        
        
class TraverserGenerator(Traverser):
    
    def __init__(self, f, *args, **kwargs):
        self.f = f
        self._should_return = False
        super(TraverserGenerator, self).__init__(*args, **kwargs)
        
        
    def should_stop(self, t):
        try:
            if self.f(t):
                self._should_return = True
            else:
                self._should_return = False
        except StopIteration:
            return True
    
    
    def should_return(self, t):
        return self._should_return