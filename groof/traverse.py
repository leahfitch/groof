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



"""Simple graph traversal

TODO: Update for current graph api
"""

from collections import deque


BFS = 1
DFS = 2



class Traversal(object):
    
    def __init__(self, last_node, node, last_edge, depth, traversed, returned):
        self.last_node = last_node
        self.node = node
        self.last_edge = last_edge
        self.depth = depth
        self.traversed = traversed
        self.returned = returned


class Traverser(object):
    
    
    def __init__(self, start_node, traversal_algorithm, rel=None):
        if traversal_algorithm == BFS:
            self.edges = self.breadth_first(deque([(start_node, 0)]))
        elif traversal_algorithm == DFS:
            self.edges = self.depth_first(start_node, 0)
        else:
            raise ValueError, "Unknown traversal algorithm: %s", self.traversal_algorithm
            
        self.rel = rel
        self.traversal = Traversal(None, start_node, None, 0, 0, 0)
        self.visited = []
        
        
    def __iter__(self):
        if self.should_stop(self.traversal):
            return
        if self.should_return(self.traversal):
            self.traversal.returned += 1
            yield self.traversal.node
        self.visited.append(self.traversal.node.id)
        for a, depth in self.edges:
            self.traversal.last_node = self.traversal.node
            self.traversal.node = a.right
            self.traversal.last_edge = a
            self.traversal.depth = depth
            self.traversal.traversed += 1
            if self.should_stop(self.traversal):
                break
            if self.should_return(self.traversal):
                self.traversal.returned += 1
                yield self.traversal.node
    
    
    def breadth_first(self, q):
        while len(q) > 0:
            node,depth = q.popleft()
            depth += 1
            for edge in node.edges(self.rel):
                if edge.right_id not in self.visited:
                    self.visited.append(edge.right_id)
                    yield edge, depth
                    q.append((edge.right, depth))
        
        
    def depth_first(self, node, depth):
        depth += 1
        for edge in node.edges(self.rel):
            if edge.right_id not in self.visited:
                self.visited.append(edge.right_id)
                yield edge, depth
                for e in self.depth_first(edge.right, depth):
                    yield e
    
    
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