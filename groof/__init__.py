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

"""A simple graph database

>>> FOUGHT = 1 # relationship types are 64bit ints
>>> NEXT = 2
>>> g = graph('/tmp/grooftest') # setup a graph environment
>>> with g: # all destructive operations must occur within graph context
...     n1 = g.create_node(name='The Green Knight') # create a node
...     n2 = g.create_node(name='The Black Knight') # ...
...     e = n1.edges.add(FOUGHT, n2, how='bravely') # create and edge from n1 to n2
>>> n1.id # nodes get sequential ids
1
>>> n2.id
2
>>> n1['name'] # access node attributes
'The Green Knight'
>>> n1.edges(FOUGHT)[0].right.id # get the id of the right side of the first FOUGHT edge leaving n1
2
>>> n1.edges(FOUGHT)[0].right.edges(FOUGHT, direction=INCOMING)[0].left['name']
'The Green Knight'
>>> with g: # create a bunch of nodes
...     start = g.create_node(name=0)
...     end = g.create_node(name=11)
...     for i in range(1,11):
...         n = g.create_node(name=i)
...         _ = start.edges.add(NEXT, n)
...         _ = n.edges.add(NEXT, end)
>>> # simple depth-first traverser that returns all nodes
>>> [n['name'] for n in traverser(lambda t: True, start, DFS, NEXT)]
[0, 1, 11, 2, 3, 4, 5, 6, 7, 8, 9, 10]
>>> # simple breadth-first traverser that returns all nodes
>>> [n['name'] for n in traverser(lambda t: True, start, BFS, NEXT)]
[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
>>> # using the traversal properties
>>> def evaluator(t):
...     if t.node['name'] < 3 or t.node['name'] in [5,7]:
...         return True # return the current node
...     if t.node['name'] == 7:
...         raise StopIteration # stop traversal
>>> [n['name'] for n in traverser(evaluator, start, BFS)]
[0, 1, 2, 5, 7]
"""

from __future__ import with_statement
from graph import Graph, INCOMING, OUTGOING
from traverse import TraverserGenerator, DFS, BFS
from storage.tc import TokyoCabinetStorageGroup


__all__ = ['graph', 'traverser', 'INCOMING', 'OUTGOING', 'DFS', 'BFS']


def graph(path):
    storage = TokyoCabinetStorageGroup(path)
    return Graph(storage)
    
    
def traverser(traversal_evaluator, start_node, traversal_algorithm, rel=None):
    return TraverserGenerator(traversal_evaluator, start_node, traversal_algorithm, rel)
    
    
if __name__ == "__main__":
    import doctest
    doctest.testmod()