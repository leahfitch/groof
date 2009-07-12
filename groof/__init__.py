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

>>> g = graph('/tmp/grooftest')
>>> with g.txn():
...     n1 = g.create()
...     n2 = g.create()
...     n1['name'] = 'The Green Knight'
...     n2['name'] = 'The Black Knight'
...     n1.add_arc('hacks', n2)
>>> n1['name']
'The Green Knight'
>>> n1.get_arcs('hacks')[0].end.get_arcs('hacks', direction=INCOMING)[0].end['name']
'The Green Knight'
>>> with g.txn():
...     start = g.create(attrs={'name':0})
...     end = g.create(attrs={'name':11})
...     for i in range(1,11):
...         v = g.create()
...         v['name'] = i
...         start.add_arc('next', v)
...         v.add_arc('next', end)
>>> [v['name'] for v in traverser(lambda t: True, start, DFS, 'next')]
[0, 1, 11, 2, 3, 4, 5, 6, 7, 8, 9, 10]
>>> [v['name'] for v in traverser(lambda t: True, start, BFS, 'next')]
[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
>>> def evaluator(t):
...     if t.vertex['name'] < 3 or t.vertex['name'] in [5,7]:
...         return True
...     if t.vertex['name'] == 7:
...         raise StopIteration
>>> [v['name'] for v in traverser(evaluator, start, BFS)]
[0, 1, 2, 5, 7]
"""

from storage.tc import TokyoCabinetStorage
from graph import Graph, INCOMING, OUTGOING
from traverse import TraverserGenerator, DFS, BFS

__all__ = ['graph', 'INCOMING', 'OUTGOING', 'DFS', 'BFS']


def graph(path):
    storage = TokyoCabinetStorage()
    storage.open(path, 'rw')
    return Graph(storage)
    
    
def traverser(traversal_evaluator, start_v, traversal_algorithm, label=None):
    return TraverserGenerator(traversal_evaluator, start_v, traversal_algorithm, label)
    
    
if __name__ == "__main__":
    import doctest
    doctest.testmod()