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
...     n1.add_arc('hacks', n2, {'how':'valiantly'})
...     n2.add_arc('hacked_by', n1, {'how': 'painless'})
>>> n1['name']
'The Green Knight'
>>> n1.get_arcs('hacks')[0].successor().get_arcs('hacked_by')[0].successor()['name']
'The Green Knight'
"""

from storage.tc import TokyoCabinetStorage
from graph import Graph
from traverse import TraverserGenerator

__all__ = ['graph']


def graph(path):
    storage = TokyoCabinetStorage()
    storage.open(path, 'rw')
    return Graph(storage)
    
    
def traverse(traverser_func, start_v, traversal_algorithm, e=None):
    return InlineTraverser(traverser_func, start_v, traversal_algorithm, e).run()
    
    
if __name__ == "__main__":
    import doctest
    doctest.testmod()