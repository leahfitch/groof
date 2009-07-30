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
"""

from __future__ import with_statement
from graph import Graph, INCOMING, OUTGOING
from storage.tc import TokyoCabinetStorageGroup


def graph(path):
    storage = TokyoCabinetStorageGroup(path)
    return Graph(storage)
    
    
if __name__ == "__main__":
    import doctest
    doctest.testmod()