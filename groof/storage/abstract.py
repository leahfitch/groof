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

"""Interfaces and abstract base classes for key-value storage.

At a minimum, implementations must provide IStorage and ITransaction.
"""



class IStorage(object):
    """Ordered key-value storage"""
    
    def __setitem__(self, k, v):
        """Set the key k to the value v."""
        raise NotImplementedError
    
    
    def __getitem__(self, k):
        """Get the value at key k. If k does not exist, raise KeyError."""
        raise NotImplementedError
    
    
    def __delitem__(self, k):
        """Remove the record at key k. If k does not exist, raise KeyError."""
        raise NotImplementedError
    
    
    def __contains__(self, k):
        """True if the key k exists"""
        raise NotImplementedError
    
    
    def __len__(self):
        """Return the total number of records"""
        raise NotImplementedError
    
    
    
class IDuplicateKeyStorage(object):
    """Storage supporting duplicate keys"""
    
    
    def setdup(self, k, v):
        """Set key k to the value of v. If k exists, add a new record."""
        raise NotImplementedError
        
        
    def getdup(self, k):
        """Get the values of all the records with key k."""
        raise NotImplementedError
        
        
    def deldup(self, k):
        """Remove all records with key k"""
        raise NotImplementedError
    
    
    
class IPrefixMatchingStorage(object):
    """Storage supporting key prefix matching"""
    
    def match_prefix(self, prefix):
        """Get keys matching the given prefix"""
    
    
    
class IFileStorage(IStorage):
    """Disk backed ordered key-value storage"""
    
    
    def open(self, path, mode):
        """Open the underlying disk file(s)"""
        raise NotImplementedError
    
    
    def close(self):
        """Close underlying disk file(s)"""
        raise NotImplementedError
    
    
    
class IStorageGroup(object):
    """Provide storage instance attributes node, left, right and indices"""
    
    def get_index(self, name):
        """Get an index (IStorage, IDuplicateKeyStorage)"""
        raise NotImplementedError