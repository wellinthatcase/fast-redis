from .hyperredis import *

__doc__     = 'A small, hyper fast Python module exposing a simple interface to Redis commands.'
__all__     = []
__author__  = 'wellinthatcase'
__version__ = '0.1.0'

for name in globals().copy():
    if name not in __builtins__ and not name.endswith('__'):
        __all__.append(name)