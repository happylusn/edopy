import asyncio
import inspect
import os,sys
from urllib import parse
from types import FunctionType
from config.config import configs

print(type(configs.db))

def has_request_arg(fn):
    sig = inspect.signature(fn)
    params = sig.parameters
    found = False
    for name, param in params.items():
        if name == 'request':
            found = True
            continue
        if found and (param.kind != inspect.Parameter.VAR_POSITIONAL and param.kind != inspect.Parameter.KEYWORD_ONLY and param.kind != inspect.Parameter.VAR_KEYWORD):
            raise ValueError('request parameter must be the last named parameter in function: %s%s' % (fn.__name__, str(sig)))
    return found

class A:
    def __init__(self):
        pass
    def say(self, name, *, root: str=None, ku=None):
        print(name)
    def __call__(self, name):
        self.say(name)



def cot(*k, **kw):
    print(k)
    print(kw)

# cot(111, request='ee')



# has_request_arg(cot)
# params = inspect.signature(cot).parameters
#
# for name, param in params.items():
#     print(name, type(param), param.kind, param.default)

#   POSITIONAL_ONLY 值必须是位置参数提供
#   POSITIONAL_OR_KEYWORD 值可以作为关键字或者位置参数提供
#   VAR_POSITIONAL 可变位置参数，对应*args
#   KEYWORD_ONLY keyword_only参数，对应*或者*args之后出现的非可变关键字参数
#   VAR_KEYWORD 可变关键字参数，对应**kwargs
#  param.annotation
k = ('a', 'b')
v = (100, 200)

for k, v in zip(k, v):
    print(k, v)

