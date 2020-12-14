import inspect, os, functools
import logging
from urllib import parse

from aiohttp import web
from typing import Tuple

from aiohttp.web_request import Request


def controller(root: str):
    if root and not root.startswith("/"):
        raise ValueError("root should be started with / or be empty")

    def decorator(cls):
        cls.__route_root__ = root
        cls.__controller_path__ = cls.__module__
        cls.__controller_name__ = cls.__name__
        return cls
    return decorator


def get(path):
    '''
    Define decorator @get('/path')
    '''
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kw):
            return func(self, *args, **kw)
        wrapper.__method__ = 'GET'
        wrapper.__route__ = path
        wrapper.__action__ = func.__name__
        return wrapper
    return decorator


def post(path):
    '''
    Define decorator @post('/path')
    '''
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            return func(*args, **kw)
        wrapper.__method__ = 'POST'
        wrapper.__route__ = path
        wrapper.__action__ = func.__name__
        return wrapper
    return decorator


def _normalize_path(path: str):
    path = path if path.startswith('/') else '/%s' % path
    path = path[:-1] if path.endswith('/') else path
    return path

#   POSITIONAL_ONLY 值必须是位置参数提供
#   POSITIONAL_OR_KEYWORD 值可以作为关键字或者位置参数提供
#   VAR_POSITIONAL 可变位置参数，对应*args
#   KEYWORD_ONLY keyword_only参数，对应*或者*args之后出现的非可变关键字参数
#   VAR_KEYWORD 可变关键字参数，对应**kwargs


def get_required_kw_args(fn):
    args = []
    params = inspect.signature(fn).parameters
    for name, param in params.items():
        if name == 'self':
            continue
        if param.kind == inspect.Parameter.KEYWORD_ONLY and param.default == inspect.Parameter.empty:
            args.append(name)
    return tuple(args)


def get_named_kw_args(fn):
    args = []
    params = inspect.signature(fn).parameters
    for name, param in params.items():
        if name == 'self':
            continue
        if param.kind == inspect.Parameter.KEYWORD_ONLY:
            args.append(name)
    return tuple(args)


def has_var_kw_arg(fn):
    params = inspect.signature(fn).parameters
    for name, param in params.items():
        if name == 'self':
            continue
        if param.kind == inspect.Parameter.VAR_KEYWORD:
            return True


def has_request_arg(fn):
    sig = inspect.signature(fn)
    params = sig.parameters
    found = False
    for name, param in params.items():
        if name == 'self':
            continue
        if name == 'request':
            found = True
            continue
        if found and (param.kind != inspect.Parameter.VAR_POSITIONAL and param.kind != inspect.Parameter.KEYWORD_ONLY and param.kind != inspect.Parameter.VAR_KEYWORD):
            raise ValueError('request parameter must be the last named parameter in function: %s%s' % (fn.__name__, str(sig)))
    return found


def has_app_arg(fn):
    sig = inspect.signature(fn)
    params = sig.parameters
    found = False
    for name, param in params.items():
        if name == 'app':
            found = True
            break
    return found


def check_arg_invalid(fn):
    sig = inspect.signature(fn)
    params = sig.parameters
    for name, param in params.items():
        if name == 'self' or name == 'request' or name == 'app':
            continue
        if param.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD or param.kind == inspect.Parameter.VAR_POSITIONAL:
            raise ValueError('%s parameter is not nonvariable keyword arguments in function: %s%s' % (name, fn.__name__, str(sig)))


class RequestHandler:
    def __init__(self, app, callback: Tuple[type, str]):
        self._app = app
        self._callback = callback
        controller = self._callback[0]
        action_name = self._callback[1]
        check_arg_invalid(controller)
        check_arg_invalid(getattr(controller, action_name))
        self._ct_has_app_arg = has_app_arg(controller)
        self._ct_has_request_arg = has_request_arg(controller)
        self._ct_has_var_kw_arg = has_var_kw_arg(controller)
        self._ct_named_kw_args = get_named_kw_args(controller)
        self._ct_required_kw_args = get_required_kw_args(controller)
        self._ac_has_app_arg = has_app_arg(getattr(controller, action_name))
        self._ac_has_request_arg = has_request_arg(getattr(controller, action_name))
        self._ac_has_var_kw_arg = has_var_kw_arg(getattr(controller, action_name))
        self._ac_named_kw_args = get_named_kw_args(getattr(controller, action_name))
        self._ac_required_kw_args = get_required_kw_args(getattr(controller, action_name))

    async def __call__(self, request: Request):
        con_kw = dict()
        act_kw = dict()
        if self._ct_has_var_kw_arg or self._ct_named_kw_args:
            con_kw = await self.get_request_params(request)
        if self._ac_has_var_kw_arg or self._ac_named_kw_args:
            act_kw = await self.get_request_params(request)

        if not self._ct_has_var_kw_arg and self._ct_named_kw_args:
            actual = dict()
            for name in self._ct_named_kw_args:
                if name in con_kw:
                    actual[name] = con_kw[name]
            con_kw = actual
        if not self._ac_has_var_kw_arg and self._ac_named_kw_args:
            actual = dict()
            for name in self._ac_named_kw_args:
                if name in act_kw:
                    actual[name] = act_kw[name]
            act_kw = actual
        if self._ct_has_app_arg:
            con_kw['app'] = self._app
        if self._ac_has_app_arg:
            act_kw['app'] = self._app
        if self._ct_has_request_arg:
            con_kw['request'] = request
        if self._ac_has_request_arg:
            act_kw['request'] = request
        if self._ct_required_kw_args:
            for name in self._ct_required_kw_args:
                if name not in con_kw:
                    return web.HTTPBadRequest(reason='Missing argument: %s' % name)
        if self._ac_required_kw_args:
            for name in self._ac_required_kw_args:
                if name not in act_kw:
                    return web.HTTPBadRequest(reason='Missing argument: %s' % name)

        res = await getattr(self._callback[0](**con_kw), self._callback[1])(**act_kw)
        return res

    async def get_request_params(self, request: Request):
        kw = None
        if request.method == 'POST':
            if not request.content_type:
                return web.HTTPBadRequest(reason='Missing Content-Type.')
            ct = request.content_type.lower()
            if ct.startswith('application/json'):
                params = await request.json()
                if not isinstance(params, dict):
                    return web.HTTPBadRequest(reason='JSON body must be object.')
                kw = params
            elif ct.startswith('application/x-www-form-urlencoded') or ct.startswith('multipart/form-data'):
                params = await request.post()
                kw = dict(**params)
            else:
                return web.HTTPBadRequest(reason='Unsupported Content-Type: %s' % request.content_type)
        if request.method == 'GET':
            qs = request.query_string
            if qs:
                kw = dict()
                for k, v in parse.parse_qs(qs, True).items():
                    kw[k] = v[0]
        if kw is None:
            kw = dict(**request.match_info)
        else:
            for k, v in request.match_info.items():
                if k in kw:
                    logging.warning('Duplicate arg name in named arg and kw args: %s' % k)
                kw[k] = v
        return kw


def add_route(app, callback: Tuple[type, str]):
    cls = callback[0]
    action_name = callback[1]
    if type(cls) != type or not isinstance(action_name, str):
        raise ValueError('Invalid callback value: %s' % str(callback))
    path_root = getattr(cls, '__route_root__', None)
    if path_root is None:
        raise ValueError('@controller not defined in %s.' % str(cls))
    members = inspect.getmembers(cls)
    for name, attr in members:
        if name == action_name and callable(attr):
            method = getattr(attr, '__method__', None)
            path = getattr(attr, '__route__', None)
            if path is None or method is None:
                raise ValueError('@get or @post not defined in %s.' % str(attr))
            path = _normalize_path(path_root) + _normalize_path(path)
            logging.info('regist metod: %s path: %s' % (method, path))
            app.router.add_route(method, path, RequestHandler(app, callback))
            break


def add_routes(app, api_path: str, module_name: str):
    for root, dirs, files in os.walk(api_path):
        file_list = []
        for file in files:
            if file.endswith('.py'):
                file_list.append(os.path.splitext(file)[0])
        module = __import__(module_name, globals(), locals(), file_list)
        for file in file_list:
            mod = getattr(module, file)
            for attr in dir(mod):
                if attr.startswith('_'):
                    continue
                ct = getattr(mod, attr)
                if type(ct) == type and getattr(ct, '__route_root__', None):
                    members = inspect.getmembers(ct)
                    for name, entity in members:
                        if name.startswith('_') or not callable(entity):
                            continue
                        if getattr(entity, '__method__', None):
                            add_route(app, (ct, name))
        for dr in dirs:
            if dr.startswith('_'):
                continue
            add_routes(app, os.path.join(api_path, dr), module_name + '.%s' % dr)
        break

