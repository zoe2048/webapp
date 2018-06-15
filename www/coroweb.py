#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import asyncio, os, inspect, logging, functools

from urllib import parse

from aiohttp import web

from apis import APIError

def get(path):
    '''
    Define decorator @get('/path')
    '''
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            return func(*args, **kw)
        wrapper.__method__ = 'GET'
        wrapper.__route__ = path
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
        return wrapper
    return decorator

#运用inspect模块，创建以下几个函数用以获取URL处理函数与request参数之间的关系，参数定义的顺序必须是：必选参数（位置参数）、默认参数、可变参数（*arg)、命名关键字参数和关键字参数(**kw)。
# inspect.Parameter.kind 类型：
# POSITIONAL_ONLY          位置参数
# KEYWORD_ONLY             命名关键字参数
# VAR_POSITIONAL           可选参数 *args
# VAR_KEYWORD              关键字参数 **kw
# POSITIONAL_OR_KEYWORD    位置或必选参数
def get_required_kw_args(fn):   #获取没有赋默认值的命名关键字参数
    args = []
    '''''
    def f(a,b=1,*arg,d,**kw):pass
    params=inspect.signature(f).parameters
    params = mappingproxy(OrderedDict([('a', <Parameter "a">), ('b', <Parameter "b=1">), ('arg', <Parameter "*arg">), ('d', <Parameter "d">), ('kw', <Parameter "**kw">)]))
    params.items()=odict_items([('a', <Parameter "a">), ('b', <Parameter "b=1">), ('arg', <Parameter "*arg">), ('d', <Parameter "d">), ('kw', <Parameter "**kw">)])

    '''''
    params = inspect.signature(fn).parameters      
    for name, param in params.items():
        if param.kind == inspect.Parameter.KEYWORD_ONLY and param.default == inspect.Parameter.empty: #如果函数参数的类型为命令关键字参数且没有赋默认值
            args.append(name)
    return tuple(args)

def get_named_kw_args(fn):      #获取命名关键字参数
    args = []
    params = inspect.signature(fn).parameters
    for name, param in params.items():
        if param.kind == inspect.Parameter.KEYWORD_ONLY:   #若参数类型为命名关键字参数
            args.append(name)
    return tuple(args)

def has_named_kw_args(fn):       #判断有没有命名关键字参数
    params = inspect.signature(fn).parameters
    for name, param in params.items():
        if param.kind == inspect.Parameter.KEYWORD_ONLY:    #若参数未命名关键字参数
            return True

def has_var_kw_arg(fn):        #判断有没有关键字参数
    params = inspect.signature(fn).parameters
    for name, param in params.items():
        if param.kind == inspect.Parameter.VAR_KEYWORD:    #若参数类型为关键字参数
            return True

def has_request_arg(fn):     #判断函数参数是否含有名叫‘request’参数，且该参数是否为最后一个参数，以布尔型返回结果
    sig = inspect.signature(fn)
    params = sig.parameters
    found = False
    for name, param in params.items():
        if name == 'request':
            found = True
            continue
        # 若有request参数且参数类型不是可变参数、命名关键字参数、关键字参数
        if found and (param.kind != inspect.Parameter.VAR_POSITIONAL and param.kind != inspect.Parameter.KEYWORD_ONLY and param.kind != inspect.Parameter.VAR_KEYWORD):
            raise ValueError('request parameter must be the last named parameter in function: %s%s' % (fn.__name__, str(sig)))
    return found

#RequestHandler目的：从URL处理函数中分析其参数，从web.Request中获取必要的参数，调用URL函数，然后把结果转换为web.Response对象，以满足符合aiohttp框架的要求
class RequestHandler(object):

    def __init__(self, app, fn):
        self._app = app
        self._func = fn
        self._has_request_arg = has_request_arg(fn)
        self._has_var_kw_arg = has_var_kw_arg(fn)
        self._has_named_kw_args = has_named_kw_args(fn)
        self._named_kw_args = get_named_kw_args(fn)
        self._required_kw_args = get_required_kw_args(fn)

    @asyncio.coroutine
    def __call__(self, request):
        # 有__call__方法，类的实例可以当做函数来调用
        # 用来统一处理 函数add_route()中app.router.add_route(method, path, RequestHandler(app, fn))的RequestHandler(app, fn)即handlers中的函数
        kw = None   #定义kw，用于保存request中参数
        if  self._has_named_kw_args or self._required_kw_args or self._has_var_kw_arg : #若有命名关键字参数或没有赋默认值的命名关键字参数或关键字参数
            if request.method == 'POST':
                if not request.content_type:
                    return web.HTTPBadRequest('Missing Content-Type.')
                    # return web.HTTPBadRequest(text='Missing Content-Type.')
                ct = request.content_type.lower()
                if ct.startswith('application/json'):
                    params = yield from request.json()  #仅解析request的body字段的json数据，request.json()返回dict对象
                    if not isinstance(params, dict):
                        return web.HTTPBadRequest('JSON body must be object.')
                    kw = params
                elif ct.startswith('application/x-www-form-urlencoded') or ct.startswith('multipart/form-data'): #from表单请求的编码形式
                    params = yield from request.post()   #返回post的内容中解析后的数据，dict-like对象
                    kw = dict(**params)  #格式化为dict，统一不同content_type下的kw为同一格式
                else:
                    return web.HTTPBadRequest('Unsupported Content-Type: %s' % request.content_type)
            if request.method == 'GET':
                qs = request.query_string  #返回URL查询语句?后的键值，string形式。
                if qs:
                    ''''' 
                      解析url中?后面的键值对的内容如一个url ：http://www.comei.cn/getar?qs='first=f,s&second=s'  
                      qs = 'first=f,s&second=s' 
                      parse.arpse_qs(qs, True).items() 
                      >>> dict([('first', ['f,s']), ('second', ['s'])]) 
                      >>>kw = {'first': 'f,s', 'second': 's'}
                  '''
                    kw = dict()
                    for k, v in parse.parse_qs(qs, True).items():
                        kw[k] = v[0]
        if kw is None:
            # 若request中没有参数
            # request.match_info返回dict对象，可变路由中的可变字段{variable}为参数名，传入request请求的path为值
            # 如若存在可变路由：/a/{name}/c，可匹配path为：/a/jack/c的request
            # 则request.match_info返回{name = jack}
            kw = dict(**request.match_info)
        else: #若request中有参数
            if not self._has_var_kw_arg and self._named_kw_args: #若参数不是关键字参数且有命名关键字参数
                # remove all unamed kw，即只保留命名关键字参数:
                copy = dict()
                for name in self._named_kw_args:
                    if name in kw:
                        copy[name] = kw[name]
                kw = copy
            # check named arg，检查kw中的参数是否和match_info中的重复:
            for k, v in request.match_info.items():
                if k in kw:
                    logging.warning('Duplicate arg name in named arg and kw args: %s' % k)
                kw[k] = v
        if self._has_request_arg:
            kw['request'] = request
        # check required kw:
        if self._required_kw_args:
            for name in self._required_kw_args:
                if not name in kw:
                    return web.HTTPBadRequest('Missing argument: %s' % name)
        #以上完成根据不同请求方式下请求参数的处理并保存到kw
        #request请求中的参数传递给URL处理函数
        logging.info('call with args: %s' % str(kw))
        try:
            r = yield from self._func(**kw)
            return r
        except APIError as e:
            return dict(error=e.error, data=e.data, message=e.message)

def add_static(app):
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
    app.router.add_static('/static/', path)
    logging.info('add static %s => %s' % ('/static/', path))

def add_route(app, fn):
    method = getattr(fn, '__method__', None)
    path = getattr(fn, '__route__', None)
    if path is None or method is None:  #再次确认fn为URL注册函数
        raise ValueError('@get or @post not defined in %s.' % str(fn))
    if not asyncio.iscoroutinefunction(fn) and not inspect.isgeneratorfunction(fn):
        fn = asyncio.coroutine(fn)
    logging.info('add route %s %s => %s(%s)' % (method, path, fn.__name__, ', '.join(inspect.signature(fn).parameters.keys())))
    #web服务器此处为app，调用URL处理函数（统一使用RequestHandler(app,fn)封装过的）
    print('this is:',RequestHandler(app,fn))
    app.router.add_route(method, path, RequestHandler(app, fn))    #了解查看aiohttp下web Application类的router.add_route(self，method，path，handler,*,name=None,expect_hander=None)方法

def add_routes(app, module_name):
    n = module_name.rfind('.')  #字符串自带的rfind方法，返回.前面的字符个数
    if n == (-1):  #字符串中没有点号.，返回值为-1，根据调用者给的module_name是否有.判断 ，用于在动态导入模块时，模块名称为xxx或者xxx.xxxx的情况
        mod = __import__(module_name, globals(), locals())   #内置的函数,用于需要动态加载模块时，此处根据app.py的调用为handlers，动态加载模块为handlers
    else:
        name = module_name[n+1:]      #切片获取.号后的字符串，注意切片是从0开始，n+1刚好是.号后的第一个字符开始切片，如aaa.bbb,  name=bbb
        mod = getattr(__import__(module_name[:n], globals(), locals(), [name]), name)    #module_name[:n]切片结果为aaa ，mode=aaa.bbb
    for attr in dir(mod):    #dir(模块名)获取模块中所有的方法等，此处为handlers模块，其中只有函数
        if attr.startswith('_'): #跳过所有_开头的方法等
            continue
        fn = getattr(mod, attr)  #获取模块mod中函数，将其赋值给fn
        if callable(fn):  #返回fn是否可调用
            method = getattr(fn, '__method__', None)          #获取fn即URL处理函数的__method__属性，若没有对应的__method__，则返回None
            path = getattr(fn, '__route__', None)
            if method and path:       #若fn满足条件（有method和path)，注册url函数，避免非url函数被注册
                add_route(app, fn) #调用add_route()开始一个一个注册URL处理函数
