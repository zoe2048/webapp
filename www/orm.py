#  -*- coding:utf-8 -*-


# ORM框架理解
# https://blog.csdn.net/haskei/article/details/57075381

import sys
import asyncio
import logging;logging.basicConfig(level=logging.INFO)
import aiomysql

def log(sql,args=()):
    logging.info('SQL:%s' % sql)

@asyncio.coroutine
def create_pool(loop,**kw):  
    logging.info('create database connection pool...')
    global __pool
    #这里有一个关于Pool的连接，讲了一些Pool的知识点，挺不错的，<a target="_blank" href="http://aiomysql.readthedocs.io/en/latest/pool.html">点击打开链接</a>，下面这些参数都会讲到，以及destroy__pool里面的  
    __pool = yield from aiomysql.create_pool(
        host=kw.get('host','192.168.99.100'), #使用字典自带的get方法
        port=kw.get('port',3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset','utf8'),
        autocommit=kw.get('autocommit',True),
        maxsize=kw.get('maxsize',10),
        minsize=kw.get('minsize',1),
        loop=loop
    )

@asyncio.coroutine
def destroy_pool():
    global __pool
    if __pool is not None:
        __pool.close()
        yield from __pool.wait_closed() 


@asyncio.coroutine
def select(sql,args,size=None):
    log(sql,args)
    global __pool
    with (yield from __pool) as conn:
        cur=yield from conn.cursor(aiomysql.DictCursor)
        yield from cur.execute(sql.replace('?','%s'),args)
        if size:
            rs=yield from cur.fetchmany(size)
        else:
            rs=yield from cur.fetchall()
        yield from cur.close()
        logging.info('row have returned %s' %len(rs))
    return rs


#理解调用aiomysql的execute()方法参数应满足的要求https://aiomysql.readthedocs.io/en/latest/cursors.html?highlight=execute
@asyncio.coroutine  
def execute(sql,args, autocommit=True):
    log(sql)  
    global __pool  
    with (yield from __pool) as conn:  
        try:  
            cur = yield from conn.cursor()  
            yield from cur.execute(sql.replace('?', '%s'), args)   #replace使用%s替代sql中的？，args为待插入的字段的value，以列表方式，如sql=insert into users (id,name,paassword,email) values ('%s','%s','%s','%s'),args=['1111','zl','123456','xx@xx.com']
            yield from conn.commit() 
            affected_line=cur.rowcount  
            yield from cur.close()  
            print('execute : ', affected_line)  
        except BaseException as e:  
            raise  
        return affected_line  


def create_args_string(num):
    lol=[]
    for n in range(num):
        lol.append('?')
    return (','.join(lol))


class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)


class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)

class BooleanField(Field):
    def __init__(self,name=None,default=False):
        super().__init__(name,'Boolean',False,default)

class IntegerField(Field):
    def __init__(self,name=None,primary_key=False,default=0.0):
        super().__init__(name, 'float', primary_key, default)

class FloatField(Field):  
    def __init__(self, name=None, primary_key=False,default=0.0):  
        super().__init__(name, 'float', primary_key, default)  

class TextField(Field):
    def __init__(self, name=None, default=None):  
        super().__init__(name,'text',False, default)     
        

  
class ModelMetaclass(type):  
    def __new__(cls, name, bases, attrs):   #元类及元类中__new__()方法的理解：在调用__init__()方法前被调用的特殊方法，可以修改使用metaclass来创建的类的定义，cls：当前准备创建的类的对象，name：类的名字，bases：类继承的父类集合，attrs：类的方法集合
        if name=='Model':   #排除掉对Model类的修改，为的是在Model类的子类继承使用ModleMetaclass创建时，对Model产生影响
            return type.__new__(cls, name, bases, attrs)  
        table_name=attrs.get('__table__', None) or name   #获取表名，若没有__table__，则使用类名做为表名
        logging.info('found table: %s (table: %s) ' %(name,table_name ))
        #found table:User (table: users)
        mappings=dict()  
        fields=[]   
        primaryKey=None   
        for k, v in attrs.items():   #attrs.items()获取字典items：如字典a={'id':'1111','name':'hihi'}，a.imtes()为dict_items([('id', '1111'), ('name', 'hihi')])
            if isinstance(v, Field):  
                logging.info('Found mapping %s===>%s' %(k, v))  
                mappings[k] = v  #以字典存储字段及字段对应的字段类型对象如'id', <orm.StringField object at 0x033BEEF0>
                if v.primary_key:  #字段对象的属性：primary_key是否为true
                    logging.info('fond primary key %s'%k)  
                    if primaryKey:  #判断是否已经赋过值给primaryKey，防止主键字段重复，因主键唯一
                        raise RuntimeError('Duplicated key for field') 
                    primaryKey=k   #若字段是主键，将该主键赋值给primaryKey
                else:  
                    fields.append(k) # 对非主键的字段加到fields列表中
        if not primaryKey:   
            raise RuntimeError('Primary key not found!')    
        for k in mappings.keys():  #前面找到的已存入类实例如User的mappings中的字段及字段类型对象，在类属性中删除该字段属性，防止实例属性对元类内因同名属性的覆盖
            attrs.pop(k)  
        escaped_fields=list(map(lambda f:'`%s`' % f, fields))  #将除主键外的字段都按照这种反引号的形式：如字段email改为`email`，为了满足sql语句格式的相关书写格式,熟悉map的用法
        attrs['__mappings__']=mappings #字段及字段类型对象的字典 
        attrs['__table__']=table_name #表名
        attrs['__primary_key__']=primaryKey #表主键  
        attrs['__fields__']=fields  #表除主键外的字段
        #构造数据库select、insert、update、delete的通用语法
        attrs['__select__']='select `%s`, %s from `%s` '%(primaryKey,', '.join(escaped_fields), table_name)  
        attrs['__insert__'] = 'insert into  `%s` (%s, `%s`) values (%s) ' %(table_name, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields)+1))  #insert语句里values(%s)使用create_args_string()返回的？代替，个数由表的字段决定
        attrs['__update__']='update `%s` set %s where `%s` = ?' % (table_name, ', '.join(map(lambda f:'`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)  
        attrs['__delete__']='delete from `%s` where `%s`=?' %(table_name, primaryKey)  
        return type.__new__(cls, name, bases, attrs)  


class Model(dict,metaclass=ModelMetaclass):  #使用ModelMetaclass定制类Model，创建的Modle要通过ModelMetaclass.__new__()来创建类
    def __init__(self, **kw): 
        super(Model,self).__init__(**kw)   #理解super，调用父类dict
        
    def __getattr__(self, key):   #动态返回类属性 <a href="https://www.liaoxuefeng.com/wiki/0014316089557264a6b348958f449949df42a6d3a2e542c000/0014319098638265527beb24f7840aa97de564ccc7f20f6000">参考资料</a>
        try:  
            return self[key]  
        except KeyError:  
            raise AttributeError("'Model' object have no attribution: %s"% key)
        
    def __setattr__(self, key, value):   #字典中添加键/值对
        self[key] =value
        
    def getValue(self, key):  
        return getattr(self, key, None)  #获取字典key对应的value
    
    def getValueOrDefault(self, key):  
        value=getattr(self, key , None)  
        if value is None:
            field = self.__mappings__[key]  
            if field.default is not None:    #根据传入的字段的default属性是否为None，
                value = field.default() if callable(field.default) else field.default  
                logging.info('using default value for %s : %s ' % (key, str(value)))  
                setattr(self, key, value)  
        return value
    
    @classmethod  
    @asyncio.coroutine  
    def find_all(cls, where=None, args=None, **kw):  
        sql = [cls.__select__]  
        if where:  
            sql.append('where')  
            sql.append(where)  
        if args is None:  
            args = []  
        orderBy = kw.get('orderBy', None)  
        if orderBy:  
            sql.append('order by')  
            sql.append(orderBy)   
        limit = kw.get('limit', None)  
        if limit is not None:  
            sql.append('limit')  
            if isinstance(limit, int):  
                sql.append('?')  
                args.append(limit)  
            elif isinstance(limit, tuple) and len(limit) ==2:  
                sql.append('?,?')  
                args.extend(limit)  
            else:  
                raise ValueError('Invalid limit value self.__insert__,: %s ' % str(limit))  
        rs = yield from select(' '.join(sql),args) 
        return [cls(**r) for r in rs]

    @classmethod  
    @asyncio.coroutine  
    def findNumber(cls, selectField, where=None, args=None):  
        sql = ['select %s __num__ from `%s`' %(selectField, cls.__table__)]  
        if where:  
            sql.append('where')  
            sql.append(where)  
        rs = yield from select(' '.join(sql), args, 1)  
        if len(rs) == 0:  
            return None  
        return rs[0]['__num__']

    @classmethod  
    @asyncio.coroutine  
    def find(cls, primarykey):  
        rs = yield from select('%s where `%s`=?' %(cls.__select__, cls.__primary_key__), [primarykey], 1)  
        if len(rs) == 0:  
            return None  
        return cls(**rs[0])


    @classmethod
    @asyncio.coroutine
    def findAll(cls, where=None, args=None, **kw):
        ' find objects by where clause. '
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = yield from select(' '.join(sql), args)
        return [cls(**r) for r in rs]


    '''
    # 以下findAll(cls,**kw)中少了参数，导致注册时在报错：users = yield from User.findAll('email=?', [email])
    # TypeError: findAll() takes 1 positional argument but 3 were given
    # 解决：修改为使用上面的findAll(cls, where=None, args=None, **kw)
    @classmethod  
    @asyncio.coroutine  
    def findAll(cls, **kw):  
        rs = []  
        if len(kw) == 0:  
            rs = yield from select(cls.__select__, None)  
        else:  
            args=[]  
            values=[]  
            for k, v in kw.items():  
                args.append('%s=?' % k )  
                values.append(v)  
            print('%s where %s ' % (cls.__select__,  ' and '.join(args)), values)  
            rs = yield from select('%s where %s ' % (cls.__select__,  ' and '.join(args)), values)  
        return rs
    '''
    
    @asyncio.coroutine  
    def save(self):  
        args = list(map(self.getValueOrDefault, self.__fields__))   #得到args，以list列表方式存储，用于满足调用aiomysql模块的execute()方法的参数
        print('save:%s' % args)  
        args.append(self.getValueOrDefault(self.__primary_key__))  #将主键加入args
        rows = yield from execute(self.__insert__, args)  
        if rows != 1:  
            print(self.__insert__)  
            logging.warning('failed to insert record: affected rows: %s' %rows)
            
    @asyncio.coroutine  
    def update(self): 
        args = list(map(self.getValue, self.__fields__))  
        args.append(self.getValue(self.__primary_key__))  
        rows = yield from execute(self.__update__, args)  
        if rows != 1:  
            logging.warning('failed to update record: affected rows: %s'%rows)  
  
    @asyncio.coroutine  
    def delete(self):  
        args = [self.getValue(self.__primary_key__)]  
        rows = yield from execute(self.__delete__, args)  
        if rows != 1:  
            logging.warning('failed to delete by primary key: affected rows: %s' %rows)  
   
   
"""
if __name__=="__main__":
    class User2(Model): 
        id = IntegerField('id',primary_key=True)
        name = StringField('name')  
        email = StringField('email')  
        password = StringField('password')
        loop = asyncio.get_event_loop()  
   
    #创建实例  
    @asyncio.coroutine  
    def test():  
        yield from create_pool(loop=loop, host='localhost', port=3306, user='root', password='Limin123?', db='test')  
        #user = User2(id=2, name='Tom', email='slysly759@gmail.com', password='12345')  
        r = yield from User2.findAll()  
        print(r)  
        #yield from user.save()  
        #ield from user.update()  
        #yield from user.delete()  
        # r = yield from User2.find(8)  
        # print(r)  
        # r = yield from User2.findAll()  
        # print(1, r)  
        # r = yield from User2.findAll(name='sly')  
        # print(2, r)  
        yield from destroy_pool()  #关闭pool  
   
    loop.run_until_complete(test())  
    loop.close()  
    if loop.is_closed():  
        sys.exit(0)
"""
            
                    
