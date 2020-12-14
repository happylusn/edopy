import logging
from datetime import datetime
from typing import Optional, Callable, Union, Dict, List, Tuple
import aiomysql
from enum import Enum


class Op(Enum):
    And = 'and'
    Or = 'or'
    Gt = '>'
    Gte = '>='
    Lt = '<'
    Lte = '<='
    Ne = '<>'
    Eq = '='
    IsNull = 'is null'
    NotNull = 'is not null'
    In = 'in'
    NotIn = 'not in'


def log(sql):
    logging.info('SQL: %s' % sql)


def create_pool(**kw):
    async def _create_pool(app):
        global _mysql_pool
        _mysql_pool = await aiomysql.create_pool(
            host=kw.get('host', 'localhost'),
            port=kw.get('port', 3306),
            user=kw['user'],
            password=kw['password'],
            db=kw['db'],
            charset=kw.get('charset', 'utf8'),
            autocommit=kw.get('autocommit', True),
            maxsize=kw.get('maxsize', 10),
            minsize=kw.get('minsize', 1)
        )
        app['__mysql_pool__'] = _mysql_pool
        yield
        app['__mysql_pool__'].close()
        await app['__mysql_pool__'].wait_closed()
    return _create_pool


async def select(sql: str, args: Optional[tuple] = None, size: Optional[int] = None):
    args = args or ()
    global _mysql_pool
    async with _mysql_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql.replace('?', '%s'), args)
            if size:
                res = await cur.fetchmany(size)
            else:
                res = await cur.fetchall()
            logging.info('rows returned: %s' % len(res))
            return res


async def execute(sql: str, args: Optional[tuple] = None):
    args = args or ()
    global _mysql_pool
    async with _mysql_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql.replace('?', '%s'), args)
            affected = cur.rowcount
            return affected


async def insert(sql: str, args: Optional[tuple] = None):
    args = args or ()
    global _mysql_pool
    async with _mysql_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql.replace('?', '%s'), args)
            return cur.lastrowid


def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)


class Field(object):

    def __init__(self, name: Optional[str], column_type: str, primary_key: bool, default):
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

    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)


class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=None, ddl='bigint'):
        super().__init__(name, ddl, primary_key, default)


class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default: Union[float, Callable[[], float]] = None):
        super().__init__(name, 'real', primary_key, default)


class TextField(Field):

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)


class DateTimeField(Field):

    def __init__(self, name=None, default: Union[float, Callable[[], float]] = None):
        super().__init__(name, 'datetime', False, default)


class ModelMetaclass(type):

    def __new__(mcs, name, bases, attrs):
        if name == 'Model':
            return type.__new__(mcs, name, bases, attrs)
        attrs['__slots__'] = []
        attrs['__dict__'] = {}
        table_name = attrs.get('__table__', None) or name
        mappings = dict()
        fields = []
        primary_key = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                attrs['__slots__'].append(k)
                mappings[k] = v
                if v.primary_key:
                    if primary_key:
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    primary_key = k
                else:
                    fields.append(k)
        if not primary_key:
            raise RuntimeError('Primary key not found.')
        for k in mappings.keys():
            attrs.pop(k)

        model = list(filter(lambda b: b is Model, bases))[0]
        attrs['__exist_created_at__'] = True if model.__created_at__ in fields else False
        attrs['__exist_updated_at__'] = True if model.__updated_at__ in fields else False
        if model.__timestamps__ is True:
            if model.__created_at__:
                attrs['__slots__'] = list({*attrs['__slots__'], model.__created_at__})
                mappings[model.__created_at__] = DateTimeField(default=datetime.now)
            if model.__updated_at__:
                attrs['__slots__'] = list({*attrs['__slots__'], model.__updated_at__})
                mappings[model.__updated_at__] = DateTimeField(default=datetime.now)
            if model.__created_at__ and model.__created_at__ not in fields:
                fields.append(model.__created_at__)
            if model.__updated_at__ and model.__updated_at__ not in fields:
                fields.append(model.__updated_at__)

        escaped_fields = list(map(lambda x: '`%s`' % x, fields))
        attrs['__mappings__'] = mappings  # 保存属性和列的映射关系
        attrs['__table__'] = table_name
        attrs['__primary_key__'] = primary_key  # 主键属性名
        attrs['__fields__'] = fields  # 除主键外的属性名
        attrs['__escaped_fields__'] = escaped_fields
        # 构造默认的SELECT, INSERT, UPDATE和DELETE语句:
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primary_key, ', '.join(escaped_fields), table_name)
        attrs['__insert__'] = 'insert into `%s` (%s) values (%s)' % (
            table_name, ', '.join(escaped_fields), create_args_string(len(escaped_fields)))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (
            table_name, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primary_key)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (table_name, primary_key)
        return type.__new__(mcs, name, bases, attrs)


WhereType = Dict[Union[str, Op], Union[int, str, float, tuple, List[Union[tuple, dict]]]]
FieldListType = List[str]


class Model(metaclass=ModelMetaclass):
    __timestamps__ = False
    __created_at__ = 'created_at'
    __updated_at__ = 'updated_at'

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            self[k] = v

    def __getattr__(self, key):
        try:
            return self.__dict__[key]
        except KeyError:
            raise AttributeError(r"'%s' object has no attribute '%s'" % (self.__class__.__name__, key))

    def __setattr__(self, key, value):
        self[key] = value

    def __setitem__(self, key, value):
        if key not in self.__slots__:
            raise AttributeError(r"'%s' object has no attribute '%s'" % (self.__class__.__name__, key))
        self.__dict__[key] = value

    def __getitem__(self, item):
        return self.__dict__[item]

    def get_value(self, key: str):
        return getattr(self, key, None)

    def get_value_or_default(self, key: str):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

    async def update(self):
        actual_fields = list(filter(lambda k: k != self.__primary_key__, self.__dict__.keys()))
        args = list(map(self.get_value, actual_fields))
        if self.__timestamps__ is True:
            if self.__updated_at__ and self.__updated_at__ not in actual_fields:
                actual_fields.append(self.__updated_at__)
                args.append(datetime.now())
            elif self.__updated_at__ and self.__updated_at__ in actual_fields:
                index = actual_fields.index(self.__updated_at__)
                args[index] = datetime.now()

        args.append(self.get_value(self.__primary_key__))
        sql = 'update `%s` set %s where `%s`=?' % (
            self.__table__, ', '.join(map(lambda f: '`%s`=?' % f, actual_fields)), self.__primary_key__)
        affected = await execute(sql, tuple(args))
        if affected != 1:
            logging.warning('failed to update by primary key: affected rows: %s' % affected)
        return affected

    @classmethod
    async def update_cls(
        cls,
        data: dict,
        where: dict
    ):
        sql = []
        args = []
        if data and isinstance(data, dict):
            keys = [k for k in data.keys()]
            sql.append('update %s set %s' % (cls.__table__, ', '.join(map(lambda k: '%s=?' % k, keys))))
            args.extend(map(lambda k: data[k], keys))
        else:
            raise ValueError('Invalid data value: %s' % str(data))
        if where and isinstance(where, dict):
            sql, args = cls._make_sql_and_args(
                sql_l=sql,
                args_l=args,
                where=where
            )
        else:
            raise ValueError('Invalid data value: %s' % str(data))
        affected = await execute(' '.join(sql), tuple(args))
        return affected

    @classmethod
    async def find(
        cls,
        pk: Optional[Union[int, str]] = None,
        where: Optional[WhereType] = None,
        attributes: Optional[FieldListType] = None,
        raw: bool = False
    ):
        sql_l = [cls.__select__]
        args_l = []
        if attributes:
            if isinstance(attributes, list):
                if cls.__primary_key__ not in attributes:
                    attributes.append(cls.__primary_key__)
                sql_l = [
                    'select %s from %s' % (', '.join(list(map(lambda k: '`%s`' % k, attributes))), cls.__table__)]
            else:
                raise ValueError('Invalid attributes value: %s' % str(attributes))
        if pk and (isinstance(pk, int) or isinstance(pk, str)):
            sql_l.append('where `%s`= ?' % cls.__primary_key__)
            args_l.append(pk)
        else:
            sql_l, args_l = cls._make_sql_and_args(
                sql_l=sql_l,
                args_l=args_l,
                where=where
            )
        rs = await select(' '.join(sql_l), tuple(args_l), 1)
        if len(rs) == 0:
            return None
        if raw is True:
            return rs[0]
        return cls(**rs[0])

    async def save(self):
        args = tuple(map(self.get_value_or_default, self.__fields__))
        last_rowid = await insert(self.__insert__, args)
        if last_rowid < 1:
            logging.warning('failed to insert record: %s' % str(self.__dict__))
        return last_rowid

    @classmethod
    async def findall(
        cls,
        where: Optional[WhereType] = None,
        attributes: Optional[FieldListType] = None,
        order_by: Optional[str] = None,
        limit: Optional[Tuple[int, int]] = None,
        raw: bool = False
    ) -> list:
        sql_l = [cls.__select__]
        if attributes:
            if isinstance(attributes, list):
                sql_l = [
                    'select %s from %s' % (', '.join(list(map(lambda k: '`%s`' % k, attributes))), cls.__table__)]
            else:
                raise ValueError('Invalid attributes value: %s' % str(attributes))
        sql_l, args_l = cls._make_sql_and_args(
            sql_l=sql_l,
            args_l=[],
            where=where,
            order_by=order_by,
            limit=limit
        )
        rs = await select(' '.join(sql_l), tuple(args_l))
        if raw is True:
            return [r for r in rs]
        return [cls(**r) for r in rs]

    # where = {'name': 'luu', 'time': [(Op.Gt, '2020-09-01'), (Op.Lt, '2020-09-02')]}
    @classmethod
    def _make_sql_and_args(
        cls,
        sql_l: Optional[list] = None,
        args_l: Optional[list] = None,
        where: Optional[WhereType] = None,
        order_by: Union[str, list, tuple] = None,
        limit: Optional[Tuple[int, int]] = None
    ) -> Tuple[list, list]:
        sql_l = sql_l if sql_l and isinstance(sql_l, list) else []
        args_l = args_l if args_l and isinstance(args_l, list) else []
        err_msg = 'Invalid where value: %s' % str(where)
        if where and isinstance(where, dict):
            for k, v in where.items():
                if isinstance(k, str) and isinstance(v, tuple):
                    cls._op_condition(k, v, sql_l, args_l, err_msg)
                elif isinstance(k, str) and (isinstance(v, int) or isinstance(v, str) or isinstance(v, float)):
                    sql_l = cls._append_sql_l('and %s = ?' % k, sql_l)
                    args_l.append(v)
                elif isinstance(k, str) and isinstance(v, list):
                    if len(v) > 0:
                        for t in v:
                            if isinstance(t, tuple):
                                sql_l, args_l = cls._op_condition(k, t, sql_l, args_l, err_msg)
                            else:
                                raise ValueError(err_msg)
                    else:
                        raise ValueError(err_msg)
                elif isinstance(k, Op) and isinstance(v, list) and len(v) > 0:
                    if k in [Op.And, Op.Or]:
                        sql_l = cls._append_sql_l('and (', sql_l)
                        for v2 in v:
                            if isinstance(v2, dict):
                                sql_l = cls._append_sql_l('%s (' % k.value, sql_l)
                                sql_l, args_l = cls._make_sql_and_args(
                                    sql_l=sql_l,
                                    args_l=args_l,
                                    where=v2
                                )
                                sql_l = cls._append_sql_l(')', sql_l)
                            else:
                                raise ValueError(err_msg)
                        sql_l = cls._append_sql_l(')', sql_l)
                    else:
                        raise ValueError(err_msg)
                else:
                    raise ValueError(err_msg)
        if order_by and isinstance(order_by, str):
            sql_l.append('order by %s' % order_by)
        if limit:
            if isinstance(limit, int):
                sql_l.append('limit ?')
                args_l.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql_l.append('limit ?, ?')
                args_l.extend(limit)
        return sql_l, args_l

    @classmethod
    def _op_condition(cls, k, v, sql_l, args_l, err_msg=None) -> Tuple[list, list]:
        if isinstance(v, tuple):
            if len(v) == 1:
                if isinstance(v[0], int) or isinstance(v[0], str) or isinstance(v[0], float):
                    sql_l = cls._append_sql_l('and %s = ?' % k, sql_l)
                    args_l.append(v[0])
                elif isinstance(v[0], Op) and v[0] in [Op.IsNull, Op.NotNull]:
                    sql_l = cls._append_sql_l('%s %s' % (k, v[0].value), sql_l)
                else:
                    raise ValueError(err_msg)
            elif len(v) == 2:
                if isinstance(v[0], Op):
                    if v[0] in [Op.In, Op.NotIn] and isinstance(v[1], list):
                        sql_l = cls._append_sql_l('and %s %s ?' % (k, v[0].value), sql_l)
                        args_l.append(str(tuple(v[1])))
                    elif not v[0] in [Op.IsNull, Op.NotNull] and (
                            isinstance(v[1], int) or isinstance(v[1], str) or isinstance(v[1], float)):
                        sql_l = cls._append_sql_l('and %s %s ?' % (k, v[0].value), sql_l)
                        args_l.append(v[1])
                    else:
                        raise ValueError(err_msg)
                else:
                    raise ValueError(err_msg)
            else:
                raise ValueError(err_msg)
        return sql_l, args_l

    @classmethod
    def _append_sql_l(cls, condition: str, sql_l) -> list:
        sql_list_len = len(sql_l)
        if sql_list_len:
            if sql_list_len == 1:
                sql_l.append('where')
            last_condition = sql_l[-1]
            if last_condition.endswith('(') or last_condition.endswith('where'):
                condition = condition.replace('and ', '')
                condition = condition.replace('or ', '')
            sql_l.append(condition)
        return sql_l


def init_model(**kw):
    _check_type(**kw)
    timestamps = kw.get('timestamps', False)
    created_at = kw.get('created_at', 'created_at')
    updated_at = kw.get('updated_at', 'updated_at')

    Model.__timestamps__ = timestamps
    Model.__created_at__ = created_at
    Model.__updated_at__ = updated_at


def table(**kw):
    _check_type(**kw)
    timestamps = kw.get('timestamps', Model.__timestamps__)
    created_at = kw.get('created_at', Model.__created_at__)
    updated_at = kw.get('updated_at', Model.__updated_at__)

    def _table(cls):
        cls.__timestamps__ = timestamps
        cls.__created_at__ = created_at
        cls.__updated_at__ = updated_at
        if cls.__timestamps__ is True:
            if cls.__created_at__:
                cls.__slots__ = list({*cls.__slots__, cls.__created_at__})
                cls.__mappings__[cls.__created_at__] = DateTimeField(default=datetime.now)
            if cls.__updated_at__:
                cls.__slots__ = list({*cls.__slots__, cls.__updated_at__})
                cls.__mappings__[cls.__updated_at__] = DateTimeField(default=datetime.now)
            if cls.__created_at__ and cls.__created_at__ not in cls.__fields__:
                cls.__fields__.append(cls.__created_at__)
            if cls.__updated_at__ and cls.__updated_at__ not in cls.__fields__:
                cls.__fields__.append(cls.__updated_at__)
        else:
            if cls.__created_at__ and cls.__created_at__ in cls.__slots__ and cls.__exist_created_at__ is False:
                cls.__slots__.remove(cls.__created_at__)
                cls.__mappings__.pop(cls.__created_at__)
            if cls.__updated_at__ and cls.__updated_at__ in cls.__slots__ and cls.__exist_updated_at__ is False:
                cls.__slots__.remove(cls.__updated_at__)
                cls.__mappings__.pop(cls.__updated_at__)
            if cls.__created_at__ and cls.__created_at__ in cls.__fields__ and cls.__exist_created_at__ is False:
                cls.__fields__.remove(cls.__created_at__)
            if cls.__updated_at__ and cls.__updated_at__ in cls.__fields__ and cls.__exist_updated_at__ is False:
                cls.__fields__.remove(cls.__updated_at__)

        cls.__escaped_fields__ = list(map(lambda k: '`%s`' % k, cls.__fields__))
        cls.__select__ = 'select `%s`, %s from `%s`' % (
            cls.__primary_key__, ', '.join(cls.__escaped_fields__), cls.__table__)
        cls.__insert__ = 'insert into `%s` (%s) values (%s)' % (
            cls.__table__, ', '.join(cls.__escaped_fields__), create_args_string(len(cls.__escaped_fields__)))
        cls.__update__ = 'update `%s` set %s where `%s`=?' % (
            cls.__table__, ', '.join(map(lambda f: '`%s`=?' % (cls.__mappings__.get(f).name or f), cls.__fields__)), cls.__primary_key__)
        return cls
    return _table


def _check_type(**kw):
    timestamps = kw.get('timestamps', None)
    created_at = kw.get('created_at', None)
    updated_at = kw.get('updated_at', None)
    if timestamps is not None:
        if not isinstance(timestamps, bool):
            raise TypeError(r"'timestamps' type must be a bool")
    if created_at is not None:
        if isinstance(created_at, bool) or isinstance(created_at, str):
            if isinstance(created_at, bool) and created_at is True:
                raise TypeError(r"'created_at' type must be a str or be False")
            elif isinstance(created_at, str) and len(created_at.strip()) == 0:
                raise TypeError(r"'created_at' can not be empty")
            elif timestamps is False and created_at is False:
                raise TypeError(r"'created_at' cannot be False when 'timestamps' is False")
        else:
            raise TypeError(r"'created_at' type must be a str or be False")
    if updated_at is not None:
        if isinstance(updated_at, bool) or isinstance(updated_at, str):
            if isinstance(updated_at, bool) and updated_at is True:
                raise TypeError(r"'updated_at' type must be a str or be False")
            elif isinstance(updated_at, str) and len(updated_at.strip()) == 0:
                raise TypeError(r"'updated_at' can not be empty")
            elif timestamps is False and updated_at is False:
                raise TypeError(r"'updated_at' cannot be False when 'timestamps' is False")
        else:
            raise TypeError(r"'updated_at' type must be a str or be False")


# init_model(timestamps=True, updated_at=False)
# @table(
#     timestamps=True
# )
# class User(Model):
#     __table__ = 'user'
#
#     id = IntegerField(primary_key=True)
#     nickname = StringField(ddl='varchar(50)')
#     email = StringField(ddl='varchar(128)')
#     password = StringField(ddl='varchar(255)')
#     openid = StringField(ddl='varchar(64)')
