import logging
import time
from typing import Optional, Callable, Union, Dict, List, Tuple
import aiomysql
import asyncio
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


def log(sql, args=()):
    logging.info('SQL: %s' % sql)


async def create_pool(app, **kw):
    user, password, db = 'root', '123456', 'egg_db'
    global _mysql_pool
    _mysql_pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=user,
        password=password,
        db=db,
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1)
    )
    app['__mysql_pool__'] = _mysql_pool
    yield
    app['__mysql_pool__'].close()
    await app['__mysql_pool__'].wait_closed()


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
            lastrowid = cur.lastrowid
            return affected, lastrowid


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

    def __init__(self, name=None, primary_key=False, default=0, ddl='bigint'):
        super().__init__(name, ddl, primary_key, default)


class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default: Union[float, Callable[[], float], None] = 0.0):
        super().__init__(name, 'real', primary_key, default)


class TextField(Field):

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)


class ModelMetaclass(type):

    def __new__(cls, name, bases, attrs):
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        table_name = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, table_name))
        mappings = dict()
        fields = []
        primary_key = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
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
        escaped_fields = list(map(lambda k: '`%s`' % k, fields))
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
        return type.__new__(cls, name, bases, attrs)


WhereType = Dict[Union[str, Op], Union[int, str, float, tuple, List[Union[tuple, dict]]]]
FieldListType = List[str]


class Model(dict, metaclass=ModelMetaclass):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def __setitem__(self, key, value):
        self.__dict__[key] = value

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

    # async def update(self, E=None, **F):
    #     actual_fields = []
    #     args = []
    #     for k in self.keys():
    #         if k in self.__fields__:
    #             actual_fields.append(k)
    #             args.append(self.get_value(k))


    @classmethod
    async def find(
        cls,
        pk: Optional[Union[int, str]] = None,
        where: Optional[WhereType] = None,
        attributes: Optional[FieldListType] = None
    ):
        rs = []
        sql_l = [cls.__select__]
        args_l = []
        if attributes:
            if isinstance(attributes, list):
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
        return cls(**rs[0])

    async def save(self):
        args = tuple(map(self.get_value_or_default, self.__fields__))
        affected, lastrowid = await execute(self.__insert__, args)
        if affected != 1:
            logging.warning('failed to insert record: affected rows: %s' % affected)
        return lastrowid

    @classmethod
    async def findall(
            cls,
            where: Optional[WhereType] = None,
            attributes: Optional[FieldListType] = None,
            order_by: Optional[str] = None,
            limit: Optional[Tuple[int, int]] = None,
            **kwargs
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
            limit=limit,
            **kwargs
        )
        rs = await select(' '.join(sql_l), tuple(args_l))
        return [cls(**r) for r in rs]

    # where = {'name': 'luu', 'time': [(Op.Gt, '2020-09-01'), (Op.Lt, '2020-09-02')]}
    @classmethod
    def _make_sql_and_args(
            cls,
            sql_l: Optional[list] = None,
            args_l: Optional[list] = None,
            where: Optional[WhereType] = None,
            order_by: Union[str, list, tuple] = None,
            limit: Optional[Tuple[int, int]] = None,
            or_and: str = 'and',
            **kwargs
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




class User(Model):
    __table__ = 'user'

    id = IntegerField(primary_key=True)
    username = StringField(ddl='varchar(50)')
    email = StringField(ddl='varchar(50)')
    password = StringField(ddl='varchar(50)')
    avatar = StringField(ddl='varchar(50)')
    version = IntegerField()
    github_id = IntegerField()
    sex = IntegerField(ddl='tinyint')
    province = StringField(ddl='varchar(50)')
    city = StringField(ddl='varchar(50)')
    sign = StringField(ddl='varchar(255)')
    createdAt = FloatField(default=time.time)
    updatedAt = FloatField(default=time.time)
