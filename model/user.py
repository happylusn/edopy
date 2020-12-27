from core.orm2 import Model, IntegerField, StringField, table


@table(timestamps=True)
class UserModel(Model):
    __table__ = 'user'

    id = IntegerField(primary_key=True)
    nickname = StringField(ddl='varchar(50)')
    email = StringField(ddl='varchar(128)')
    password = StringField(ddl='varchar(255)')
    openid = StringField(ddl='varchar(64)')
