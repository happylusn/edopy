from core.coroweb import controller, get, post


@controller('/v1/user')
class UserController:
    def __init__(self, request):
        self._request = request

    @get('/users')
    async def get_users(self, app):
        print(app.render)
        return app.render('__base__.html')

    @post('/register')
    async def register_user(self, *, account: str, type: int = 100):
        print('register_user....')
        print('account: %s  type: %s' % (account, type))
        return 'success'
