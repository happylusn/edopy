from core.coroweb import controller, get, post
from core.function import render
from model.user import UserModel


@controller('/v1/user')
class UserController:
    def __init__(self, request):
        self._request = request

    @get('/users')
    async def get_users(self, app):
        res = await UserModel.findall(raw=True)
        # user = UserModel(nickname='jkl', email='23@qq.com')
        # res = await user.save()
        # print(res)
        # return app.render('__base__.html')
        return render('__base__.html')

    @post('/register')
    async def register_user(self, *, account: str, type: int = 100):
        print('register_user....')
        print('account: %s  type: %s' % (account, type))
        return 'success'
