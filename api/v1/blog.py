from core.coroweb import controller, get


@controller('/v1')
class BlogController:

    @get('/blog')
    async def blog(self):
        return 'blog'

