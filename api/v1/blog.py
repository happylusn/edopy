from core.coroweb import controller, get


@controller('/')
class BlogController:

    @get('/blog')
    def blog(self):
        return 'blog'

