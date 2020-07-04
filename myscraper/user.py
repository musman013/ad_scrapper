class User(object):
    def __init__(self, username, password):
        self.username = username
        self.password = password

    def __str__(self):
        return "User(id='%s')" % self.id