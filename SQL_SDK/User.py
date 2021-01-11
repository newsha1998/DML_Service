class User:
    def __init__(self, user, pass1):
        self.username = user
        self.password = pass1
        if user is not None:
            self.dir = "./" + user
