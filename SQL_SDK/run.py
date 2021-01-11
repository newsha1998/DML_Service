import os

import sdk
from User import User
from UserManager import UserManager


def run_command(dir, command):
    if not os.path.exists(dir):
        os.mkdir(dir)
    if command == 'sql':
        cmd = input()
        sdk.run_sql(dir, cmd)
    if command == 'show':
        pass
    if command == 'create':
        pass
    if command == 'description':
        pass
    if command == 'import':
        path = input()
        sdk.importt(dir, path)
    pass


if __name__ == "__main__":
    um = UserManager()
    print("Print your username (if you want to create new account enter \'create\':")
    command = input()
    auth = False
    user = User(None, None)
    while not auth:
        if command == "create":
            print("Enter Username:")
            user = input()
            print("Enter Password:")
            pas = input()
            um.add_user(user, pas)
            user = User(user, pas)
            auth = True
        else:
            user = command
            print("Enter Password:")
            pas = input()
            auth = um.check_authentication(user, pas)
            user = User(user, pas)
    print('Welcome', user.username)

    command = input()
    while command != 'q':
        run_command(user.dir, command)
        command = input()
    um.close()
