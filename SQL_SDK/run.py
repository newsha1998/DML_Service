import os

import sdk
from User import User
from UserManager import UserManager



def run_command(dir, command,df):
    if not os.path.exists(dir):
        os.mkdir(dir)
    if command == 'Sql':
        cmd = input("wriet sql command:")
        sdk.run_sql(cmd,df)
    if command == 'show':
        pass
    if command == 'create':
        pass
      
    if command == 'description':
        pass
    if command == 'Import':
        cmdpath = input("Path file:")
        cmdformat=input("file format(csv/json):")
        cmdheader=input("file has a header column (true/false):")
        cmdname=input("tabele name:")
        df= sdk.importt(cmdpath,cmdname,cmdformat,cmdheader)
        return df
    if command=='ShowColumn':
        sdk.ShowColumn(df)
    if command=='ChangeColumnTpye':
        cmdColumnName=input('Enter column name:')
        cmdColumntype=input('Enter new column type:')
        df=sdk.ChangeColumntype(df,cmdColumnName,cmdColumntype)
        return df
    if command=='ChangeColumnName':
        cmdColumnName=input('Enter old column name:')
        cmdColumnNName=input('Enter new column name:')
        df=sdk.ChangeColumnName(df,cmdColumnName,cmdColumnNName)
        return df
    if command=='AddColumn':
        cmddfName=input('Enter table name:')
        cmdColumnName=input('Enter column name:')
        cmdColumnType=input('Enter column type:')
        df=sdk.AddColumn(df,cmddfName,cmdColumnName,cmdColumnType)
        return df
    if command=='DropNull':
        df=sdk.dropNull(df)
        return df
    if command=='Save':
        cmd=input('Enter path and output file neme:')
        sdk.Save(df,cmd)


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

    print("enter a command(Import/Sql/ShowColumn/ChangeColumnTpye/ChangeColumnName/AddColumn/DropNull/Save/q")
    command = input()
    df =None
    while command != 'q':
        df=run_command(user.dir, command,df)
        print("enter a command(Import/Sql/ShowColumn/ChangeColumnTpye/ChangeColumnName/AddColumn/DropNull/Save/q")
        command = input()
    um.close()
