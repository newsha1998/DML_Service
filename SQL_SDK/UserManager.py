import os
import sqlite3


class UserManager:

    def __init__(self):
        if not os.path.exists('users.db'):
            self.conn = sqlite3.connect('users.db')
            self.conn.execute("CREATE TABLE Users \
                     (Username varchar(255) PRIMARY KEY     NOT NULL, \
                     Password            varchar(255)     NOT NULL);")
        else:
            self.conn = sqlite3.connect('users.db')
        print("Opened database successfully")
        # self.conn.execute("INSERT INTO rrr (Username, Password)\
        #                   VALUES ('a', 'b');")

    def add_user(self, user, pas):
        self.conn.execute("INSERT INTO Users (Username, Password) \
      VALUES (?, ?)", [user, pas])

        self.conn.commit()

    def check_authentication(self, user, pas):
        cur = self.conn.execute("SELECT * FROM users WHERE username= ? and password= ?",
                                (user, pas))
        found = cur.fetchone()
        if found:
            return True
        else:
            return False

    def close(self):
        self.conn.close()
