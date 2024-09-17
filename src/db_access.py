# -*- coding: utf-8 -*-

import mysql.connector
from mysql.connector import pooling

class DBconnector:
    """
    Provides methods to access and manipulate local RunoffDB instance
    """

    def __init__(self):
        # server to connect to
        self.server = "localhost"
        # database name to use
        self.db_name = "runoffdb"
        # username for the db access
        self.username = "root"
        # password of the user
        self.pwd = "vochechule"

        self.pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="rdb_pool",
            pool_size=5,  # Set pool size as needed
            host=self.server,
            database=self.db_name,
            user=self.username,
            password=self.pwd
        )

    def connect(self, pool_name = "rdb"):
        try:
            db_connection = mysql.connector.connect(
                host = self.server,
                user = self.username,
                password = self.pwd,
                database = self.db_name,
                pool_name = pool_name
            )
        except mysql.connector.errors.InterfaceError as e:
            print("The RunoffDB database server is not accessible:")
            print(e)
            return False
        except mysql.connector.errors.ProgrammingError as e:
            print("Provided user credentials or database name seem to be invalid:")
            print(e)
            return False
        else:
            # print ("successfully connected to RunoffDB")
            return db_connection

