import functools
from flask import make_response, session
from sqlalchemy import create_engine

random_string = "zkqygj"
history_suffix = random_string + "history"


class GlobalVariables:
    def __init__(self):
        self.migratedRows = {}
        self.totalRows = 0
        self.migratedTables = []

    def getMigratedRows(self):
        return self.migratedRows

    def setMigratedRows(self, tableName, count):
        self.migratedRows[tableName] = count

    def getMigratedTables(self):
        return self.migratedTables

    def setMigratedTables(self, t):
        self.migratedTables.append(t)


globalVariables = GlobalVariables()


class DbConnection(object):
    """
    A class used to represent a connection to a database

    Attributes
    ----------
    username : str
        the username for the database
    password : str
        the password for the given username for the database
    url : str
        the url which the database is hosted on. Formatted as {hostname}/{schema}
    connector : str
        the type of connector to use when connecting to the database, e.g. pymysql, mysqlconnector
    engine : sqlalchemy.Engine
        the engine for accessing the database
    isValid : bool
        whether a connection to the database can be established with the given parameters

    Methods
    -------
    connection_string()
        Returns the connection string to be given to the engine

    get_engine()
        Returns the engine that connects to the database

    reset()
        Resets all the parameter values to their default (None, isValid is set to False)
    """

    def __init__(self, username, password, url, connector):
        self.reset()
        self.username = username
        self.password = password
        self.url = url
        self.connector = connector

    def connection_string(self):
        return f"mysql+{self.connector}://{self.username}:{self.password}@{self.url}"

    def get_engine(self):
        """
        Returns the engine that connects to the database. If an engine does not exist, it creates one
        """

        if self.connector is not None and self.engine is None:
            self.engine = create_engine(self.connection_string())
        return self.engine

    def reset(self):
        self.username = None
        self.password = None
        self.url = None
        self.connector = None
        self.engine = None
        self.isValid = False


localDbConnectionDict = {}
cloudDbConnectionDict = {}


def early_return_decorator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if "session_id" not in session:
            return make_response(
                "No connection defined in current session, define session credentials first",
                401,
            )
        return func(*args, **kwargs)

    return wrapper
