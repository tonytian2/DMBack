from flask import Blueprint, make_response, request, session, jsonify
from sqlalchemy import create_engine
import secrets
import os

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

        if self.connector != None and self.engine == None:
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

credentials_blueprint = Blueprint("credentials", __name__)


@credentials_blueprint.route("/v1/credentials", methods=["POST"])
def set_local_credentials():
    required_fields = [
        "local_username",
        "local_password",
        "local_url",
        "cloud_username",
        "cloud_password",
        "cloud_url",
    ]
    for field in required_fields:
        if field not in request.form:
            return make_response(f"Missing required field: {field}", 400)

    clear_session()
    # create session id if it doesn't exist
    if "session_id" not in session:
        session["session_id"] = secrets.token_hex(16)

    session_id = session["session_id"]
    session[session_id] = session_id
    local_username = request.form["local_username"]
    local_password = request.form["local_password"]
    local_url = request.form["local_url"]
    local_connector = "pymysql"
    cloud_username = request.form["cloud_username"]
    cloud_password = request.form["cloud_password"]
    cloud_url = request.form["cloud_url"]
    cloud_connector = "mysqlconnector"
    # create the DbConnection objects and add them to the dictionary
    localDbConnectionDict[session_id] = DbConnection(
        local_username, local_password, local_url, local_connector
    )
    cloudDbConnectionDict[session_id] = DbConnection(
        cloud_username, cloud_password, cloud_url, cloud_connector
    )
    localDbConnection = localDbConnectionDict[session_id]
    cloudDbConnection = cloudDbConnectionDict[session_id]
    # Establish the database connections
    source_engine = localDbConnection.get_engine()
    destination_engine = cloudDbConnection.get_engine()
    try:
        source_engine.connect()
        localDbConnection.isValid = True
        try:
            destination_engine.connect()
            cloudDbConnection.isValid = True
        except Exception:
            cloudDbConnection.reset()
            return make_response("Cloud credentials incorrect", 500)

        return make_response("OK", 200)
    except Exception:
        localDbConnection.reset()
        cloudDbConnection.reset()
        return make_response("Local credentials incorrect", 500)


@credentials_blueprint.route("/v1/session", methods=["DELETE"])
def reset():
    clear_session()
    delete_log()
    return make_response("OK", 200)


def clear_session():
    if "session_id" in session:
        session_id = session["session_id"]
        del localDbConnectionDict[session_id]
        del cloudDbConnectionDict[session_id]
        session.clear()
        
        
        
def delete_log():
    files = [f for f in os.listdir()]
    
    for file in files:
        if "log" in file:
            os.remove(file)
    
