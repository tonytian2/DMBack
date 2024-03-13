from flask import Blueprint, make_response, request
from sqlalchemy import create_engine

class DbConnection(object):
    def __init__(self):
        self.reset()

    def connection_string(self):
        return f"mysql+{self.connector}://{self.username}:{self.password}@{self.url}"
    
    def get_engine(self):
        if(self.connector != None and self.engine == None):
            self.engine = create_engine(self.connection_string())
        return self.engine
    
    def reset(self):
        self.username = None
        self.password = None
        self.url = None
        self.connector = None
        self.engine = None
        self.isValid = False

localDbConnection = DbConnection()
cloudDbConnection = DbConnection()

credentials_blueprint = Blueprint('credentials', __name__)

@credentials_blueprint.route('/api/set_credentials', methods=['POST'])
def set_local_credentials():
    reset()
    localDbConnection.username = request.form['local_username']
    localDbConnection.password = request.form['local_password']
    localDbConnection.url = request.form['local_url']
    localDbConnection.connector = "pymysql"
    cloudDbConnection.username = request.form['cloud_username']
    cloudDbConnection.password = request.form['cloud_password']
    cloudDbConnection.url = request.form['cloud_url']
    cloudDbConnection.connector = "mysqlconnector"
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
            return make_response("Cloud credentials incorrect", 511)
        
        return make_response("OK", 200)
    except Exception:
        localDbConnection.reset()
        cloudDbConnection.reset()
        return make_response("Local credentials incorrect", 511)

@credentials_blueprint.route('/api/reset', methods=['GET'])
def reset():
    localDbConnection.reset()
    cloudDbConnection.reset()
    return make_response("OK", 200)