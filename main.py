from flask import Flask, jsonify, request, make_response
from sqlalchemy import create_engine, MetaData, text, inspect
from sqlalchemy.orm import sessionmaker

app = Flask(__name__)

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

@app.route('/api/set_credentials', methods=['POST'])
def set_local_credentials():
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
            cloudDbConnection.isValid = False
            return make_response("Cloud credentials incorrect", 511)
        
        return make_response("OK", 200)
    except Exception:
        localDbConnection.isValid = False
        cloudDbConnection.isValid = False
        return make_response("Local credentials incorrect", 511)
    
@app.route('/api/get_table_info', methods=['GET'])
def get_table_info():
    if(localDbConnection.isValid):

        source_engine = localDbConnection.get_engine()
        source_metadata = MetaData()
        source_metadata.reflect(source_engine)
        Session = sessionmaker(bind=source_engine)
        source_session = Session()
        columnNames = {}

        for table_name in source_metadata.tables.keys():

            # Get row count
            query = text("SELECT COUNT(*) FROM " + localDbConnection.url.split('/')[-1] + "." + table_name)
            rowCount = source_session.execute(query).scalar()
            
            # Get column names
            column_names = source_metadata.tables[table_name].columns.keys()
            
            columnNames[table_name] = {'rows':rowCount,'columns':column_names}
        
        json_response = jsonify(columnNames)
        source_session.close()
        return json_response
    return 'Local connection not valid'

@app.route('/api/reset', methods=['GET'])
def reset():
    localDbConnection.reset()
    cloudDbConnection.reset()
    return make_response("OK", 200)