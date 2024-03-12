from flask import Flask, jsonify, request, make_response
from sqlalchemy import create_engine, MetaData, text, inspect
from sqlalchemy.orm import sessionmaker

app = Flask(__name__)

class DbConnection(object):
    def __init__(self):
        return
    def __init__(self, username, password, url):
        self.username = username
        self.password = password
        self.url = url
        self.isValid = False

    def connection_string(self):
        return f"mysql+pymysql://{self.username}:{self.password}@{self.url}"


localDbConnection = DbConnection("username","password","url")

@app.route('/api/set_local_credentials', methods=['POST'])
def set_local_credentials():
    localDbConnection.username=request.form['username']
    localDbConnection.password=request.form['password']
    localDbConnection.url=request.form['url']
    # Establish the database connection
    source_engine = create_engine(localDbConnection.connection_string())
    try:
        source_engine.connect()
        localDbConnection.isValid = True
        return make_response("OK", 200)
    except:
        return make_response("Connection not valid", 511)

@app.route('/api/get_table_info', methods=['GET'])
def get_table_info():
    if(localDbConnection.isValid):

        source_engine = create_engine(localDbConnection.connection_string())
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