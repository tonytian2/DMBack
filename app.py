from flask import Flask, jsonify, request, make_response
from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.orm import sessionmaker
import traceback


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







completed_row_count = 0
def resetCompleted_row_count():
    global completed_row_count
    completed_row_count = 0
    
# the completed row count should be according to the cloud rows not the local
@app.route('/api/updateCompletedRowCount/<table_name>', methods=['POST'])
def updateCompletedRowCount(table_name):
    global completed_row_count
    # completed_row_count = globalVariables.getMigratedRows()    # use this 

    if cloudDbConnection.isValid:
        source_engine = cloudDbConnection.get_engine()
        source_metadata = MetaData()
        source_metadata.reflect(source_engine)
        Session = sessionmaker(bind=source_engine)
        source_session = Session()

        try:
            # Check if the table exists
            if table_name not in source_metadata.tables:
                return make_response("Failed to locate the table", 404)

            # Get the row count for the specified table
            with source_session.begin():
                table = source_metadata.tables[table_name]
                row_count = source_session.query(table).count()
                # updated the completed row count
                completed_row_count += row_count

            # Return the row count as JSON response
            return jsonify(row_count=row_count, completed_row_count=completed_row_count)
        
        except Exception as e:
            traceback.print_exc()
            return make_response("Failed to connect to the cloud", 511)

    else:
        return make_response("Failed to connect to the cloud", 511)

# this one is for the progress bar
@app.route('/api/getValidateCompleteness', methods=['GET'])
def getValidateCompleteness():
    if localDbConnection.isValid:
        source_engine = create_engine(localDbConnection.connection_string())
        source_metadata = MetaData()
        source_metadata.reflect(source_engine)
        Session = sessionmaker(bind=source_engine)
        source_session = Session()

        total_row_count = 0

        for table_name in source_metadata.tables.keys():
            # Get row count
            query = text("SELECT COUNT(*) FROM " + localDbConnection.url.split('/')[-1] + "." + table_name)
            row_count = source_session.execute(query).scalar()

            total_row_count += row_count
            

        completeness = 0 if total_row_count == 0 else completed_row_count / total_row_count

        json_response = jsonify({
            'completeness': completeness,
            'total_row_count': total_row_count,
            'completed_row_count': completed_row_count
        })
        source_session.close()
        return json_response

    return 'Local connection not valid'

# this one is for checking the completeness for a specific table 
@app.route('/api/getValidateCompletenessbyTable/<tableName>', methods=['GET'])
def getValidateCompletenessbyTable(tableName):
    if localDbConnection.isValid and cloudDbConnection.isValid:
        local_engine = localDbConnection.get_engine()
        cloud_engine = cloudDbConnection.get_engine()

        local_metadata = MetaData()
        local_metadata.reflect(local_engine)

        if tableName not in local_metadata.tables:
            return f"Table '{tableName}' does not exist in the local database."

        Session = sessionmaker(bind=local_engine)
        local_session = Session()
        cloud_session = Session(bind=cloud_engine)

        local_table = local_metadata.tables[tableName]

        # Get row count for the specified table in the local database
        local_row_count = local_session.query(local_table).count()

        # Get row count for the specified table in the cloud database
        cloud_row_count = cloud_session.query(local_table).count()

        if local_row_count == cloud_row_count:
            return "The row count matches between the local and cloud databases."
        else:
            return "The row count does not match between the local and cloud databases."

    return "Local or cloud connection not valid"




# for validating accuracy 
# get cloud connection from both local and cloud, compare each table 
@app.route('/api/getValidationAccueacy/<tableName>/<percentage>', methods=['GET'])
def getValidationAccuracy(tableName, percentage):
    source_engine = localDbConnection.get_engine()
    cloud_engine = cloudDbConnection.get_engine()

    source_metadata = MetaData()
    source_metadata.reflect(source_engine)
    Session = sessionmaker(bind=source_engine)
    source_session = Session()

    try:
        source_table = source_metadata.tables[tableName]

        # Query the local database
        local_row_count = source_session.query(source_table).count()
        local_rows = source_session.query(source_table).limit(int(local_row_count * float(percentage))).all()

        # Query the cloud database
        with cloud_engine.connect() as con:
            result = con.execute(text(f"SELECT * from {tableName} LIMIT {int(local_row_count * float(percentage))}"))
            cloud_rows = result.fetchall()

        # Compare the accuracy
        match_count = sum(local_row == cloud_row for local_row, cloud_row in zip(local_rows, cloud_rows))

        accuracy = match_count / len(local_rows) * 100

        return jsonify({"accuracy": accuracy})

    except Exception as e:
        error_message = f"Error retrieving data: {str(e)}"
        response = make_response(jsonify({"error": error_message}), 500)
        return response

    finally:
        source_session.close()

