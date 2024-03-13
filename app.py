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


# # setting up the Azure connection with the input of username, password, hostname, and database name
# @app.route('/api/set_azure_credentials', methods=['POST'])
# # engine = create_engine(f'mysql+mysqlconnector://fdm:qwe123!!@data-migration.mysql.database.azure.com/destination')
# def set_azure_credentials():
#     username = request.form['username']
#     password = request.form['password']
#     hostname = request.form['hostname']
#     database_name = request.form['database_name']
    
#     connection_string = 'mysql+mysqlconnector://' + username + ':' + password + '@' + hostname + '/' + database_name
    
#     # Establish the database connection
#     source_engine = create_engine(connection_string)
#     try:
#         source_engine.connect()
#         localDbConnection.username = username
#         localDbConnection.password = password
#         localDbConnection.url = hostname
#         localDbConnection.schema = database_name
#         localDbConnection.isValid = True
#         return 'Ok'
#     except Exception as e:
#         traceback.print_exc()
#         return 'Not Ok: ' + str(e)
    

# my job 
    # validate completeness
    # optional param: list of tables
    # send update per table

    # validate accuracy 
    # param: percentage 
    # optional param: list of tables
    # send update per table

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


# for validating accuracy 
# get cloud connection from both local and cloud, compare each table 
@app.route('/api/getValidationAccueacy/<percentage>', methods=['GET'])
def getValidationAccueacy(percentage):
    if(localDbConnection.isValid and cloudDbConnection.isValid):
        # check for each tables in local and cloud 
        local_engine = localDbConnection.get_engine()
        cloud_engine = cloudDbConnection.get_engine()

        local_metadata = MetaData()
        cloud_metadata = MetaData()

        local_metadata.reflect(local_engine)
        cloud_metadata.reflect(cloud_engine)

        local_tables = local_metadata.tables
        cloud_tables = cloud_metadata.tables

        validation_results = {}

        for table_name in local_tables:
            if table_name in cloud_tables:
                local_table = local_tables[table_name]
                cloud_table = cloud_tables[table_name]

                local_row_count = local_engine.execute(local_table.count()).scalar()
                cloud_row_count = cloud_engine.execute(cloud_table.count()).scalar()

                if local_row_count == cloud_row_count:
                    match_count = int(local_row_count * float(percentage))

                    local_rows = local_engine.execute(local_table.select().limit(match_count)).fetchall()
                    cloud_rows = cloud_engine.execute(cloud_table.select().limit(match_count)).fetchall()

                    match_count = sum(local_row == cloud_row for local_row, cloud_row in zip(local_rows, cloud_rows))

                    validation_results[table_name] = match_count

        return jsonify(validation_results)

    else:
        return jsonify(error='One or both database connections are not valid')
        

    # total_row_local = 

    # return 0

