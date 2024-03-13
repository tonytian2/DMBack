from flask import Blueprint, jsonify, request, make_response, session
from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.orm import sessionmaker
from api.credentials import localDbConnectionDict, cloudDbConnectionDict
from api.migrate import globalVariables
import traceback

validate_blueprint = Blueprint('validate', __name__)
    
# the completed row count should be according to the cloud rows not the local
@validate_blueprint.route('/api/updateCompletedRowCount/<table_name>', methods=['POST'])
def updateCompletedRowCount(table_name):
    if 'session_id' not in session:
        return make_response("No connection defined in current session, define session credentials first", 428)
    
    session_id = session['session_id']
    cloudDbConnection = cloudDbConnectionDict[session_id]
    completed_row_count = globalVariables.getMigratedRows()    # use this 

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
            return make_response(jsonify(row_count=row_count, completed_row_count=completed_row_count), 200)
        
        except Exception as e:
            traceback.print_exc()
            return make_response("Failed to connect to the cloud", 511)

    else:
        return make_response("Failed to connect to the cloud", 511)

# this one is for the progress bar
@validate_blueprint.route('/api/getValidateCompleteness', methods=['GET'])
def getValidateCompleteness():
    if 'session_id' not in session:
        return make_response("No connection defined in current session, define session credentials first", 428)
    
    session_id = session['session_id']
    localDbConnection = localDbConnectionDict[session_id]
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
            
        completed_row_count = globalVariables.getMigratedRows()
        completeness = 0 if total_row_count == 0 else completed_row_count / total_row_count

        json_response = jsonify({
            'completeness': completeness,
            'total_row_count': total_row_count,
            'completed_row_count': completed_row_count
        })
        source_session.close()
        return make_response(json_response, 200)

    return make_response('Local connection not valid', 511)

# this one is for checking the completeness for a specific table 
@validate_blueprint.route('/api/getValidateCompletenessbyTable/<tableName>', methods=['GET'])
def getValidateCompletenessbyTable(tableName):
    if 'session_id' not in session:
        return make_response("No connection defined in current session, define session credentials first", 428)
    
    session_id = session['session_id']
    localDbConnection = localDbConnectionDict[session_id]
    cloudDbConnection = cloudDbConnectionDict[session_id]
    if localDbConnection.isValid and cloudDbConnection.isValid:
        local_engine = localDbConnection.get_engine()
        cloud_engine = cloudDbConnection.get_engine()

        local_metadata = MetaData()
        local_metadata.reflect(local_engine)

        if tableName not in local_metadata.tables:
            return make_response(f"Table '{tableName}' does not exist in the local database.", 404)

        Session = sessionmaker(bind=local_engine)
        local_session = Session()
        cloud_session = Session(bind=cloud_engine)

        local_table = local_metadata.tables[tableName]

        # Get row count for the specified table in the local database
        local_row_count = local_session.query(local_table).count()

        # Get row count for the specified table in the cloud database
        cloud_row_count = cloud_session.query(local_table).count()

        if local_row_count == cloud_row_count:
            return make_response("The row count matches between the local and cloud databases.", 200)
        else:
            return make_response("The row count does not match between the local and cloud databases.", 500)

    return make_response("Local or cloud connection not valid", 511)




# for validating accuracy 
# get cloud connection from both local and cloud, compare each table 
@validate_blueprint.route('/api/getValidationAccuracy/<tableName>/<percentage>', methods=['GET'])
def getValidationAccuracy(tableName, percentage):
    if 'session_id' not in session:
        return make_response("No connection defined in current session, define session credentials first", 428)
    
    session_id = session['session_id']
    localDbConnection = localDbConnectionDict[session_id]
    cloudDbConnection = cloudDbConnectionDict[session_id]
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
        local_rows = source_session.query(source_table).limit(max(1,int(local_row_count * float(percentage)))).all()

        # Query the cloud database
        with cloud_engine.connect() as con:
            result = con.execute(text(f"SELECT * from {tableName} LIMIT {max(1,int(local_row_count * float(percentage)))}"))
            cloud_rows = result.fetchall()

        # Compare the accuracy
        match_count = sum(local_row == cloud_row for local_row, cloud_row in zip(local_rows, cloud_rows))

        accuracy = match_count / len(local_rows) * 100

        return make_response(jsonify({"accuracy": accuracy}), 200)

    except Exception as e:
        error_message = f"Error retrieving data: {str(e)}"
        response = make_response(jsonify({"error": error_message}), 500)
        return response

    finally:
        source_session.close()

