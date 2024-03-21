from flask import Blueprint, jsonify, make_response, session, request
from sqlalchemy import MetaData, text
from sqlalchemy.orm import sessionmaker
from api.credentials import localDbConnectionDict, cloudDbConnectionDict, early_return_decorator
from api.migrate import globalVariables
from util.snapshots import get_latest_snapshot_data
import csv
import random

validate_blueprint = Blueprint("validate", __name__)

# validate completeness of the cloud against the snapshot csvs
@validate_blueprint.route("/v1/validation/completeness", methods=["GET", "POST"])
@early_return_decorator
def getValidateCompleteness():
    session_id = session["session_id"]
    cloud_db_connection = cloudDbConnectionDict[session_id]

    if cloud_db_connection.isValid:
        cloud_engine = cloud_db_connection.get_engine()

        # if was GET request
        table_list = []
        if request.method == "GET":
            table_list = globalVariables.getMigratedRows().keys()
        else:
            # must be POST request
            if "tables" in request.get_json() and isinstance(request.get_json()["tables"], list):
                table_list = request.get_json()["tables"]
            else:
                return make_response("Invalid request", 400)
            
        output = validate_snapshot_completeness(table_list, cloud_engine)

        json_response = jsonify(output)
        return make_response(json_response, 200)
    else:
        return make_response("Cloud credentials incorrect", 500)


# actual completeness validation logic of a given list of tables
def validate_snapshot_completeness(table_names, cloud_engine):

    Session = sessionmaker(bind=cloud_engine)
    cloud_session = Session()

    total_row_count_dict = {}
    cloud_row_count_dict = {}
    output = {}
    for table_name in table_names:
        snapshot_data = get_latest_snapshot_data(table_name)
        if snapshot_data is None:
            output[table_name] = {
                "error": "Snapshot data not found"
            }
        else:
            snapshot_row_count = len(get_latest_snapshot_data(table_name))
            total_row_count_dict[table_name] = snapshot_row_count
            cloud_row_count = cloud_session.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            cloud_row_count_dict[table_name] = cloud_row_count
            output[table_name] = {
                "source_row_count": snapshot_row_count,
                "destination_row_count": cloud_row_count,
                "completeness": 0 if snapshot_row_count == 0 else cloud_row_count / snapshot_row_count
            }

    return output

@validate_blueprint.route("/v1/validation/accuracy/<accuracy>", methods=["GET", "POST"])
@early_return_decorator
def getValidateAccuracy(accuracy):
    session_id = session["session_id"]
    cloud_db_connection = cloudDbConnectionDict[session_id]

    try:
        accuracy = float(accuracy)
    except ValueError:
        return make_response("Invalid accuracy value: must be a float", 400)

    if cloud_db_connection.isValid:
        cloud_engine = cloud_db_connection.get_engine()

        # if was GET request
        table_list = []
        if request.method == "GET":
            table_list = globalVariables.getMigratedRows().keys()
        else:
            # must be POST request
            if "tables" in request.get_json() and isinstance(request.get_json()["tables"], list):
                table_list = request.get_json()["tables"]
            else:
                return make_response("Invalid request", 400)
            
        output = validate_snapshot_accuracy(table_list, cloud_engine, min(1, max(0, accuracy)))

        json_response = jsonify(output)
        return make_response(json_response, 200)
    else:
        return make_response("Cloud credentials incorrect", 500)
    

# compare the local and the cloud database 
@validate_blueprint.route("/v1/validateLocalAndCloud", methods=["POST"])
@early_return_decorator
def validateLocalAndCloud():
    session_id = session["session_id"]
    localDbConnection = localDbConnectionDict[session_id]
    cloudDbConnection = cloudDbConnectionDict[session_id]
    if localDbConnection.isValid and cloudDbConnection.isValid:
        source_engine = localDbConnection.get_engine()
        cloud_engine = cloudDbConnection.get_engine()

        source_metadata = MetaData()
        source_metadata.reflect(source_engine)
        Session = sessionmaker(bind=source_engine)
        source_session = Session()

        output = {}
        for i in range(len(source_metadata.tables.keys())):
            table_name = list(source_metadata.tables.keys())[i]
            try:
                accuracy = get_accuracy_for_table(table_name, 1, source_metadata, source_session, cloud_engine)
                output[table_name] = {"accuracy": accuracy}
            except Exception as e:
                error_message = f"Error retrieving data: {str(e)}"
                output[table_name] = {"error": error_message,"accuracy": 0}
        
        source_session.close()
        return make_response(jsonify(output), 200)
    else:
        return make_response("Database credentials incorrect.", 500)

def validate_snapshot_accuracy(table_names, cloud_engine, percentage):
    Session = sessionmaker(bind=cloud_engine)
    cloud_session = Session()
    output = {}
    for table_name in table_names:
        snapshot_data = get_latest_snapshot_data(table_name)
        if snapshot_data is None:
            output[table_name] = {
                "error": "Snapshot data not found"
            }
        else:
            snapshot_data = get_latest_snapshot_data(table_name)
            snapshot_row_count = len(snapshot_data)
            snapshot_rows = snapshot_data[:max(1, int(snapshot_row_count * float(percentage)))]
            cloud_rows = cloud_session.execute(text(f"SELECT * from {table_name} LIMIT {max(1,int(snapshot_row_count * float(percentage)))}")).fetchall()

            # # FOR DEBUGGING
            # match_count = 0
            # for snapshot_row, cloud_row in zip(snapshot_rows, cloud_rows):
            #     print("ss:")
            #     print(snapshot_row)
            #     print("cl:")
            #     print(cloud_row)
            #     if snapshot_row == cloud_row:
            #         match_count += 1


            match_count = sum(snapshot_row == cloud_row for snapshot_row, cloud_row in zip(snapshot_rows, cloud_rows))

            accuracy = match_count / len(snapshot_rows) * 100
            output[table_name] = {
                "accuracy": accuracy
            }

    return output

def get_accuracy_for_table(tableName, percentage, source_metadata, source_session, cloud_engine):
    if tableName not in source_metadata.tables:
        raise ValueError(f"Table does not exist in local database.")
    source_table = source_metadata.tables[tableName]

    # Query the local database
    local_row_count = source_session.query(source_table).count()
    local_rows = (
        source_session.query(source_table)
        .limit(max(1, int(local_row_count * float(percentage))))
        .all()
    )

    # Query the cloud database
    with cloud_engine.connect() as con:
        result = con.execute(
            text(
                f"SELECT * from {tableName} LIMIT {max(1,int(local_row_count * float(percentage)))}"
            )
        )
        cloud_rows = result.fetchall()

    # Compare the accuracy
    match_count = sum(
        local_row == cloud_row
        for local_row, cloud_row in zip(local_rows, cloud_rows)
    )

    accuracy = match_count / len(local_rows) * 100

    return accuracy