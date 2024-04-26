from flask import Blueprint, jsonify, make_response, request, session
from sqlalchemy import MetaData, text
from sqlalchemy.orm import sessionmaker

from api.migrate import globalVariables
from util.globals import (
    cloudDbConnectionDict,
    early_return_decorator,
    localDbConnectionDict,
)
from util.snapshots import get_latest_snapshot_data

validate_blueprint = Blueprint("validate", __name__)


# validate completeness of the cloud against the snapshot csvs
@validate_blueprint.route("/v1/firstValidation/completeness", methods=["GET", "POST"])
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
            if "tables" in request.get_json() and isinstance(
                request.get_json()["tables"], list
            ):
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
            output[table_name] = {"error": "Snapshot data not found"}
        else:
            snapshot_row_count = len(get_latest_snapshot_data(table_name))
            total_row_count_dict[table_name] = snapshot_row_count
            cloud_row_count = cloud_session.execute(
                text(f"SELECT COUNT(*) FROM {table_name}")
            ).scalar()
            cloud_row_count_dict[table_name] = cloud_row_count
            output[table_name] = {
                "source_row_count": snapshot_row_count,
                "destination_row_count": cloud_row_count,
                "completeness": 0
                if snapshot_row_count == 0
                else cloud_row_count / snapshot_row_count,
            }

    return output


@validate_blueprint.route(
    "/v1/firstValidation/accuracy/<accuracy>", methods=["GET", "POST"]
)
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
            if "tables" in request.get_json() and isinstance(
                request.get_json()["tables"], list
            ):
                table_list = request.get_json()["tables"]
            else:
                return make_response("Invalid request", 400)

        output = validate_snapshot_accuracy(
            table_list, cloud_engine, min(1, max(0, accuracy))
        )

        json_response = jsonify(output)
        return make_response(json_response, 200)
    else:
        return make_response("Cloud credentials incorrect", 500)


def validate_snapshot_accuracy(table_names, cloud_engine, percentage):
    Session = sessionmaker(bind=cloud_engine)
    cloud_session = Session()
    output = {}
    for table_name in table_names:
        snapshot_data = get_latest_snapshot_data(table_name)
        if snapshot_data is None:
            output[table_name] = {"error": "Snapshot data not found"}
        else:
            snapshot_data = get_latest_snapshot_data(table_name)
            snapshot_row_count = len(snapshot_data)
            snapshot_rows = snapshot_data[
                : max(1, int(snapshot_row_count * float(percentage)))
            ]
            cloud_rows = cloud_session.execute(
                text(
                    f"SELECT * from {table_name} LIMIT {max(1,int(snapshot_row_count * float(percentage)))}"
                )
            ).fetchall()

            # # FOR DEBUGGING
            # match_count = 0
            # for snapshot_row, cloud_row in zip(snapshot_rows, cloud_rows):
            #     print("ss:")
            #     print(snapshot_row)
            #     print("cl:")
            #     print(cloud_row)
            #     if snapshot_row == cloud_row:
            #         match_count += 1

            match_count = sum(
                snapshot_row == cloud_row
                for snapshot_row, cloud_row in zip(snapshot_rows, cloud_rows)
            )

            accuracy = match_count / len(snapshot_rows) * 100
            output[table_name] = {"accuracy": accuracy}

    return output


# validate completeness of specific tables
@validate_blueprint.route("/v1/secondValidation/completeness", methods=["POST"])
@early_return_decorator
def getSecondValidateCompletenessbyTable():
    session_id = session["session_id"]
    data = request.get_json()
    if "tables" in data and isinstance(data["tables"], list):
        table_names = data["tables"]
        localDbConnection = localDbConnectionDict[session_id]
        cloudDbConnection = cloudDbConnectionDict[session_id]
        if localDbConnection.isValid and cloudDbConnection.isValid:
            local_engine = localDbConnection.get_engine()
            cloud_engine = cloudDbConnection.get_engine()

            local_metadata = MetaData()
            local_metadata.reflect(local_engine)

            output = {}
            for i in range(len(table_names)):
                tableName = table_names[i]

                if tableName not in local_metadata.tables:
                    output[tableName] = {
                        "error": "Table does not exist in the local database."
                    }
                    continue

                Session = sessionmaker(bind=local_engine)
                local_session = Session()
                cloud_session = Session(bind=cloud_engine)

                local_table = local_metadata.tables[tableName]

                # Get row count for the specified table in the local database
                local_row_count = local_session.query(local_table).count()

                # Get row count for the specified table in the cloud database
                cloud_row_count = cloud_session.query(local_table).count()
                data = {}
                data["source_row_count"] = local_row_count
                data["destination_row_count"] = cloud_row_count
                data["completeness"] = (
                    0 if local_row_count == 0 else cloud_row_count / local_row_count
                )
                output[tableName] = data

            return make_response(
                jsonify(output), 200 if local_row_count == cloud_row_count else 500
            )

        return make_response("Local or cloud connection not valid", 500)
    else:
        return make_response("Invalid request.", 400)


# validate the local history table and the cloud --> assume it is 100% so does not need for input
@validate_blueprint.route("/v1/secondValidation/accuracy", methods=["POST"])
def getSecondValidateAccuracy():
    table_names = request.json.get("tables")
    response_messages = []

    if table_names:
        session_id = session["session_id"]
        localDbConnection = localDbConnectionDict[session_id]
        cloudDbConnection = cloudDbConnectionDict[session_id]

        if localDbConnection.isValid and cloudDbConnection.isValid:
            local_engine = localDbConnection.get_engine()
            cloud_engine = cloudDbConnection.get_engine()

            local_metadata = MetaData()
            local_metadata.reflect(local_engine)

            for table_name in table_names:
                primary_key = get_primary_key_column(cloud_engine, table_name)
                print(f"primary_key is {primary_key}")

                # Check if history table exists
                history_table_name = table_name + "_zkqygjhistory"
                if history_table_name not in local_metadata.tables:
                    response_messages.append(
                        f"History table does not exist for table: {table_name}"
                    )
                    continue

                # Get the latest change from local history table
                latest_change = get_latest_change_from_history(
                    local_engine, history_table_name, primary_key
                )
                print(f"latest_change is {latest_change}")

                total_row = len(latest_change)
                matched_row = 0

                # Next step is to use a loop for each history and check for update, insert, delete
                for row in latest_change:
                    action = row[
                        0
                    ]  # Get the action type (either insert, delete, update)
                    primary_key_values = row[3 : 3 + len(primary_key)]
                    where_clause = " AND ".join(
                        [
                            f"{column} = {value}"
                            for column, value in zip(primary_key, primary_key_values)
                        ]
                    )

                    # Construct the final query
                    query = f"""SELECT EXISTS (
                                    SELECT 1 FROM {table_name} 
                                    WHERE {where_clause}
                                    ) AS id_exist;"""

                    with cloud_engine.connect() as con:
                        r = con.execute(text(query))
                        result = r.fetchall()

                    if action == "insert":
                        if result[0] == (1,):
                            # The row is in the cloud
                            matched_row += 1
                        else:
                            # Just do nothing here
                            matched_row += 0

                    elif action == "update":
                        if result[0] == (0,):
                            matched_row += 0
                        else:
                            query = (
                                f"""SELECT * FROM {table_name} WHERE {where_clause}"""
                            )
                            # Not only need to check if it's in the cloud but also have to check the data
                            with cloud_engine.connect() as con:
                                # Execute the query using the provided engine
                                r2 = con.execute(text(query))
                                # Fetch the results and return them
                                cloud_data = r2.fetchall()

                            local_data = row[3:]
                            if cloud_data == local_data:
                                matched_row += 1
                            else:
                                matched_row += 0

                    elif action == "delete":
                        if result[0] == (0,):
                            # The row does not exist in the cloud
                            matched_row += 1
                        else:
                            matched_row += 0

                    else:
                        # Handle unrecognized action
                        print("Unrecognized action:", action)

                if matched_row == total_row:
                    # Return success message for each table
                    response_messages.append(
                        f"Second Validation success for table: {table_name}"
                    )
                else:
                    # Return failure message for each table
                    response_messages.append(
                        f"Second Validation not matched for table: {table_name}"
                    )

            # Return the combined response messages
            return make_response("\n".join(response_messages), 200)

        else:
            return make_response("Database credentials incorrect.", 500)
    else:
        return make_response("Table names not provided in the request body.", 400)


# get all the lastest chages for all the row in the history table
def get_latest_change_from_history(engine, history_table_name, primary_key_column):
    # Construct the comma-separated primary key column names
    primary_key_columns_str = ", ".join(primary_key_column)
    # Construct the IN subquery to get the latest revisions
    in_subquery = f"""
        SELECT {primary_key_columns_str}, MAX(revision_zkqygj)
        FROM {history_table_name}
        GROUP BY {primary_key_columns_str}
    """

    # Construct the main query using the primary key columns
    query = f"""
        SELECT * FROM {history_table_name}
        WHERE ({primary_key_columns_str}, revision_zkqygj) IN ({in_subquery})
    """
    with engine.connect() as con:
        # Execute the query using the provided engine
        result = con.execute(text(query))

        # Fetch the results and return them
        rows = result.fetchall()

    return rows


def get_primary_key_column(
    engine, table_name
):  # may cause issue since it only work for single primary key
    query = f"SHOW KEYS FROM {table_name} WHERE Key_name = 'PRIMARY'"
    with engine.connect() as con:
        result = con.execute(text(query))
        # primary_key = result.fetchone()
        primary_key = result.fetchall()

    if primary_key:
        # primary_key_column = primary_key[4]
        primary_key_column = [pk[4] for pk in primary_key]
        return primary_key_column
    else:
        raise ValueError(f"No primary key found for table: {table_name}")


def get_accuracy_for_table(
    tableName, percentage, source_metadata, source_session, cloud_engine
):
    if tableName not in source_metadata.tables:
        raise ValueError("Table does not exist in local database.")
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
        local_row == cloud_row for local_row, cloud_row in zip(local_rows, cloud_rows)
    )

    accuracy = match_count / len(local_rows) * 100

    return accuracy
