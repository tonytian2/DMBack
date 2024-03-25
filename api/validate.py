from flask import Blueprint, jsonify, make_response, session, request
from sqlalchemy import MetaData, text, select, func, Table, tuple_
from sqlalchemy.orm import sessionmaker
from api.credentials import localDbConnectionDict, cloudDbConnectionDict, early_return_decorator
from api.migrate import globalVariables
import csv
import random

validate_blueprint = Blueprint("validate", __name__)

# validate the completness of the csv and also the cloud, should pass the table name and also the url for the csv file in postman 
@validate_blueprint.route("/v1/firstValidation/completeness", methods=["POST"])
@early_return_decorator
def getFirstValidateCompletenessbyTable():

    session_id = session["session_id"]
    data = request.get_json()
    if "table" in data and isinstance(data["table"], str):      # assume only a table in the csv file 
        table_name = data["table"]
        local_csv_path = data.get("csv_file")
        cloud_db_connection = cloudDbConnectionDict[session_id]()

        if local_csv_path and cloud_db_connection.isValid:
            cloud_engine = cloud_db_connection.get_engine()

            Session = sessionmaker(bind=cloud_engine)
            cloud_session = Session()

            # Read row count from the CSV file
            with open(local_csv_path, 'r') as csvfile:
                csv_reader = csv.reader(csvfile)
                header = next(csv_reader)  # Skip the header row
                row = next(csv_reader)  # Read the first row
                csv_table_name = row[0]
                row_count = int(row[1])

            if csv_table_name != table_name:
                return make_response("Table name in the CSV file does not match the requested table.", 400)

            # Get destination row count from the cloud database
            cloud_row_count = cloud_session.execute(f"SELECT COUNT(*) FROM {table_name}").scalar()

            data = {
                "source_row_count": row_count,
                "destination_row_count": cloud_row_count,
                "completeness": 0 if row_count == 0 else cloud_row_count / row_count
            }

            return make_response(jsonify(data), 200)

        return make_response("CSV file or cloud connection not valid", 500)
    else:
        return make_response("Invalid request.", 400)
    

@validate_blueprint.route("/v1/firstValidation/accuracy/<float:accuracy>", methods=["POST"])
@early_return_decorator
def getFirstValidateAccuracy(accuracy):

    session_id = session["session_id"]
    data = request.get_json()
    if "table" in data and isinstance(data["table"], str):
        table_name = data["table"]
        local_csv_path = data.get("csv_file")
        cloud_db_connection = cloudDbConnectionDict[session_id]()

        if local_csv_path and cloud_db_connection.isValid:
            cloud_engine = cloud_db_connection.get_engine()

            Session = sessionmaker(bind=cloud_engine)
            cloud_session = Session()

            # Read the local CSV file
            with open(local_csv_path, 'r') as csvfile:
                csv_reader = csv.reader(csvfile)
                header = next(csv_reader)  # Skip the header row
                rows = list(csv_reader)  # Read all remaining rows

            # Calculate the number of rows to compare based on accuracy
            num_rows_to_compare = int(len(rows) * accuracy)
            num_rows_to_compare = min(num_rows_to_compare, len(rows))  # Ensure it does not exceed the total number of rows

            # Randomly select rows to compare
            random_rows = random.sample(rows, num_rows_to_compare)

            accuracy_results = []

            for row in random_rows:
                csv_row = row[1:]  # Exclude the first column (table_name) in the CSV row
                csv_row = [int(value) for value in csv_row]  # Convert values to integers if needed

                # Query the cloud database for the same row
                cloud_row = cloud_session.execute(f"SELECT * FROM {table_name} WHERE id = {csv_row[0]}").fetchone()

                if cloud_row:
                    cloud_row_values = list(cloud_row[1:])  # Exclude the first column (id) in the cloud row
                    cloud_row_values = [int(value) for value in cloud_row_values]  # Convert values to integers if needed

                    # Compare the values
                    row_accuracy = sum(1 for csv_value, cloud_value in zip(csv_row, cloud_row_values) if csv_value == cloud_value) / len(csv_row)
                    accuracy_results.append(row_accuracy)

            # Calculate the overall accuracy
            overall_accuracy = sum(accuracy_results) / len(accuracy_results) if accuracy_results else 0

            data = {
                "num_rows_to_compare": num_rows_to_compare,
                "overall_accuracy": overall_accuracy,
                "row_accuracy": accuracy_results
            }

            cloud_session.close()

            return make_response(jsonify(data), 200)

        return make_response("CSV file or cloud connection not valid", 500)
    else:
        return make_response("Invalid request.", 400)


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
                    output[tableName] = {"error": "Table does not exist in the local database."}
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
                data["completeness"] = 0 if local_row_count == 0 else cloud_row_count / local_row_count
                output[tableName] = data

            return make_response(jsonify(output), 200 if local_row_count == cloud_row_count else 500)

        return make_response("Local or cloud connection not valid", 500)
    else:
        return make_response("Invalid request.", 400)
    

# validate the local history table and the cloud 
@validate_blueprint.route("/v1/secondValidation/accuracy", methods=["POST"])
@early_return_decorator
def getSecondValidateAccuracy():
    table_name = request.json.get("table_name")
    
    if table_name:
        session_id = session["session_id"]
        localDbConnection = localDbConnectionDict[session_id]
        cloudDbConnection = cloudDbConnectionDict[session_id]

        if localDbConnection.isValid and cloudDbConnection.isValid:
            local_engine = localDbConnection.get_engine()
            cloud_engine = cloudDbConnection.get_engine()

            local_metadata = MetaData()
            local_metadata.reflect(local_engine)
            primary_key = get_primary_key_column(cloud_engine, table_name)

            print(f"primary_key is {primary_key}")

            # Check if history table exists
            history_table_name = table_name + "_zkqygjhistory"
            if history_table_name not in local_metadata.tables:
                return make_response("History table does not exist: "+history_table_name, 500)

            # Get the latest change from local history table
            latest_change = get_latest_change_from_history(local_engine, history_table_name, primary_key)
            
            print(f"latest_change is {latest_change}")

            total_row = len(latest_change)
            matched_row = 0
            # print(latest_change)       

            # next step using loop for each history, check for update , insert , delete 
            for row in latest_change:

                action = row[0]  # get the action type (either insert, delete, update)
                primary_key_values = row[3:3+len(primary_key)]

                # check for the column is exist in the DB or not (0, --> not exist , 1, --> exist)
                # query = f"""select exists (
                # select 1 from {table_name} 
                # where {primary_key[0]} = {row[3]}
                # ) as id_exist;"""
                where_clause = ' AND '.join([f"{column} = {value}" for column, value in zip(primary_key, primary_key_values)])

                # Construct the final query
                query = f"""SELECT EXISTS (
                                SELECT 1 FROM {table_name} 
                                WHERE {where_clause}
                                ) AS id_exist;"""
                
                print(query)

                with cloud_engine.connect() as con:
                    r = con.execute(text(query))
                    result = r.fetchall()
                    
                if action == 'insert':

                    if(result[0] == (1,)):
                        # means the row in not in the cloud 
                        matched_row += 1    # the row is in the cloud 
                    else:
                        matched_row += 0    # just does nothing here

                elif action == 'update':
                    if(result[0] == (0,)):
                        matched_row += 0    
                    else:
                        query = f"""select * from {table_name} where {where_clause}"""
                        # not only need to check is it in the cloud but also have to check the data
                        with cloud_engine.connect() as con:
                            # Execute the query using the provided engine
                            r2 = con.execute(text(query))
                        
                            # Fetch the results and return them
                            cloud_data = r2.fetchall()

                        local_data = row[3:]
                        if(cloud_data == local_data):
                            matched_row += 1
                        else:
                            matched_row += 0
                elif action == 'delete':
                    if(result[0] == (0,)):
                        # means the row in not exist in the cloud 
                        matched_row += 1    
                    else:
                        matched_row += 0    
                else:
                    # Handle unrecognized action
                    print("Unrecognized action:", action)
            if(matched_row == total_row):
                # return yes as json
                return make_response("Second Validation success", 200)
            
            return make_response("Second Validation Not matched ", 200)
            

        else:
            return make_response("Database credentials incorrect.", 500)
    else:
        return make_response("Table name not provided in the request body.", 400)



# get all the lastest chages for all the row in the history table
def get_latest_change_from_history(engine, history_table_name, primary_key_column):
    # query = f"""
    #     SELECT * FROM {history_table_name}
    #     WHERE ({primary_key_column}, revision_zkqygj) IN (
    #         SELECT {primary_key_column}, MAX(revision_zkqygj)
    #         FROM {history_table_name}
    #         GROUP BY {primary_key_column}
    #     )
    # """
    # Construct the comma-separated primary key column names
    primary_key_columns_str = ', '.join(primary_key_column)
    print(f"primary_key_columns_str is {primary_key_columns_str}")

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


def get_primary_key_column(engine, table_name):       # may cause issue since it only work for single primary key
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

