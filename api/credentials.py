from flask import Blueprint, make_response, request, session
from sqlalchemy import MetaData, text
from util.globals import history_suffix, localDbConnectionDict, cloudDbConnectionDict, DbConnection
import secrets
import os

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
    session_id = session["session_id"]
    if session_id in localDbConnectionDict:
        delete_history(localDbConnectionDict[session_id])
    clear_session()
    delete_log()
    delete_snapshot()
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

def delete_snapshot():
    files = [f for f in os.listdir("snapshot")]
    for file in files:
        if "csv" in file:
            os.remove("snapshot/" + file)

def delete_history(localDbConnection):
    source_engine = localDbConnection.get_engine()
    source_metadata = MetaData()
    source_metadata.reflect(source_engine)
    with source_engine.connect() as conn:
        for table_name in source_metadata.tables:
            if history_suffix in table_name:
                conn.execute(text(f"DROP TRIGGER IF EXISTS {table_name}__ai"))
                conn.execute(text(f"DROP TRIGGER IF EXISTS {table_name}__au"))
                conn.execute(text(f"DROP TRIGGER IF EXISTS {table_name}__bd"))
                conn.execute(text(f"DROP table IF EXISTS {table_name}"))
        conn.commit()