from flask import Blueprint, session, make_response, jsonify
from sqlalchemy import MetaData, text
from sqlalchemy.orm import sessionmaker
from api.credentials import localDbConnectionDict

schema_data_blueprint = Blueprint("schema_data", __name__)


@schema_data_blueprint.route("/v1/metadata/source", methods=["GET"])
def get_table_info():
    if "session_id" not in session:
        return make_response(
            "No connection defined in current session, define session credentials first",
            401,
        )

    session_id = session["session_id"]
    localDbConnection = localDbConnectionDict[session_id]
    if localDbConnection.isValid:

        source_engine = localDbConnection.get_engine()
        source_metadata = MetaData()
        source_metadata.reflect(source_engine)
        Session = sessionmaker(bind=source_engine)
        source_session = Session()
        columnNames = {}

        for table_name in source_metadata.tables.keys():

            # Get row count
            query = text(
                "SELECT COUNT(*) FROM "
                + localDbConnection.url.split("/")[-1]
                + "."
                + table_name
            )
            rowCount = source_session.execute(query).scalar()

            # Get column names
            column_names = source_metadata.tables[table_name].columns.keys()

            columnNames[table_name] = {"rows": rowCount, "columns": column_names}

        json_response = jsonify(columnNames)
        source_session.close()
        return make_response(json_response, 200)
    return make_response("No connection to local database", 500)
