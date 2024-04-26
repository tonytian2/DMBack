import os

from flask import Blueprint, jsonify, make_response, session
from sqlalchemy import MetaData, text
from sqlalchemy.orm import sessionmaker

from util.globals import early_return_decorator, localDbConnectionDict

schema_data_blueprint = Blueprint("schema_data", __name__)


@schema_data_blueprint.route("/v1/metadata/source", methods=["GET"])
@early_return_decorator
def get_table_info():
    session_id = session["session_id"]
    localDbConnection = localDbConnectionDict[session_id]
    if localDbConnection.isValid:
        source_engine = localDbConnection.get_engine()
        source_metadata = MetaData()
        source_metadata.reflect(source_engine)
        Session = sessionmaker(bind=source_engine)
        source_session = Session()
        columnNames = {}
        table_names = source_metadata.tables.keys()
        filtered_table_names = [
            table_name
            for table_name in table_names
            if "zkqygjhistory" not in table_name
        ]

        for table_name in filtered_table_names:
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
        respone = {"metadata": columnNames, "recovered": load_log()}
        json_response = jsonify(respone)
        source_session.close()
        return make_response(json_response, 200)
    return make_response("No connection to local database", 500)


def load_log():
    files = [f for f in os.listdir()]

    for file in files:
        if "log" in file:
            r = {
                "timestamp": file.split(".")[0].replace(";", ":").replace("_", " "),
                "finishedTables": [],
            }
            with open(file, "r") as f:
                Lines = f.readlines()
                for line in Lines:
                    if "Finished Table:" in line:
                        r["finishedTables"].append(line.strip().split(":", 1)[1])

                return r

    return {}
