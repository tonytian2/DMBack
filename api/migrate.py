from flask import Blueprint, jsonify, make_response, request, session
from sqlalchemy import MetaData, text
from sqlalchemy.orm import sessionmaker
import decimal, datetime
import logging
from api.credentials import localDbConnectionDict, cloudDbConnectionDict, early_return_decorator
from util.snapshots import snapshot_database_tables, get_latest_snapshot_data

NoneType = type(None)


class GlobalVariables():
    def __init__(self):
        self.migratedRows = {}
        self.totalRows = 0
        self.migratedTables = []

    def getMigratedRows(self):
        return self.migratedRows

    def setMigratedRows(self, tableName, count):
        self.migratedRows[tableName] = count

    def getMigratedTables(self):
        return self.migratedTables

    def setMigratedTables(self, t):
        self.migratedTables.append(t)


globalVariables = GlobalVariables()

migrate_blueprint = Blueprint("migrate", __name__)


@migrate_blueprint.route("/v1/migrate_tables", methods=["POST"])
@early_return_decorator
def migrate_tables():
    session_id = session["session_id"]
    localDbConnection = localDbConnectionDict[session_id]
    cloudDbConnection = cloudDbConnectionDict[session_id]
    try:
        if localDbConnection.isValid and cloudDbConnection.isValid:

            data = request.get_json()
            if "tables" in data and isinstance(data["tables"], list):
                table_names = data["tables"]
                source_engine = localDbConnection.get_engine()
                destination_engine = cloudDbConnection.get_engine()

                source_metadata = MetaData()
                source_metadata.reflect(source_engine)

                with destination_engine.connect() as con:
                    con.execute(text("SET FOREIGN_KEY_CHECKS=0"))
                    con.commit()

                Session = sessionmaker(bind=source_engine)
                source_session = Session()

                # Create a session for the destination database
                Session = sessionmaker(bind=destination_engine)
                destination_session = Session()

                # Run the migration over the table list
                output = migrate_table_list(table_names, source_metadata, source_session, destination_engine)

                # Commit the changes in the destination database
                destination_session.commit()

                with destination_engine.connect() as con:
                    con.execute(text("SET FOREIGN_KEY_CHECKS=1"))
                    con.commit()

                # Close the sessions
                source_session.close()
                destination_session.close()
                return make_response(jsonify(output), 200)
            else:
                return make_response("Invalid request.", 400)
        else:
            return make_response("Database credentials incorrect.", 500)
    except Exception as error:
        # handle the exception
        print("An exception occurred:", error)
        return make_response(str(error), 500)


@migrate_blueprint.route("/v1/migrate_all", methods=["GET"])
@early_return_decorator
def migrate_all():
    session_id = session["session_id"]
    localDbConnection = localDbConnectionDict[session_id]
    cloudDbConnection = cloudDbConnectionDict[session_id]
    try:
        if localDbConnection.isValid and cloudDbConnection.isValid:
            source_engine = localDbConnection.get_engine()
            destination_engine = cloudDbConnection.get_engine()
            
            # duplicate the schema from local to cloud
            meta = MetaData()
            meta.reflect(source_engine)
            meta.create_all(destination_engine)

            current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H;%M;%S")
            logging.basicConfig(
                                handlers=[
                                    logging.FileHandler(f"{current_time}.log", "w"),
                                    logging.StreamHandler()
                                    ],
                                format='%(message)s',level = logging.INFO)
            start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"Migration started at {start_time}")
            source_metadata = MetaData()
            source_metadata.reflect(source_engine)

            with destination_engine.connect() as con:
                con.execute(text("SET FOREIGN_KEY_CHECKS=0"))
                con.commit()

            # Create a session for the source database
            Session = sessionmaker(bind=source_engine)
            source_session = Session()

            # Create a session for the destination database
            Session = sessionmaker(bind=destination_engine)
            destination_session = Session()

            # Save a snapshot of the table data
            snapshot_database_tables(source_metadata, source_session)

            # Run the migration over the table list
            output = migrate_table_list(list(source_metadata.tables.keys()), source_metadata, destination_engine, True)

            # Commit the changes in the destination database
            destination_session.commit()

            with destination_engine.connect() as con:
                con.execute(text("SET FOREIGN_KEY_CHECKS=1"))
                con.commit()

            # Close the sessions
            source_session.close()
            destination_session.close()
            end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"Migration ended at {end_time}")
            logging.shutdown()
            return make_response(jsonify(output), 200)
        else:
            return make_response("Database credentials incorrect.", 500)

    except Exception as error:
        # handle the exception
        print("An exception occurred:", error)
        return make_response(str(error), 500)


def migrate_table_list(table_list, source_metadata, destination_engine, shouldLog = False):
    output = {}
    for i in range(len(table_list)):
        table_name = table_list[i]
        if table_name not in source_metadata.tables:
            output[table_name] = "Table does not exist in local database."
            continue

        rows = get_latest_snapshot_data(table_name)
        # Insert rows into the destination table
        with destination_engine.connect() as conn:
            conn.execute(text(f"truncate {table_name};"))
            entire_value = ""
            migratedRowCount = 0
            for row in rows:
                values = ", ".join(
                    [
                        (str(v) if not isinstance(v, NoneType) else "NULL")
                        if (
                            isinstance(v, (int, NoneType, decimal.Decimal, float))
                        )
                        else (
                            '"' + v.strftime("%Y-%m-%d %H:%M:%S") + '"' if isinstance(v, datetime.datetime)
                            else '"' + v.strftime("%Y-%m-%d") + '"' if isinstance(v, datetime.date)
                            else "'" + v + "'" if '"' in v
                            else '"' + v + '"'
                        )
                        for v in row
                    ]
                )
                wraped_values = "( " + values + " ), "
                entire_value += wraped_values
                migratedRowCount += 1
            conn.execute(
                text(f"INSERT INTO {table_name} VALUES {entire_value[:-2]}")
            )
            conn.commit()
        globalVariables.setMigratedRows(table_name, migratedRowCount)
        if shouldLog:
            logging.info("Finished Table:"+table_name)
        output[table_name] = migratedRowCount
    return output