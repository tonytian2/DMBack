from flask import Blueprint, make_response, request, session
from sqlalchemy import MetaData, text
from sqlalchemy.orm import sessionmaker
from api.credentials import localDbConnectionDict, cloudDbConnectionDict
import decimal, datetime
import logging
NoneType = type(None)

logging.basicConfig(filename='progress.log', filemode='w', format='%(message)s')
class GlobalVariables():
    def __init__(self):
        self.migratedRows = 0
        self.totalRows = 0
        self.migratedTables = []

    def getMigratedRows(self):
        return self.migratedRows

    def setMigratedRows(self, i):
        self.migratedRows = i

    def getMigratedTables(self):
        return self.migratedTables

    def setMigratedTables(self, t):
        self.migratedTables.append(t)


globalVariables = GlobalVariables()

migrate_blueprint = Blueprint("migrate", __name__)


@migrate_blueprint.route("/v1/migrate_tables", methods=["POST"])
def migrate_tables():
    if "session_id" not in session:
        return make_response(
            "No connection defined in current session, define session credentials first",
            401,
        )
    session_id = session["session_id"]
    localDbConnection = localDbConnectionDict[session_id]
    cloudDbConnection = cloudDbConnectionDict[session_id]
    try:
        if localDbConnection.isValid and cloudDbConnection.isValid:

            data = request.get_json()
            # print(data)
            # print(data["tables"])
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

                # Iterate over tables in the source database
                for i in range(len(table_names)):
                    table_name = table_names[i]
                    source_table = source_metadata.tables[table_name]
                    rows = source_session.query(source_table).all()
                    with destination_engine.connect() as conn:
                        conn.execute(text(f"truncate {table_name};"))
                        entire_value = ""
                        # migratedRowCount = 0
                        for row in rows:
                            # print([type(i) for i in row])
                            values = ", ".join(
                                [
                                    (str(v) if not isinstance(v, NoneType) else "NULL")
                                    if (
                                        isinstance(v, int)
                                        or isinstance(v, NoneType)
                                        or isinstance(v, decimal.Decimal)
                                        or isinstance(v, float)
                                    )
                                    else '"'
                                    + (
                                        v.strftime("%Y-%m-%d %H:%M:%S")
                                        if isinstance(v, datetime.datetime)
                                        else v
                                    )
                                    + '"'
                                    for v in row
                                ]
                            )
                            wraped_values = "( " + values + " ), "
                            entire_value += wraped_values
                            # migratedRowCount += 1
                            # print(f"INSERT INTO {table_name} VALUES ({values})")
                        # print(entire_value[:-2])
                        conn.execute(
                            text(f"INSERT INTO {table_name} VALUES {entire_value[:-2]}")
                        )
                        conn.commit()
                        # globalVariables.setMigratedRows(migratedRowCount)

                # Commit the changes in the destination database
                destination_session.commit()

                with destination_engine.connect() as con:
                    # r = con.execute(text(f"SELECT * from {table_name}"))
                    # print(r.mappings().all())
                    con.execute(text("SET FOREIGN_KEY_CHECKS=1"))
                    con.commit()

                # Close the sessions
                source_session.close()
                destination_session.close()
                return make_response("OK", 200)
            else:
                return make_response("Invalid request.", 400)
        else:
            return make_response("Database credentials incorrect.", 500)
    except Exception as error:
        # handle the exception
        print("An exception occurred:", error)
        return make_response(str(error), 500)


@migrate_blueprint.route("/v1/migrate_all", methods=["GET"])
def migrate_all():
    if "session_id" not in session:
        return make_response(
            "No connection defined in current session, define session credentials first",
            401,
        )

    session_id = session["session_id"]
    localDbConnection = localDbConnectionDict[session_id]
    cloudDbConnection = cloudDbConnectionDict[session_id]
    try:
        if localDbConnection.isValid and cloudDbConnection.isValid:
            source_engine = localDbConnection.get_engine()
            destination_engine = cloudDbConnection.get_engine()

            source_metadata = MetaData()
            source_metadata.reflect(source_engine)

            with destination_engine.connect() as con:
                con.execute(text("SET FOREIGN_KEY_CHECKS=0"))
                con.commit()
                # r = con.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'destination';"))
                # print(r.mappings().all())

            # Create a session for the source database
            Session = sessionmaker(bind=source_engine)
            source_session = Session()

            # Create a session for the destination database
            Session = sessionmaker(bind=destination_engine)
            destination_session = Session()

            # Iterate over tables in the source database
            for i in range(len(source_metadata.tables.keys())):
                table_name = list(source_metadata.tables.keys())[i]
                source_table = source_metadata.tables[table_name]
                # print("source", source_table)

                rows = source_session.query(source_table).all()
                # print(rows)

                # list_of_column_names = list(source_metadata.tables.values())[i].columns.keys()
                # dic = [{list_of_column_names[i]:row[i] for i in range(len(list_of_column_names))} for row in rows]
                # Insert rows into the destination table
                with destination_engine.connect() as conn:
                    conn.execute(text(f"truncate {table_name};"))
                    entire_value = ""
                    migratedRowCount = 0
                    for row in rows:
                        # print([type(i) for i in row])
                        values = ", ".join(
                            [
                                (str(v) if not isinstance(v, NoneType) else "NULL")
                                if (
                                    isinstance(v, int)
                                    or isinstance(v, NoneType)
                                    or isinstance(v, decimal.Decimal)
                                    or isinstance(v, float)
                                )
                                else '"'
                                + (
                                    v.strftime("%Y-%m-%d %H:%M:%S")
                                    if isinstance(v, datetime.datetime)
                                    else v
                                )
                                + '"'
                                for v in row
                            ]
                        )
                        wraped_values = "( " + values + " ), "
                        entire_value += wraped_values
                        migratedRowCount += 1
                        # print(f"INSERT INTO {table_name} VALUES ({values})")
                    # print(entire_value[:-2])
                    conn.execute(
                        text(f"INSERT INTO {table_name} VALUES {entire_value[:-2]}")
                    )
                    conn.commit()
                    globalVariables.setMigratedRows(migratedRowCount)
                logging.info(table_name)    
                

            # Commit the changes in the destination database
            destination_session.commit()

            with destination_engine.connect() as con:
                con.execute(text("SET FOREIGN_KEY_CHECKS=1"))
                con.commit()

            # Close the sessions
            source_session.close()
            destination_session.close()
            return make_response("OK", 200)
        else:
            return make_response("Database credentials incorrect.", 500)

    except Exception as error:
        # handle the exception
        print("An exception occurred:", error)
        return make_response(str(error), 500)
