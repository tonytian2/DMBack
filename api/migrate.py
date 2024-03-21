from flask import Blueprint, jsonify, make_response, request, session
from sqlalchemy import MetaData, text, Table
from sqlalchemy.orm import sessionmaker
from api.credentials import localDbConnectionDict, cloudDbConnectionDict, early_return_decorator
import decimal, datetime
import logging
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
history_suffix = "zkqygjhistory"

def alter_table_statement(table_name,source_metadata):
    table = Table(table_name,source_metadata)
    primaryKeyColNames = [pk_column.name for pk_column in table.primary_key.columns.values()]
    modify_pk = [f"MODIFY COLUMN {pk_column.name} int(11) NOT NULL,"   for pk_column in table.primary_key.columns.values()]
    primaryKeyColNames_str = ", ".join(primaryKeyColNames)
    modify_pk_str = " ".join(modify_pk)
    return f""" 
    ALTER TABLE {table_name} {modify_pk_str} 
    DROP PRIMARY KEY, ENGINE = MyISAM,
     ADD action VARCHAR(8) DEFAULT 'insert' FIRST, 
    ADD revision INT(6) NOT NULL AUTO_INCREMENT AFTER action,
    ADD dt_datetime DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER revision,
    ADD PRIMARY KEY ({primaryKeyColNames_str}, revision);
    """
def drop_trigger(con,table_name):
    con.execute(text(f"DROP TRIGGER IF EXISTS {table_name}__ai;"))
    con.execute(text(f"DROP TRIGGER IF EXISTS {table_name}__au;"))
    con.execute(text(f"DROP TRIGGER IF EXISTS {table_name}__bd;"))

    
    
def add_trigger(con, table_name,source_metadata):
    table = Table(table_name,source_metadata)
    primaryKeyColNames = [f" d.{pk_column.name} = NEW.{pk_column.name} " for pk_column in table.primary_key.columns.values()]
    primaryKeyColNames_str = " AND ".join(primaryKeyColNames)
    for trigger in [("ai","insert"), ("au","update"), ("bd","delete")]:
        con.execute(text(f"""CREATE TRIGGER {table_name}__{trigger[0]} AFTER INSERT ON {table_name} FOR EACH ROW
        INSERT INTO {table_name}_{history_suffix} SELECT '{trigger[1]}', NULL, NOW(), d.* 
        FROM  {table_name} AS d WHERE {primaryKeyColNames_str};"""))

        
@migrate_blueprint.route("/v1/create_history", methods=["POST"])
@early_return_decorator
def create_hisotry_tables():
    session_id = session["session_id"]
    localDbConnection = localDbConnectionDict[session_id]
    source_engine = localDbConnection.get_engine()
    source_metadata = MetaData()
    source_metadata.reflect(source_engine)
    with  source_engine.connect() as con:
        con.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        con.commit()
    #create_history_table_statements = ""
    #create_history_table_template = lambda name: f"DROP table IF EXISTS {name}_zKQygJhistory;\n CREATE TABLE {name}_zKQygJhistory LIKE {name}; \n"
    with  source_engine.connect() as con:  
        for table_name in source_metadata.tables:
            if history_suffix not in table_name: 
                con.execute(text(f"DROP table IF EXISTS {table_name}_{history_suffix};"))
                con.execute(text(f"CREATE TABLE {table_name}_{history_suffix} LIKE {table_name};"))
                #create_history_table_statements += create_history_table_template(table_name)
                #print(create_history_table_statements)
         
        con.commit()

    source_metadata.reflect(source_engine)
    with  source_engine.connect() as con:  
        for table_name in source_metadata.tables:
            
            if history_suffix in table_name:
                con.execute(text(alter_table_statement(table_name,source_metadata)))
            else:
                drop_trigger(con,table_name)
        con.commit()
    source_metadata.reflect(source_engine)
    with  source_engine.connect() as con:  
        for table_name in source_metadata.tables:
            if history_suffix not in table_name:
                add_trigger(con, table_name, source_metadata)
        con.commit()
        


    return "OK"    







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

            # Run the migration over the table list
            output = migrate_table_list(list(source_metadata.tables.keys()), source_metadata, source_session, destination_engine, True)

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


def migrate_table_list(table_list, source_metadata, source_session, destination_engine, shouldLog = False):
    output = {}
    for i in range(len(table_list)):
        table_name = table_list[i]
        if table_name not in source_metadata.tables:
            output[table_name] = "Table does not exist in local database."
            continue

        source_table = source_metadata.tables[table_name]
        rows = source_session.query(source_table).all()
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
                            isinstance(v, int)
                            or isinstance(v, NoneType)
                            or isinstance(v, decimal.Decimal)
                            or isinstance(v, float)
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