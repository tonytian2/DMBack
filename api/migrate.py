from flask import Blueprint, jsonify, make_response, request, session
from sqlalchemy import MetaData, text, Table
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
random_string = "zkqygj"
history_suffix = random_string + "history"

def alter_table_statement(table_name,source_metadata):
    table = Table(table_name,source_metadata)
    primaryKeyColNames = [pk_column.name for pk_column in table.primary_key.columns.values()]
    #pk_without_revision =   primaryKeyColNames.copy()
    #if f"revision_{random_string}" in pk_without_revision:
    #    pk_without_revision.remove(f"revision_{random_string}")
        
    modify_pk = [f"MODIFY COLUMN {pk_column.name} int(11) NOT NULL,"   for pk_column in table.primary_key.columns.values()]
    #primaryKeyColNames_without_revision_str = ", ".join(pk_without_revision)
    primaryKeyColName_str = ", ".join(primaryKeyColNames)
    modify_pk_str = " ".join(modify_pk)
    return f""" 
    ALTER TABLE {table_name} {modify_pk_str} 
    DROP PRIMARY KEY, ENGINE = MyISAM,
    ADD action_{random_string} VARCHAR(8) DEFAULT 'insert' FIRST, 
    ADD revision_{random_string} INT(6) NOT NULL AUTO_INCREMENT AFTER action_{random_string},
    ADD dt_datetime_{random_string} DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER revision_{random_string},
    ADD PRIMARY KEY ({primaryKeyColName_str}, revision_{random_string});
    """
def drop_trigger(con,table_name):
    con.execute(text(f"DROP TRIGGER IF EXISTS {table_name}__ai;"))
    con.execute(text(f"DROP TRIGGER IF EXISTS {table_name}__au;"))
    con.execute(text(f"DROP TRIGGER IF EXISTS {table_name}__bd;"))

    
    
def add_trigger(con, table_name,source_metadata):
    table = Table(table_name,source_metadata)
    primaryKeyColNames = [f" d.{pk_column.name} = NEW.{pk_column.name} " for pk_column in table.primary_key.columns.values()]
    primaryKeyColNames_str = " AND ".join(primaryKeyColNames)
    old_primaryKeyColNames = [f" d.{pk_column.name} = OLD.{pk_column.name} " for pk_column in table.primary_key.columns.values()]
    old_primaryKeyColNames_str = " AND ".join(old_primaryKeyColNames)
    con.execute(text(f"""CREATE TRIGGER {table_name}__ai AFTER INSERT ON {table_name} FOR EACH ROW
    INSERT INTO {table_name}_{history_suffix} SELECT 'insert', NULL, NOW(), d.* 
    FROM {table_name} AS d WHERE {primaryKeyColNames_str};"""))
    con.execute(text(f"""CREATE TRIGGER {table_name}__au AFTER UPDATE ON {table_name} FOR EACH ROW
    INSERT INTO {table_name}_{history_suffix} SELECT 'update', NULL, NOW(), d.*
    FROM {table_name} AS d WHERE {primaryKeyColNames_str};
                     """))
    con.execute(text(f"""CREATE TRIGGER {table_name}__bd BEFORE DELETE ON {table_name} FOR EACH ROW
    INSERT INTO {table_name}_{history_suffix} SELECT 'delete', NULL, NOW(), d.* 
    FROM {table_name} AS d WHERE {old_primaryKeyColNames_str};
                     """))

        
@migrate_blueprint.route("/v1/create_history", methods=["POST"])
@early_return_decorator
def create_history_tables():
    session_id = session["session_id"]
    localDbConnection = localDbConnectionDict[session_id]
    source_engine = localDbConnection.get_engine()
    source_metadata = MetaData()
    source_metadata.reflect(source_engine)
    #create_history_table_statements = ""
    #create_history_table_template = lambda name: f"DROP table IF EXISTS {name}_zKQygJhistory;\n CREATE TABLE {name}_zKQygJhistory LIKE {name}; \n"
    with  source_engine.connect() as con:  
        for table_name in source_metadata.tables:
            if history_suffix not in table_name: 
                con.execute(text(f"DROP table IF EXISTS {table_name}_{history_suffix};"))
                print(table_name,history_suffix)
                con.execute(text(f"CREATE TABLE {table_name}_{history_suffix} LIKE {table_name};"))
                #create_history_table_statements += create_history_table_template(table_name)
                #print(create_history_table_statements)
         
        con.commit()
    source_metadata = MetaData()
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

                Session = sessionmaker(bind=source_engine)
                source_session = Session()

                # Create a session for the destination database
                Session = sessionmaker(bind=destination_engine)
                destination_session = Session()

                # Run the migration over the table list
                output = migrate_table_list(table_names, source_metadata, destination_engine)
                # Commit the changes in the destination database
                destination_session.commit()

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

            # Create a session for the source database
            Session = sessionmaker(bind=source_engine)
            source_session = Session()

            # Create a session for the destination database
            Session = sessionmaker(bind=destination_engine)
            destination_session = Session()

            # Save a snapshot of the table data
            snapshot_database_tables(source_metadata, source_session)
            table_list = source_metadata.tables.keys()
            filtered_table_list = [table_name for table_name in table_list if "zkqygjhistory" not in table_name]
            # Run the migration over the table list
            output = migrate_table_list(filtered_table_list, source_metadata, destination_engine, True)

            # Commit the changes in the destination database
            destination_session.commit()

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
    with destination_engine.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        conn.commit()
        for i in range(len(table_list)):
            table_name = table_list[i]
            if table_name not in source_metadata.tables:
                output[table_name] = "Table does not exist in local database."
                continue

            rows = get_latest_snapshot_data(table_name)
            # Insert rows into the destination table
            
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
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
        conn.commit()
    return output

def value_wrapper(v):
    if isinstance(v, NoneType):
        return "NULL"
    if  isinstance(v, (int, NoneType, decimal.Decimal, float)):
        return str(v)
    if isinstance(v, datetime.date):
        return '"' + v.strftime("%Y-%m-%d") + '"'
    if isinstance(v, datetime.datetime):
        return '"' + v.strftime("%Y-%m-%d %H:%M:%S") + '"'
    if '"' in v:
        return "'" + v + "'"
    return '"' + v + '"'



@migrate_blueprint.route("/v1/migrate_updates", methods=["POST"])
@early_return_decorator
def migrate_updates():
    session_id = session["session_id"]
    localDbConnection = localDbConnectionDict[session_id]
    cloudDbConnection = cloudDbConnectionDict[session_id]
    response = {"updated_tables":[]}
    try:
        if  localDbConnection.isValid and cloudDbConnection.isValid:
            source_engine = localDbConnection.get_engine()
            destination_engine = cloudDbConnection.get_engine()
            
            # duplicate the schema from local to cloud
            source_metadata = MetaData()
            source_metadata.reflect(source_engine)
            with destination_engine.connect() as destination_con:
                with source_engine.connect() as con:  
                    for table_name in source_metadata.tables:
                        if history_suffix in table_name: 
                            updated_row_count = con.execute(text(f"select count(*) from  {table_name} where revision_{random_string} = 1;"))
                            count = updated_row_count.mappings().all()[0]["count(*)"]
                            
                            if count != 0:
                                destination_table_name = table_name.replace("_"+history_suffix,'')
                                #updated_row_count = con.execute(text(f"select count(*) from  {table_name} where revision_{random_string} = 1;"))
                                response["updated_tables"].append({destination_table_name:count})

                                table = Table(table_name,source_metadata)
                                all_col_names = [c.__getattribute__("name") for c in table.c]
                                print(all_col_names)
                                primaryKeyColNames = [pk_column.name for pk_column in table.primary_key.columns.values()]
                                primaryKeyColNames_no_revision = []
                                for c in primaryKeyColNames:
                                    if "revision_" + random_string not in c:
                                        primaryKeyColNames_no_revision.append(c)
                                
                                allColNames_no_suffix_no_primary = []
                                allColNames_no_suffix = []
                                for c in all_col_names:
                                    if random_string not in c:
                                        allColNames_no_suffix.append(c)
                                        if c not in primaryKeyColNames:
                                            allColNames_no_suffix_no_primary.append(c) 
                                

                                select_string_t2 = ", ".join(primaryKeyColNames_no_revision)
                                inner_join_condition = " AND ".join([f" t1.{c} = t2.{c} " for c in  primaryKeyColNames_no_revision])
                                query = f"""
                                SELECT t1.*
                                FROM {table_name} t1
                                INNER JOIN
                                    (
                                        SELECT {select_string_t2}, MAX(revision_{random_string}) AS max_revision_{random_string}
                                        FROM {table_name}
                                        GROUP BY {select_string_t2}
                                    ) t2
                                ON {inner_join_condition} AND t1.revision_{random_string} = t2.max_revision_{random_string};"""   
                                
                                last_updates = list(con.execute(text(query)).mappings().all())
                                #print(last_updates.mappings().all())
                                #where_clause = ' AND '.join([f"{column} = {value}" for column, value in zip(primary_key, primary_key_values)])
                                #print(last_updates)
                                for update in last_updates:
                                    primary_key_condition = ' AND '.join([f" {colName} = {value_wrapper(update[colName])} " for colName in primaryKeyColNames_no_revision])
                                    insert_values = " ( " + " , ".join([c for c in allColNames_no_suffix]) + " ) " + "values" + " ( " + " , ".join([value_wrapper(update[c]) for c in allColNames_no_suffix]) + " ) "
                                    update_values = " , ".join([f" {c} = {value_wrapper(update[c])} " for c in allColNames_no_suffix_no_primary])
                                    delete_query = f"DELETE FROM {destination_table_name} WHERE {primary_key_condition};"
                                    select_query = f"SELECT * FROM {destination_table_name} WHERE {primary_key_condition};"
                                    insert_query = f"INSERT into {destination_table_name} {insert_values} ;"
                                    update_query = f"UPDATE {destination_table_name} SET {update_values} WHERE {primary_key_condition};"
                                    if (update["action_"+random_string] == "delete"):
                                        destination_con.execute(text(delete_query))
                                        print(delete_query)
                                    elif update["action_"+random_string] in ['update', 'insert'] :
                                        if (destination_con.execute(text(select_query)).rowcount == 0):  
                                            destination_con.execute(text(insert_query))
                                            print(insert_query)
                                        elif (destination_con.execute(text(select_query)).rowcount == 1):  
                                            destination_con.execute(text(update_query))
                                            print(update_query)
                                        else:
                                            return make_response("not ok, wrong history table structure" , 500)
                                    else:
                                        return  make_response("not ok, wrong history table structure" , 500)
                                            
                                            
                                                
                        # con.execute(text(f"CREATE TABLE {table_name}_{history_suffix} LIKE {table_name};"))
                            #create_history_table_statements += create_history_table_template(table_name)
                            #print(create_history_table_statements)
                    
                    con.commit()
                destination_con.commit()
            return make_response(jsonify(response), 200)    
        else:
            return make_response("Database credentials incorrect.", 500)
               
    except Exception as e:
        return   make_response("not ok" + str(e), 500)
    