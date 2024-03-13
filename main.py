from flask import Flask, jsonify, request, make_response
from sqlalchemy import create_engine, MetaData, text, inspect
from sqlalchemy.orm import sessionmaker
import decimal,datetime

app = Flask(__name__)
NoneType = type(None)
class DbConnection(object):
    def __init__(self):
        self.reset()

    def connection_string(self):
        return f"mysql+{self.connector}://{self.username}:{self.password}@{self.url}"
    
    def get_engine(self):
        if(self.connector != None and self.engine == None):
            self.engine = create_engine(self.connection_string())
        return self.engine
    
    def reset(self):
        self.username = None
        self.password = None
        self.url = None
        self.connector = None
        self.engine = None
        self.isValid = False

class GlobalVariables():
    def __init__(self):
        self.migratedRows = 0
        self.totalRows
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
localDbConnection = DbConnection()
cloudDbConnection = DbConnection()

@app.route('/api/set_credentials', methods=['POST'])
def set_local_credentials():
    reset()
    localDbConnection.username = request.form['local_username']
    localDbConnection.password = request.form['local_password']
    localDbConnection.url = request.form['local_url']
    localDbConnection.connector = "pymysql"
    cloudDbConnection.username = request.form['cloud_username']
    cloudDbConnection.password = request.form['cloud_password']
    cloudDbConnection.url = request.form['cloud_url']
    cloudDbConnection.connector = "mysqlconnector"
    # Establish the database connections
    source_engine = localDbConnection.get_engine()
    destination_engine = cloudDbConnection.get_engine()
    try:
        source_engine.connect()
        localDbConnection.isValid = True
        try:
            destination_engine.connect()
            cloudDbConnection.isValid = True
        except Exception as error:
    # handle the exception
            print("An exception occurred:", error) 
            cloudDbConnection.reset()
            return make_response("Cloud credentials incorrect", 511)
        
        return make_response("OK", 200)
    except Exception as error:
    # handle the exception
        print("An exception occurred:", error) 
        localDbConnection.reset()
        cloudDbConnection.reset()
        return make_response("Local credentials incorrect", 511)
    
@app.route('/api/get_table_info', methods=['GET'])
def get_table_info():
    if(localDbConnection.isValid):

        source_engine = localDbConnection.get_engine()
        source_metadata = MetaData()
        source_metadata.reflect(source_engine)
        Session = sessionmaker(bind=source_engine)
        source_session = Session()
        columnNames = {}

        for table_name in source_metadata.tables.keys():

            # Get row count
            query = text("SELECT COUNT(*) FROM " + localDbConnection.url.split('/')[-1] + "." + table_name)
            rowCount = source_session.execute(query).scalar()
            
            # Get column names
            column_names = source_metadata.tables[table_name].columns.keys()
            
            columnNames[table_name] = {'rows':rowCount,'columns':column_names}
        
        json_response = jsonify(columnNames)
        source_session.close()
        return json_response
    return 'Local connection not valid'




@app.route('/api/migrate_all', methods=['GET'])
def migrate_all():
    try:
        if(localDbConnection.isValid and cloudDbConnection.isValid):
            source_engine = localDbConnection.get_engine()
            destination_engine = cloudDbConnection.get_engine()

            source_metadata = MetaData()
            source_metadata.reflect(source_engine)
            
            with destination_engine.connect() as con:
                con.execute(text("SET FOREIGN_KEY_CHECKS=0"))
                con.commit()
                #r = con.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'destination';"))
                #print(r.mappings().all())


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
                #print("source", source_table)

                rows = source_session.query(source_table).all()
                #print(rows)
                
                #list_of_column_names = list(source_metadata.tables.values())[i].columns.keys()
                #dic = [{list_of_column_names[i]:row[i] for i in range(len(list_of_column_names))} for row in rows]
                # Insert rows into the destination table
                with destination_engine.connect() as conn:
                    conn.execute(text(f"truncate {table_name};"))
                    entire_value = ""
                    migratedRowCount = 0
                    for row in rows:
                        #print([type(i) for i in row])
                        values = ', '.join([(str(v) if not isinstance(v,NoneType) else "NULL") if (isinstance(v, int) or isinstance(v,NoneType) or isinstance(v,decimal.Decimal) or isinstance(v, float)) else '"' + ( v.strftime('%Y-%m-%d %H:%M:%S') if isinstance(v,datetime.datetime) else v) + '"' for v in row])
                        wraped_values = "( " + values + " ), "
                        entire_value += wraped_values
                        migratedRowCount += 1
                        #print(f"INSERT INTO {table_name} VALUES ({values})")
                    #print(entire_value[:-2])
                    conn.execute(text(f"INSERT INTO {table_name} VALUES {entire_value[:-2]}"))
                    conn.commit()
                    globalVariables.setMigratedRows(migratedRowCount)
                


            # Commit the changes in the destination database
            destination_session.commit()
            
            with destination_engine.connect() as con:
                con.execute(text("SET FOREIGN_KEY_CHECKS=1"))
                con.commit()

            # Close the sessions
            source_session.close()
            destination_session.close()
            return "OK"
        else:
            return "Database credentials incorrect."
    
              
    except Exception as error:
    # handle the exception
        print("An exception occurred:", error) 
        return "Not ok"





@app.route('/api/reset', methods=['GET'])
def reset():
    localDbConnection.reset()
    cloudDbConnection.reset()
    return make_response("OK", 200)