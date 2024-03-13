from flask import Blueprint, make_response, jsonify
from api.credentials import localDbConnection
from sqlalchemy import MetaData, text
from sqlalchemy.orm import sessionmaker

schema_data_blueprint = Blueprint('schema_data', __name__)

@schema_data_blueprint.route('/api/get_schema_info', methods=['GET'])
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
        return make_response(json_response,200)
    return make_response("No connection to local database", 511)