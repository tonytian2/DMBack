from flask import Flask
from flask_cors import CORS
from api.credentials import credentials_blueprint
from api.schema_data import schema_data_blueprint
from api.migrate import migrate_blueprint
from api.validate import validate_blueprint
import secrets


app = Flask(__name__)
cors = CORS(app, supports_credentials=True)
app.secret_key = secrets.token_hex(16)
app.config['SESSION_COOKIE_SAMESITE'] = 'None'
app.config['SESSION_COOKIE_SECURE'] = True
app.register_blueprint(credentials_blueprint)
app.register_blueprint(schema_data_blueprint)
app.register_blueprint(migrate_blueprint)
app.register_blueprint(validate_blueprint)