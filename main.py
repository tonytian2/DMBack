from flask import Flask
from api.credentials import credentials_blueprint
from api.schema_data import schema_data_blueprint
from api.migrate import migrate_blueprint
import secrets


app = Flask(__name__)
app.secret_key = secrets.token_hex(16)
app.register_blueprint(credentials_blueprint)
app.register_blueprint(schema_data_blueprint)
app.register_blueprint(migrate_blueprint)