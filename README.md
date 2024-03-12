
# Database Migration - Backend

Backend for migrating a MySQL project from a local database to a cloud (Azure) platform.

## Set up

### Set up virtual environment
Navigate to project root directory, then set up virtual environment:
#### OSX/Linux
```bash
python3 -m venv .venv

```
#### Windows
```cmd
py -3 -m venv .venv
```

### Activate the environment

#### OSX/Linux
```bash
. .venv/bin/activate
```

#### Windows
```cmd
.venv\Scripts\activate
```
### Install dependencies

```bash
pip install Flask
pip install sqlalchemy
pip install pymysql
```
## Run Locally

Start the server using the following command:
```bash
flask --app main run --debug
```
Make requests to `localhost:5000`

the `--debug` flag means the server will auto-reload when a change is detected.