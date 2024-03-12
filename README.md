
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