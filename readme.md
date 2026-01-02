
## After cloned repo do this

# To create a python virutal env, (Must at root folder MainProject)
python3 -m venv projenv
source projenv/bin/activate

# Install python dependencies
pip install -r reqeuirements.txt

# After install the reqeuirement.txt just run this before run the project
python -m nltk.downloader punkt

# install postgresql and do setup

## After models.py changed then verify and do this 
# To make migration or versions
alembic revision --autogenerate -m "msg here"

# To create/reflect tables in db just run this
alembic upgrade head

## To run the app
uvicorn app.main:app --reload --log-level info

## All about postgresql
# Some commmads at psql shell
\q = quit from shell
\dt = list all tables only
\d = what inside db along with tables
\d table_name = describe the table(table_name)

# To start psql (Replace 'start' with 'stop', 'status' for various details )
sudo service postgresql start

# Login as superuser postgres to main db psql 
sudo -u postgres psql

# To creating our db
CREATE DATABASE literature_db;

# If creating own user and pass do this or use postgres user
CREATE USER hola WITH PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE literature_db TO hola;
ALTER USER hola CREATEDB;

# Also can be login using postgresql user (auth at tcp) to our db
sudo -u postgres psql literature_db


## Model update and generation using alembic(Do this if any edits on the model.py)
alembic revision --autogenerate -m "add ingestion and text block tables"

# Applay migration to db
alembic upgrade head


## Best tool to view postgres db
sudo apt update
sudo apt install phppgadmin

# To access
http://localhost/phppgadmin

## If login failed
sudo nano /etc/phppgadmin/config.inc.php

# change 'true' to 'false'
$conf['extra_login_security'] = false;

# then restart apache2
sudo systemctl restart apache2


## To setup spacy
python -m spacy download en_core_web_sm

## Test graph drawer
python3 -m scripts.draw_graph --job-id 6