from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from app.config.system_settings import system_settings

DATABASE_URL = system_settings.DATABASE_URL

engine = create_engine(DATABASE_URL)
Base = declarative_base()
