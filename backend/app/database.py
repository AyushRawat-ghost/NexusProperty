import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

try:
    from dotenv import load_dotenv
    env_path = os.path.join(os.path.dirname(__file__), "../../.env")
    load_dotenv(env_path)
    print("Loaded Env Files....")
except ImportError :
    print("Failed to load env files....")
    pass

RDS_USER = os.getenv("RDS_USERNAME")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")
RDS_ENDPOINT = os.getenv("RDS_ENDPOINT")
RDS_PORT = os.getenv("RDS_PORT")
RDS_DB_NAME = os.getenv("RDS_DB_NAME")

DATABASE_URL = f"postgresql://{RDS_USER}:{RDS_PASSWORD}@{RDS_ENDPOINT}:{RDS_PORT}/{RDS_DB_NAME}"

engine = create_engine(DATABASE_URL,pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()
print("Network Layer Established : Database engine connection pool ready : ",RDS_ENDPOINT)


