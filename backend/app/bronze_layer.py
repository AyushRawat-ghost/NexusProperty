import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
import pandas as pd
import datetime
from sqlalchemy import text, Column, Integer, String, DateTime, Text
from sqlalchemy.orm import Session
from app.database import Base, engine, SessionLocal

class ProcessedFileLedger(Base):
    __tablename__ = "processed_files_ledger"
    __table_args__ = {"schema": "bronze"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    file_key = Column(String(512), unique=True, nullable=False, index=True)
    processed_at = Column(DateTime, default=datetime.datetime.utcnow)

class RawPropertyLandingTable(Base):
    __tablename__ = "raw_property_landing"
    __table_args__ = {"schema": "bronze"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_file = Column(String(512), nullable=False, index=True)
    raw_data_json = Column(Text, nullable=False)              
    ingested_at = Column(DateTime, default=datetime.datetime.utcnow)


# Bronze Ingestion Engine Class
class BronzeIngestionEngine:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3')
        
        with engine.connect() as conn:
            print("Bronze Layer: Validating database schema containers inside AWS...")
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))
            conn.commit()
            
        print("Bronze Layer: Syncing Cloud ledger & landing frameworks Online.....")
        Base.metadata.create_all(bind=engine, tables=[
            ProcessedFileLedger.__table__,
            RawPropertyLandingTable.__table__
        ])

    def is_file_processed(self, db: Session, file_key: str) -> bool:
        return db.query(ProcessedFileLedger).filter(ProcessedFileLedger.file_key == file_key).first() is not None

    def mark_as_processed(self, db: Session, file_key: str) -> bool:
        try:
            ledger_record = ProcessedFileLedger(file_key=file_key)
            db.add(ledger_record)
            db.commit()
            print(f"File Processed State Secured : {file_key}")
            return True
        except Exception as e:
            db.rollback()
            print(f"Error marking file as processed: {e}")
            return False

    def scan_s3_folder(self, prefix: str):
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            
            file_keys = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    if key.startswith(prefix) and key.endswith('.csv'):
                        file_keys.append(key)
            return file_keys
        except Exception as e:
            print(f"Error scanning S3 folder: {e}")
            return []

    def stream_raw_csv(self, s3_file_key: str, chunk_size: int = 5000):
        db = SessionLocal()
        try:
            if self.is_file_processed(db, s3_file_key):
                print("Skipping the file (Ledger match found): ", s3_file_key)
                return
            
            print("Processing the file... : ", s3_file_key)
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_file_key)
            
            for chunk_idx, chunk_df in enumerate(pd.read_csv(response['Body'], chunksize=chunk_size, low_memory=False)):
                print(f"File : {s3_file_key}, Chunk : {chunk_idx}")
                
                self.save_chunk_to_bronze_db(db, chunk_df, s3_file_key)
                yield chunk_df
            
            self.mark_as_processed(db, s3_file_key)
            print(f"File {s3_file_key} processed successfully and state secured.....")
        except Exception as e:
            print(f"Error processing file {s3_file_key}: {e}")
            return None
        finally:
            db.close()

    def save_chunk_to_bronze_db(self, db: Session, chunk_df: pd.DataFrame, source_file: str):
        records = []
        for _, row in chunk_df.iterrows():
            row_json_str = row.to_json() # Bypasses all column mismatch issues instantly
            
            raw_record = RawPropertyLandingTable(
                source_file=source_file,
                raw_data_json=row_json_str
            )
            records.append(raw_record)
            
        db.bulk_save_objects(records)
        db.commit()
        print(f"Bulk-inserted {len(records)} raw entries into bronze.raw_property_landing.")


# =====================================================================
# 🏃‍♂️ RUN INGESTION EXECUTION PIPELINE
# =====================================================================
if __name__ == "__main__":
    BUCKET = os.getenv("Bucket", "ayush-nexusproperty-raw-lake")
    CITY_DATA_PREFIX = os.getenv("CITY_DATA_PREFIX")
    METADATA_PREFIX = os.getenv("METADATA_PREFIX")
    
    bronze_worker = BronzeIngestionEngine(bucket_name=BUCKET)
    
    print("\nWorking on cities ingestion....")
    city_files = bronze_worker.scan_s3_folder(prefix=CITY_DATA_PREFIX)
    print(f"Identified {len(city_files)} city assets target keys to parse.")

    for file_path in city_files:
        for raw_chunk in bronze_worker.stream_raw_csv(s3_file_key=file_path, chunk_size=2000):
            pass

    print("\nWorking on metadata ingestion.....")
    metadata_files = bronze_worker.scan_s3_folder(prefix=METADATA_PREFIX)
    print(f"Identified {len(metadata_files)} metadata assets target keys to parse.")

    for file_path in metadata_files:
        for raw_chunk in bronze_worker.stream_raw_csv(s3_file_key=file_path, chunk_size=2000):
            pass

    print("\n Bronze Layer Ingestion Completed Successfully!")