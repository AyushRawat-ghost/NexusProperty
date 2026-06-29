from sqlalchemy import exc
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import pandas as pd
from sqlalchemy import text,String,Integer,DateTime,Column,Float,Text
from app.database import engine,SessionLocal,Base
from sqlalchemy.orm import Session

class SilverDimLookup(Base):
    __tablename__ = 'dim_lookups'
    __table_args__ = {'schema':'silver'}

    id = Column(Integer,primary_key=True,autoincrement=True)
    category = Column(String(100),nullable=False,index=True)
    lookup_id = Column(String(255),nullable=False,index=True)
    label = Column(String(255),nullable=False)

class SilverUnifiedProperty(Base):
    __tablename__ = "properties"
    __table_args__ = {"schema":"silver"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    prop_id = Column(String(100), unique=True, nullable=False, index=True)
    prop_name = Column(String(512))
    city = Column(String(100), nullable=False, index=True)
    property_type = Column(String(100))
    price = Column(Float)
    area_sqft = Column(Float)
    price_per_sqft = Column(Float)
    
    bedrooms = Column(String(50))
    bathrooms = Column(String(50))
    furnishing = Column(String(100))
    facing_direction = Column(String(100))
    property_age = Column(String(100))
    description = Column(Text)

class SilverProcessingEngine:
    def __init__(self):
        with engine.connect() as conn:
            print("Silver Layer Establishing connection....")
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))
            conn.commit()
        Base.metadata.create_all(bind=engine, tables=[
            SilverDimLookup.__table__,
            SilverUnifiedProperty.__table__
            ])
        print("Silver Schema Online & Ready....")
    
    def build_dimension_lookups(self):
        db=SessionLocal()
        try:
            print("Processing metadata lookup....")
            query = text("""
                SELECT source_file, raw_data_json 
                FROM bronze.raw_property_landing 
                WHERE source_file LIKE 'Housing-Detail-CSV/%';
            """)
            results = db.execute(query).fetchall()
            print("Number of records found : ",len(results))
            db.query(SilverDimLookup).delete(synchronize_session=False)
            records_to_insert = []
            for row in results:
                file_path = row[0]
                category = os.path.basename(file_path).replace(".csv", "")
                json_data = json.loads(row[1])
                
                if 'id' in json_data and 'label' in json_data:
                    lookup_record = SilverDimLookup(
                        category=category,
                        lookup_id=str(json_data['id']),
                        label=str(json_data['label'])
                    )
                    records_to_insert.append(lookup_record)
            
            if records_to_insert:
                db.bulk_save_objects(records_to_insert)
                db.commit()
                print(f"Saved {len(records_to_insert)} dimensional records.")
            else:
                print("No dimensional records to save.")
        except Exception as e:
            print("Error whil compiling dimensions layout : ",e)
            db.rollback()
        finally:
            db.close()

    def load_lookup_dictionary(self,db:Session) ->dict:
        lookups = db.query(SilverDimLookup).all()
        lookup_dict={}
        for l in lookups:
            if l.category not in lookup_dict:
                lookup_dict[l.category]={}
            lookup_dict[l.category][str(l.lookup_id)]=l.label
        return lookup_dict

    def clean_and_flatten_properties(self):
        db = SessionLocal()
        try:
            maps = self.load_lookup_dictionary(db)
            
            print("\n Processing and cleaning structural property vectors across cities...")
            query = text("""
                SELECT source_file, raw_data_json 
                FROM bronze.raw_property_landing 
                WHERE source_file LIKE 'Housing-By-City-CSV/%';
            """)
            results = db.execute(query).fetchall()
            cleaned_properties = []
            seen_prop_ids = set() # Avoid inserting duplicate rows if they exist in source chunks
            
            for row in results:
                raw_json = json.loads(row[1])
                
                prop_id = str(raw_json.get('PROP_ID'))
                if not prop_id or prop_id == 'None' or prop_id in seen_prop_ids:
                    continue
                    
                try:
                    price = float(raw_json.get('PRICE')) if raw_json.get('PRICE') else None
                    area = float(raw_json.get('AREA')) if raw_json.get('AREA') else None
                    price_sqft = float(raw_json.get('PRICE_SQFT')) if raw_json.get('PRICE_SQFT') else None
                except ValueError:
                    price, area, price_sqft = None, None, None

                bedroom_id = str(raw_json.get('BEDROOM_NUM'))
                bathroom_id = str(raw_json.get('BATHROOM_NUM'))
                furnish_id = str(raw_json.get('FURNISH'))
                facing_id = str(raw_json.get('FACING'))
                age_id = str(raw_json.get('AGE'))
                clean_property = SilverUnifiedProperty(
                    prop_id=prop_id,
                    prop_name=raw_json.get('PROP_NAME') or raw_json.get('PROP_HEADING'),
                    city=str(raw_json.get('CITY')).upper(),
                    property_type=raw_json.get('PROPERTY_TYPE'),
                    price=price,
                    area_sqft=area,
                    price_per_sqft=price_sqft,
                    
                    bedrooms=maps.get('BEDROOM_NUM', {}).get(bedroom_id, bedroom_id),
                    bathrooms=maps.get('BATHROOM_NUM', {}).get(bathroom_id, bathroom_id),
                    furnishing=maps.get('FURNISH', {}).get(furnish_id, furnish_id),
                    facing_direction=maps.get('FACING_DIRECTION', {}).get(facing_id, facing_id),
                    property_age=maps.get('AGE', {}).get(age_id, age_id),
                    
                    description=raw_json.get('DESCRIPTION')
                )
                cleaned_properties.append(clean_property)
                seen_prop_ids.add(prop_id)
            if cleaned_properties:
                print(f"Bulk-uploading {len(cleaned_properties)} unified entries to silver.properties warehouse...")
                db.bulk_save_objects(cleaned_properties)
                db.commit()
                print("🏆 Silver Master Production Repository is fully synchronized and operational!")

        except Exception as e:
            print(f"Silver processing cycle failure: {e}")
            db.rollback()
        finally:
            db.close()

if __name__ == "__main__":
    print("\nNITIATING PRODUCTION SILVER CLEANING LAYER PIPELINE RUN...")
    engine_cleaner = SilverProcessingEngine()
    engine_cleaner.build_dimension_lookups()
    engine_cleaner.clean_and_flatten_properties()
    