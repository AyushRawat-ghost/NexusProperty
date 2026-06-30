import os
import json
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import time
from sqlalchemy import text, Column, Integer, String, Float, Text
from sqlalchemy.orm import Session
from openai import OpenAI
from app.database import Base, engine, SessionLocal

class GoldMLFeatureStore(Base):
    __tablename__ = "ml_feature_store"
    __table_args__ = {"schema": "gold"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    prop_id = Column(String(100), unique=True, nullable=False, index=True)
    city = Column(String(100), nullable=False, index=True)
    normalized_price = Column(Float)
    area_sqft = Column(Float)
    luxury_score = Column(Integer)     
    proximity_score = Column(Integer)  
    extracted_tags = Column(Text)      

class GoldCoreListings(Base):
    __tablename__ = "core_listings"
    __table_args__ = {"schema": "gold"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    prop_id = Column(String(100), unique=True, nullable=False, index=True)
    prop_name = Column(String(512))
    city = Column(String(100), nullable=False, index=True) 
    property_type = Column(String(100), index=True)        
    price = Column(Float, index=True)                      
    area_sqft = Column(Float)
    bedrooms = Column(String(50))
    bathrooms = Column(String(50))
    furnishing = Column(String(100))
    luxury_score = Column(Integer, index=True)             
    proximity_score = Column(Integer)

class GoldLocalizedAnalytics(Base):
    __tablename__ = "localized_analytics"
    __table_args__ = {"schema": "gold"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String(100), unique=True, nullable=False, index=True)
    total_listings = Column(Integer)
    average_price = Column(Float)
    average_luxury_score = Column(Float)

class GoldProcessingEngine:
    def __init__(self):
        with engine.connect() as conn:
            print("Gold Layer: Validating database schema containers inside AWS...")
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold;"))
            conn.commit()
            
        print("Gold Layer: Synchronizing operational application & feature tables...")
        Base.metadata.create_all(bind=engine, tables=[
            GoldMLFeatureStore.__table__,
            GoldCoreListings.__table__,
            GoldLocalizedAnalytics.__table__
        ])

        self.ai_client = OpenAI(
            base_url="https://api.fireworks.ai/inference/v1",
            api_key=os.getenv("FIREWORKS_API_KEY")
        )

    def extract_llm_features(self, description: str) -> dict:
        if not description or len(description.strip()) < 15:
            return {"luxury_score": 1, "proximity_score": 1, "tags": ["basic"]}

        prompt = f"""
        Analyze this real estate listing description and extract key feature metrics.
        Return your answer strictly as a valid JSON object with keys "luxury_score" (integer 1-10), "proximity_score" (integer 1-10), and "tags" (list of lower_case strings representing premium amenities or location benefits).

        Description: "{description}"
        JSON Output:
        """
        try:
            response = self.ai_client.chat.completions.create(
                model="accounts/fireworks/models/llama-v3-8b-instruct",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0, 
                max_tokens=150
            )
            raw_text = response.choices[0].message.content.strip()
            return json.loads(raw_text)
        except Exception:
            return {"luxury_score": 3, "proximity_score": 3, "tags": ["standard_listing"]}

    def execute_full_gold_pipeline(self, batch_size: int = 100, max_rows: int = 5000):
        db = SessionLocal()
        try:
            print(f"\nBooting Full GenAI Pipeline. Target: Up to {max_rows} entries in batches of {batch_size}...")
            
            existing_ids = set(r[0] for r in db.execute(text("SELECT prop_id FROM gold.core_listings")).fetchall())
            print(f" Found {len(existing_ids)} rows already securely processed in Gold layer.")

            query = text("""
    SELECT s.prop_id, s.prop_name, s.city, s.property_type, s.price, s.area_sqft, 
           s.bedrooms, s.bathrooms, s.furnishing, s.description 
    FROM silver.properties s
    LEFT JOIN gold.core_listings g ON s.prop_id = g.prop_id
    WHERE s.description IS NOT NULL 
      AND length(s.description) > 15
      AND g.prop_id IS NULL -- Only fetches remaining unparsed entries!
    LIMIT {max_rows};
""")
            candidates = db.execute(query).fetchall()
            total_candidates = len(candidates)
            print(f"Total remaining items to parse in this execution block: {total_candidates}")
            
            if total_candidates == 0:
                print("Everything is already up to date!")
                self.recalculate_analytics(db)
                return

            ml_features = []
            core_listings = []
            processed_count = 0
            start_time = time.time()

            for idx, row in enumerate(candidates):
                (prop_id, prop_name, city, property_type, price, area_sqft, 
                 bedrooms, bathrooms, furnishing, description) = row
                
                ai_extracted = self.extract_llm_features(description)
                lux_score = ai_extracted.get("luxury_score", 5)
                prox_score = ai_extracted.get("proximity_score", 5)
                tags_array = ai_extracted.get("tags", [])

                ml_features.append(GoldMLFeatureStore(
                    prop_id=prop_id, city=city, normalized_price=price, area_sqft=area_sqft,
                    luxury_score=lux_score, proximity_score=prox_score, extracted_tags=json.dumps(tags_array)
                ))

                core_listings.append(GoldCoreListings(
                    prop_id=prop_id, prop_name=prop_name, city=city, property_type=property_type,
                    price=price, area_sqft=area_sqft, bedrooms=bedrooms, bathrooms=bathrooms,
                    furnishing=furnishing, luxury_score=lux_score, proximity_score=prox_score
                ))

                if (idx + 1) % batch_size == 0 or (idx + 1) == total_candidates:
                    processed_count += len(ml_features)
                    
                    db.bulk_save_objects(ml_features)
                    db.bulk_save_objects(core_listings)
                    db.commit()
                    
                    elapsed = time.time() - start_time
                    speed = processed_count / elapsed if elapsed > 0 else 0
                    print(f"Batch Saved! Processed: {processed_count}/{total_candidates} | Speed: {speed:.1f} rows/sec")
                    
                    ml_features.clear()
                    core_listings.clear()

            self.recalculate_analytics(db)

        except Exception as e:
            print(f"Gold layer failure: {e}")
            db.rollback()
        finally:
            db.close()

    def recalculate_analytics(self, db: Session):
        print("\nCompiling localized aggregate business indicators for Table 3...")
        db.query(GoldLocalizedAnalytics).delete() 
        aggregation_query = text("""
            INSERT INTO gold.localized_analytics (city, total_listings, average_price, average_luxury_score)
            SELECT city, COUNT(*), AVG(price), AVG(luxury_score)
            FROM gold.core_listings
            GROUP BY city;
        """)
        db.execute(aggregation_query)
        db.commit()
        print("Gold Master Layer Production Pipeline Completed Successfully!")

if __name__ == "__main__":
    print("\n INITIATING FULL PRODUCTION GOLD LAYER CORE RUN...")
    gold_engine = GoldProcessingEngine()
    gold_engine.execute_full_gold_pipeline(batch_size=100, max_rows=2500)