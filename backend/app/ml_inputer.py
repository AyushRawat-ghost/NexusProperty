import os
import json
import sys
# Adjust path to root directory to support direct execution
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import pickle
import re
import pandas as pd
import numpy as np
from sqlalchemy import text
from xgboost import XGBRegressor
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
from app.database import engine, SessionLocal
from app.gold_layer import GoldMLFeatureStore, GoldCoreListings


class GoldFeatureImputer:
    def __init__(self):
        self.city_encoder = LabelEncoder()
        self.type_encoder = LabelEncoder()
        self.furnish_encoder = LabelEncoder()
        
        self.lux_model = XGBRegressor(n_estimators=100, max_depth=6, learning_rate=0.1, random_state=42)
        self.prox_model = XGBRegressor(n_estimators=100, max_depth=6, learning_rate=0.1, random_state=42)
        self.models_dir = os.path.join(os.path.dirname(__file__), "saved_models")
        
        self.global_price_median = 0.0
        self.global_area_median = 0.0
        self.global_bedrooms_median = 2.0
        self.global_bathrooms_median = 2.0
        
        os.makedirs(self.models_dir, exist_ok=True)

    def load_pipeline(self):
        """Deserializes and loads the trained model pipeline and parameters."""
        pipeline_path = os.path.join(self.models_dir, "imputer_pipeline.pkl")
        if not os.path.exists(pipeline_path):
            raise FileNotFoundError(f"Model pipeline not found at {pipeline_path}. Please train the models first.")
            
        print(f"Loading trained pipelines and parameters from {pipeline_path}...")
        with open(pipeline_path, "rb") as f:
            artifacts = pickle.load(f)
            
        self.lux_model = artifacts["lux_model"]
        self.prox_model = artifacts["prox_model"]
        self.city_encoder = artifacts["city_encoder"]
        self.type_encoder = artifacts["type_encoder"]
        self.furnish_encoder = artifacts["furnish_encoder"]
        self.global_price_median = artifacts.get("global_price_median", 0.0)
        self.global_area_median = artifacts.get("global_area_median", 0.0)
        self.global_bedrooms_median = artifacts.get("global_bedrooms_median", 2.0)
        self.global_bathrooms_median = artifacts.get("global_bathrooms_median", 2.0)
        print("Model pipeline loaded successfully.")

    def fetch_training_seed(self):
        """Fetches labeled properties from the gold layer to use as training data."""
        query = "SELECT city, property_type, price, area_sqft, bedrooms, bathrooms, furnishing, luxury_score, proximity_score FROM gold.core_listings"
        with engine.connect() as conn:
            return pd.read_sql(text(query), conn)

    def fetch_unlabeled_silver(self):
        """Fetches silver properties that have not yet been processed/imputed into the gold layer."""
        query = """
            SELECT s.prop_id, s.prop_name, s.city, s.property_type, s.price, s.area_sqft, s.bedrooms, s.bathrooms, s.furnishing 
            FROM silver.properties s
            LEFT JOIN gold.core_listings g ON s.prop_id = g.prop_id
            WHERE g.prop_id IS NULL;
        """
        with engine.connect() as conn:
            return pd.read_sql(text(query), conn)

    def preprocess_dataframe(self, df, is_training=True):
        df = df.copy()
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['area_sqft'] = pd.to_numeric(df['area_sqft'], errors='coerce')
        
        df['bedrooms'] = df['bedrooms'].astype(str).str.extract(r'(\d+)')[0].astype(float)
        df['bathrooms'] = df['bathrooms'].astype(str).str.extract(r'(\d+)')[0].astype(float)
        
        if is_training:
            self.global_price_median = float(df['price'].median()) if not df['price'].isna().all() else 0.0
            self.global_area_median = float(df['area_sqft'].median()) if not df['area_sqft'].isna().all() else 0.0
            self.global_bedrooms_median = float(df['bedrooms'].median()) if not df['bedrooms'].isna().all() else 2.0
            self.global_bathrooms_median = float(df['bathrooms'].median()) if not df['bathrooms'].isna().all() else 2.0
            
        df['price'] = df['price'].fillna(self.global_price_median)
        df['area_sqft'] = df['area_sqft'].fillna(self.global_area_median)
        df['bedrooms'] = df['bedrooms'].fillna(self.global_bedrooms_median)
        df['bathrooms'] = df['bathrooms'].fillna(self.global_bathrooms_median)

        # Standardize strings
        df['city'] = df['city'].astype(str).str.upper()
        df['property_type'] = df['property_type'].astype(str)
        df['furnishing'] = df['furnishing'].astype(str)

        if is_training:
            df['city_encoded'] = self.city_encoder.fit_transform(df['city'])
            df['type_encoded'] = self.type_encoder.fit_transform(df['property_type'])
            df['furnish_encoded'] = self.furnish_encoder.fit_transform(df['furnishing'])
        else:
            # Safe OOV (Out-Of-Vocabulary) handling by mapping unknown classes to class[0]
            city_classes = set(self.city_encoder.classes_)
            type_classes = set(self.type_encoder.classes_)
            furnish_classes = set(self.furnish_encoder.classes_)

            df['city_encoded'] = df['city'].map(lambda s: s if s in city_classes else self.city_encoder.classes_[0])
            df['type_encoded'] = df['property_type'].map(lambda s: s if s in type_classes else self.type_encoder.classes_[0])
            df['furnish_encoded'] = df['furnishing'].map(lambda s: s if s in furnish_classes else self.furnish_encoder.classes_[0])
            
            df['city_encoded'] = self.city_encoder.transform(df['city_encoded'])
            df['type_encoded'] = self.type_encoder.transform(df['type_encoded'])
            df['furnish_encoded'] = self.furnish_encoder.transform(df['furnish_encoded'])

        return df
    
    def train_and_evaluate_imputer(self, min_r2=0.5):
        """Trains the model and evaluates it. Only saves if target R2 accuracy is met."""
        print(" Loading ground-truth seed dataset from AWS...")
        df_seed = self.fetch_training_seed()
        if df_seed.empty:
            print("No seed data found in gold.core_listings. Please process the initial seed first.")
            return False

        # Drop any records with missing target labels to prevent training crashes
        df_seed = df_seed.dropna(subset=['luxury_score', 'proximity_score'])
        if df_seed.empty:
            print("No valid target labels (luxury/proximity score) found in seed data. Cannot train.")
            return False

        df_train_full = self.preprocess_dataframe(df_seed, is_training=True)
        features = ['price', 'area_sqft', 'bedrooms', 'bathrooms', 'city_encoded', 'type_encoded', 'furnish_encoded']
        
        X = df_train_full[features]
        y_lux = df_train_full['luxury_score']
        y_prox = df_train_full['proximity_score']

        X_train, X_test, y_lux_train, y_lux_test, y_prox_train, y_prox_test = train_test_split(
            X, y_lux, y_prox, test_size=0.2, random_state=42
        )

        print("\nTraining and validating XGBoost Imputer models...")
        self.lux_model.fit(X_train, y_lux_train)
        self.prox_model.fit(X_train, y_prox_train)

        lux_preds = self.lux_model.predict(X_test)
        prox_preds = self.prox_model.predict(X_test)

        lux_r2 = r2_score(y_lux_test, lux_preds)
        prox_r2 = r2_score(y_prox_test, prox_preds)
        lux_mae = mean_absolute_error(y_lux_test, lux_preds)
        prox_mae = mean_absolute_error(y_prox_test, prox_preds)

        print("\n --- MODEL EVALUATION METRICS ---")
        print(f"Luxury Score    -> MAE: {lux_mae:.2f} | R2 Score: {lux_r2:.2f}")
        print(f"Proximity Score -> MAE: {prox_mae:.2f} | R2 Score: {prox_r2:.2f}")

        # Enforce accuracy constraint
        if lux_r2 < min_r2 or prox_r2 < min_r2:
            print(f"\n[FAIL] Accuracy check FAILED! Required R2 Score: {min_r2:.2f}")
            print("Aborting model saving and pipeline launch to prevent low-accuracy imputations.")
            return False

        print(f"\n[OK] Accuracy check PASSED! (Both models achieved R2 >= {min_r2:.2f})")
        print("Performing final model calibration over full seed data...")
        self.lux_model.fit(X, y_lux)
        self.prox_model.fit(X, y_prox)

        print(f" Saving trained pipelines to {self.models_dir}...")
        artifacts = {
            "lux_model": self.lux_model,
            "prox_model": self.prox_model,
            "city_encoder": self.city_encoder,
            "type_encoder": self.type_encoder,
            "furnish_encoder": self.furnish_encoder,
            "global_price_median": self.global_price_median,  
            "global_area_median": self.global_area_median,
            "global_bedrooms_median": self.global_bedrooms_median,
            "global_bathrooms_median": self.global_bathrooms_median
        }
        with open(os.path.join(self.models_dir, "imputer_pipeline.pkl"), "wb") as f:
            pickle.dump(artifacts, f)
        print(" Serialization artifacts completely secured.")
        return True
    
    def compute_and_upload_remaining(self, batch_size=500):
        self.load_pipeline()

        print("\n Fetching remaining unlabeled rows from Silver layer...")
        df_unlabeled = self.fetch_unlabeled_silver()
        total_rows = len(df_unlabeled)
        print(f" Found {total_rows} rows requiring imputation computation.")

        if total_rows == 0:
            print(" All warehouse data is already fully populated!")
            return

        df_proc = self.preprocess_dataframe(df_unlabeled, is_training=False)
        features = ['price', 'area_sqft', 'bedrooms', 'bathrooms', 'city_encoded', 'type_encoded', 'furnish_encoded']
        
        X_unlabeled = df_proc[features]
        print(" Executing fast local XGBoost matrix prediction...")
        pred_lux = np.clip(self.lux_model.predict(X_unlabeled), 1, 10).astype(int)
        pred_prox = np.clip(self.prox_model.predict(X_unlabeled), 1, 10).astype(int)

        df_unlabeled['luxury_score'] = pred_lux
        df_unlabeled['proximity_score'] = pred_prox

        print(f"Streaming processed rows up to AWS RDS in batches of {batch_size}...")
        db = SessionLocal()
        try:
            ml_features = []
            core_listings = []
            
            for idx, row in enumerate(df_unlabeled.to_dict(orient="records")):
                p_id = str(row['prop_id'])
                city = str(row['city']).upper()
                price = float(row['price']) if pd.notna(row['price']) else None
                area = float(row['area_sqft']) if pd.notna(row['area_sqft']) else None
                lux = int(row['luxury_score'])
                prox = int(row['proximity_score'])

                ml_features.append(GoldMLFeatureStore(
                    prop_id=p_id, city=city, normalized_price=price, area_sqft=area,
                    luxury_score=lux, proximity_score=prox, extracted_tags=json.dumps(["imputed_feature"])
                ))

                # Safe handling of missing values before converting to string (avoids saving string "nan" or "None")
                bedrooms = str(row['bedrooms']) if pd.notna(row['bedrooms']) else None
                bathrooms = str(row['bathrooms']) if pd.notna(row['bathrooms']) else None
                furnishing = str(row['furnishing']) if pd.notna(row['furnishing']) else None

                core_listings.append(GoldCoreListings(
                    prop_id=p_id, prop_name=row['prop_name'], city=city, property_type=row['property_type'],
                    price=price, area_sqft=area, bedrooms=bedrooms, bathrooms=bathrooms,
                    furnishing=furnishing, luxury_score=lux, proximity_score=prox
                ))

                if len(ml_features) == batch_size or (idx + 1) == total_rows:
                    db.bulk_save_objects(ml_features)
                    db.bulk_save_objects(core_listings)
                    db.commit()
                    ml_features.clear()
                    core_listings.clear()
                    print(f"[UPLOAD] Progress: Uploaded {idx + 1}/{total_rows} entries...")

            print("\n Recalculating Master Localized Analytics...")
            db.execute(text("DELETE FROM gold.localized_analytics;"))
            db.execute(text("""
                INSERT INTO gold.localized_analytics (city, total_listings, average_price, average_luxury_score)
                SELECT city, COUNT(*), AVG(price), AVG(luxury_score)
                FROM gold.core_listings GROUP BY city;
            """))
            db.commit()
            print("[SUCCESS] Gold Matrix fully filled and synchronized across all entries!")
        except Exception as e:
            print(f" Imputation batch run failed: {e}")
            db.rollback()
        finally:
            db.close()

    def run_imputation_flow(self, min_r2=0.5, batch_size=500):
        """
        Orchestrates training and imputation execution.
        First trains and validates the model. If the R2 accuracy is below min_r2,
        it aborts and refuses to compute or upload.
        """
        print(f"Starting complete imputation flow with min accuracy threshold (R2 >= {min_r2})...")
        success = self.train_and_evaluate_imputer(min_r2=min_r2)
        if not success:
            print("[FAIL] Pipeline execution ABORTED: The trained model did not achieve the required accuracy.")
            return False
            
        print("\n[RUN] Accuracy threshold satisfied. Launching imputation computation and database upload...")
        self.compute_and_upload_remaining(batch_size=batch_size)
        return True


if __name__ == "__main__":
    imputer = GoldFeatureImputer()
    # Execute full pipeline enforcing a minimum accuracy of R2 >= 0.50
    imputer.run_imputation_flow(min_r2=0.5)