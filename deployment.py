import azure.functions as func
import joblib 
import pandas as pd
import requests
from datetime import datetime, timedelta
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder
from sklearn.inspection import permutation_importance
import io
import pandas as pd
app = func.FunctionApp()

@app.time_trigger(schedule="0 0 8 * * *",
                  arg_name="myTimer")
def daily_prediction(myTimer:func.TimerRequest) -> None:
    url_weather = "https://www.data.gouv.fr/api/1/datasets/r/e5d64034-f9b3-415c-a7f1-abafb23b89bc"
    response = requests.get(url_weather)
    weather = pd.read_csv(io.BytesIO(response.content), delimiter=';', compression='gzip')

    url_trafic = "URL"
    trafic = pd.read_csv()



#premierement 
# il faut imporer les prevision meteo aussi

# @app.timer_trigger(schedule="0 0 8 * * *", 
#                    arg_name="myTimer")
# def daily_prediction(myTimer: func.TimerRequest) -> None:
#     """Fait les prédictions pour aujourd'hui/demain"""
    
#     # 1. Récupère les données via tes APIs
#     traffic_data = fetch_from_api("https://ton-api.com/traffic")
#     weather_data = fetch_from_api("https://ton-api.com/weather")
#     holidays = fetch_from_database("SELECT * FROM vacances WHERE date >= TODAY()")
    
#     # 2. Charge le modèle
#     model = joblib.load('traffic_model.pkl')
    
#     # 3. Prépare les features (comme dans ton notebook)
#     features = prepare_features(traffic_data, weather_data, holidays)
    
#     # 4. Prédictions
#     predictions = model.predict(features)
    
#     # 5. Sauvegarde
#     save_predictions(predictions)
    
#     print(f"✅ Prédictions du {datetime.now().date()} terminées")


# # ===== FONCTION 2 : Réentraînement hebdomadaire =====
# @app.timer_trigger(schedule="0 0 2 * * SUN",  # Dimanche 2h du matin
#                    arg_name="myTimer")
# def weekly_retrain(myTimer: func.TimerRequest) -> None:
#     """Réentraîne le modèle avec les nouvelles données"""
    
#     # 1. Récupère données historiques (30 derniers jours par ex)
#     start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
#     traffic_hist = fetch_from_api(f"https://ton-api.com/traffic?start={start_date}")
#     weather_hist = fetch_from_api(f"https://ton-api.com/weather?start={start_date}")
#     holidays = fetch_from_database(f"SELECT * FROM vacances WHERE date >= '{start_date}'")
    
#     # 2. Prépare les données
#     X, y = prepare_training_data(traffic_hist, weather_hist, holidays)
    
#     # 3. Entraîne XGBoost
#     from xgboost import XGBRegressor
    
#     model = XGBRegressor(
#         n_estimators=100,
#         learning_rate=0.1,
#         max_depth=5
#         # Tes hyperparamètres
#     )
#     model.fit(X, y)
    
#     # 4. Sauvegarde le nouveau modèle
#     joblib.dump(model, 'traffic_model.pkl')
    
#     # Upload vers Blob Storage pour versioning
#     upload_model_to_blob(model, version=datetime.now().strftime('%Y%m%d'))
    
#     print(f"✅ Modèle réentraîné le {datetime.now()}")


# # ===== FONCTIONS UTILITAIRES =====

# def fetch_from_api(url):
#     """Récupère données depuis ton API"""
#     headers = {"Authorization": "Bearer TON_TOKEN"}  # Si besoin
#     response = requests.get(url, headers=headers)
#     return response.json()


# def fetch_from_database(query):
#     """Récupère données vacances depuis ta DB"""
#     import pyodbc  # ou psycopg2 selon ta DB
    
#     conn_str = "Driver={ODBC Driver 17 for SQL Server};Server=ton-server.database.windows.net;Database=traffic_db;Uid=user;Pwd=pass;"
#     conn = pyodbc.connect(conn_str)
#     df = pd.read_sql(query, conn)
#     conn.close()
#     return df


# def prepare_features(traffic_data, weather_data, holidays):
#     """Transforme les données comme dans ton notebook"""
    
#     df = pd.DataFrame()
    
#     # Features trafic historique (lags, rolling means, etc.)
#     df['traffic_lag_1h'] = # ...
#     df['traffic_lag_24h'] = # ...
#     df['traffic_rolling_mean_7d'] = # ...
    
#     # Features météo
#     df['temperature'] = weather_data['temp']
#     df['precipitation'] = weather_data['rain']
#     df['wind_speed'] = weather_data['wind']
    
#     # Features temporelles
#     df['hour'] = pd.to_datetime(weather_data['timestamp']).dt.hour
#     df['day_of_week'] = pd.to_datetime(weather_data['timestamp']).dt.dayofweek
#     df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    
#     # Features vacances
#     df['is_holiday'] = df['date'].isin(holidays['date']).astype(int)
    
#     return df


# def prepare_training_data(traffic_hist, weather_hist, holidays):
#     """Prépare X et y pour l'entraînement"""
#     features = prepare_features(traffic_hist, weather_hist, holidays)
    
#     X = features.drop('traffic_actual', axis=1)  # Tes features
#     y = features['traffic_actual']  # Ta target
    
#     return X, y


# def save_predictions(predictions):
#     """Sauvegarde dans Blob ou DB"""
    
#     # Option 1: Blob Storage (simple)
#     from azure.storage.blob import BlobServiceClient
    
#     blob_service = BlobServiceClient.from_connection_string("CONNECTION_STRING")
    
#     result = pd.DataFrame({
#         'timestamp': pd.date_range(start='now', periods=len(predictions), freq='H'),
#         'predicted_traffic': predictions
#     })
    
#     blob_client = blob_service.get_blob_client(
#         container="predictions",
#         blob=f"pred_{datetime.now().date()}.csv"
#     )
#     blob_client.upload_blob(result.to_csv(index=False), overwrite=True)
    
#     # Option 2: Sauvegarder dans ta DB
#     # conn = pyodbc.connect(conn_str)
#     # result.to_sql('predictions', conn, if_exists='append', index=False)


# def upload_model_to_blob(model, version):
#     """Sauvegarde versions du modèle"""
#     from azure.storage.blob import BlobServiceClient
    
#     blob_service = BlobServiceClient.from_connection_string("CONNECTION_STRING")
#     blob_client = blob_service.get_blob_client(
#         container="models",
#         blob=f"traffic_model_{version}.pkl"
#     )
    
#     import io
#     buffer = io.BytesIO()
#     joblib.dump(model, buffer)
#     buffer.seek(0)
#     blob_client.upload_blob(buffer, overwrite=True)