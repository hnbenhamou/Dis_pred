import joblib
import yfinance as yf
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from flask import Flask, request, jsonify
import DbConnect as db
from flask_cors import CORS
import logging

# Initialiser les journaux
logging.basicConfig(level=logging.DEBUG)

database = db.DbConnect()

def predict_stock_trade_decision(model, today_features):
    prediction = model.predict([today_features])
    logging.debug(f"Prédiction : {prediction}")
    if prediction[0] == 1:
        return "Open"
    else:
        return "Close"

def train_and_save_model(stock_symbol, start_date, end_date, model_filename):
    logging.debug(f"Entraînement du modèle pour {stock_symbol} du {start_date} au {end_date}")
    stock_data = yf.download(stock_symbol, start=start_date, end=end_date)
    df = pd.DataFrame(stock_data)
    
    short_window = 20
    long_window = 50
    df['Short_MA'] = df['Close'].rolling(window=short_window).mean()
    df['Long_MA'] = df['Close'].rolling(window=long_window).mean()
    df['Label'] = 0
    df['Label'][short_window:] = np.where(df['Short_MA'][short_window:] > df['Long_MA'][short_window:], 1, 0)
    df.dropna(inplace=True)
    
    X = df[['Open', 'High', 'Low', 'Close', 'Volume', 'Short_MA', 'Long_MA']]
    y = df['Label']
    
    model = LogisticRegression()
    model.fit(X, y)
    
    joblib.dump(model, model_filename)
    logging.debug(f"Modèle sauvegardé sous {model_filename}")

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})
model_filename = 'stock_prediction_model.pkl'

@app.route('/train', methods=['POST'])
def train_model_endpoint():
    try:
        data = request.json
        stock_symbol = data['stock_symbol']
        start_date = data['start_date']
        end_date = data['end_date']
        
        logging.debug(f"Requête de formation reçue pour {stock_symbol} du {start_date} au {end_date}")
        train_and_save_model(stock_symbol, start_date, end_date, model_filename)
        
        return jsonify({'message': f"Modèle formé et sauvegardé sous {model_filename}"})
    except Exception as e:
        logging.error(f"Erreur pendant l'entraînement : {str(e)}")
        database.insert_one({'error': str(e)})
        return jsonify({'error': str(e)})


@app.route('/predict', methods=['POST'])
def predict_endpoint():
    try:
        data = request.json
        today_features = data['features']
        
        logging.debug(f"Les caractéristiques d'aujourd'hui : {today_features}")
        
        model = joblib.load(model_filename)
        logging.debug(f"Modèle chargé : {model}")
        
        prediction = predict_stock_trade_decision(model, today_features)
        logging.debug(f"Résultat de la prédiction : {prediction}")
        
        response = {'prediction': prediction}
        return jsonify(response)
    except Exception as e:
        logging.error(f"Erreur pendant la prédiction : {str(e)}")
        return jsonify({'error': str(e)})


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
