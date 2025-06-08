import os
from google.cloud import storage
import joblib
from flask import Flask, request, jsonify
import logging

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r''
# os.environ['AIP_STORAGE_URI'] = 'gs://.../pipeline_root/XXX/e-commerce-analysis-XXX/rfc-training_XXX/'
# os.environ['AIP_HEALTH_ROUTE'] = '/health'
# os.environ['AIP_PREDICT_ROUTE'] = '/predictions'

try:
    print(f"AIP_STORAGE_URI: {os.environ['AIP_STORAGE_URI']}")
    logging.basicConfig(level = logging.INFO)
    logging.info(f"AIP_STORAGE_URI: {os.environ['AIP_STORAGE_URI']}")
except:
    pass

client = storage.Client(project = '')

with open('model.joblib', 'wb') as file:
    client.download_blob_to_file(os.environ['AIP_STORAGE_URI'] + '/model.joblib', file)
model = joblib.load('model.joblib')

app = Flask(__name__)

HEALTH_ROUTE = os.environ['AIP_HEALTH_ROUTE']
PREDICTIONS_ROUTE = os.environ['AIP_PREDICT_ROUTE']

@app.route(HEALTH_ROUTE, methods = ['GET'])
def get_health():
    return jsonify({
        'status': 'healthy!'
    })

@app.route(PREDICTIONS_ROUTE, methods = ['POST'])
def get_predictions():
    try:
        # Parsing the request body.
        body = request.get_json()
        X_test = body['instances']
        y_predicted = model.predict(X_test)
        return jsonify({
            'predictions': y_predicted.tolist() # Object of type ndarray is not JSON serializable.
        })
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 400

if __name__ == '__main__':
    # 0.0.0.0 means the Flask app will listen on all available network interfaces, so it accepts traffic from any IP.
    # In Docker, 0.0.0.0 is used so the app is accessible from outside the container.
    app.run(host = '0.0.0.0', port = 8080, debug = True)