# %%
import flask
import mlflow
import pandas as pd

mlflow.set_tracking_uri("http://127.0.0.1:5000")

# %%
models = mlflow.search_registered_models(filter_string="name='f1_driver_champion'")[-1]
last_version = max([int(i.version) for i in models.latest_versions])
MODEL = mlflow.sklearn.load_model(f"models:/f1_driver_champion/{last_version}")
# %%

app = flask.Flask(__name__)

@app.route('/health_check')
def health_check():
    return "OK", 200

@app.route('/predict', methods=['POST'])
def predict():
    payload = flask.request.get_json()
    data = payload.get('data', [])

    if len(data) == 0:
        return {'error': 'no features provided'}, 400
    
    df = pd.DataFrame(data)
    X = df[MODEL.feature_names_in_]
    proba = MODEL.predict_proba(X)[:,1]

    df['proba'] = proba
    payload = df[['id', 'proba']].to_dict(orient='records')

    return {'data':payload}, 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)