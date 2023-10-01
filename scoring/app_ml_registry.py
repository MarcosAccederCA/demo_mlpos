

from mlreg_client import Ml_registry

from flask import Flask, request, jsonify
import pickle

import pandas as pd

import json

app = Flask(__name__)


@app.route("/metadata", methods={'GET'})
def metadata():
    return jsonify({'desc' : desc})


@app.route("/", methods=['GET'])
def api():

    _data = json.loads(request.data)

    #Remplace missing values by None, otherwise Pandas will treat them as string
    data = {k: None if not v else v for k, v in _data.items() }

    df= pd.DataFrame(data, index=[0])

    prediction = model.predict_proba(df)[0][1]
    

    print(prediction)

    response = {'prob' : prediction}

    return jsonify(response)

if __name__ == "__main__":
    MODEL_NAME = "Logistic Pipeline"

    ml_r = Ml_registry(company_id=3, user_id=1,
                       bucket_name='modelfactorymarcos',
                       bucket_folder = 'demo_septiembre',
                       client_secret='../secret/client_secret.json'
                       )  
    model_id = ml_r.get_model_by_name(MODEL_NAME).get('data').get('id')

    # descargar el modelo del ML Registry
    model, desc = ml_r.load_model_version(model_id,status='PROD', filename='model.pkl')
    app.run(debug=True)
