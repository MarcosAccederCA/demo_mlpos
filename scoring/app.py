from flask import Flask, request, jsonify

import pandas as pd
import json
import pickle


app = Flask(__name__)


@app.route('/', methods=['GET'])
def score():

    
    _data = json.loads(request.data)

    #Remplace missing values by None, otherwise Pandas will treat them as string
    #and the model will not work
    data = {k: None if not v else v for k, v in _data.items() }
    
    df = pd.DataFrame(data,index=[0])
    print(df)
    # llamar el modelo

    prob = model.predict_proba(df)[0][1]

    

    response = {'prob' : prob}
    print(response)

    return jsonify(response)


if __name__ == '__main__':

    MODEL_PICKLE_NAME = "model.pickle"
    file = open(MODEL_PICKLE_NAME, 'rb')
    model = pickle.load(file)



    app.run(debug=True)