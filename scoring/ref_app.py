from flask import Flask, request, jsonify
import pickle
import pandas as pd
import json


def create_app()
    
        #Cargar el modelo en memoria cuando se enciende el app
    MODEL_PICKLE_NAME = "scoring/model.pickle"
    file = open(MODEL_PICKLE_NAME, 'rb')
    model = pickle.load(file)

    app = Flask(__name__)
    return app

app = create_app()


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





    app.run(debug=True)