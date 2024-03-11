from flask import Flask, Response, request
import json
from sklearn import datasets
from sklearn.decomposition import PCA
import pandas as pd
import numpy as np

app = Flask(__name__)
dr_methods = ['PCA', 'UMAP']

@app.route('/')
def hello():
    return 'Hello, World!'

@app.route('/dr-methods', methods=['GET'])
def list_dr_methods():
    resp = Response(response=json.dumps(dr_methods), status=200, mimetype="text/plain")
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp

@app.route('/iris-data-dr', methods=['GET'])
def getIrisDataDr():
    method = request.args.get('method', default="PCA", type = str)
    iris = datasets.load_iris()
    X = iris.data
    y = iris.target
    
    if method == "PCA":
        pca = PCA(n_components=2)
        pca.fit(X)
        X = pca.transform(X)
        print(type(X))
        X = X.tolist()
        #for item, idx in X:
        #    print(item)
        #    #item.append(y[idx])
        
        resp = Response(response=json.dumps(X), status=200, mimetype="text/plain")
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    else:
        return "Not available method " + method