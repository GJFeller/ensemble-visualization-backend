from flask import Flask, Response, request
import json
from sklearn import datasets
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import umap
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
        df = pd.DataFrame(X, columns=['x', 'y'])
        df['set'] = y
        df['set'] = df['set'].astype('string')
        df['set'] = df['set'].map({'0': 'Setosa', '1': 'Versicolour', '2': 'Virginica'})
        df['point'] = list(zip(df.x, df.y))
        grouped = df.groupby('set')['point'].apply(list)
        
        resp = Response(response=grouped.to_json(orient='index'), status=200, mimetype="text/plain")
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    elif method == "UMAP":
        reducer = umap.UMAP()
        scaled_X = StandardScaler().fit_transform(X)
        embedding = reducer.fit_transform(scaled_X)
        df = pd.DataFrame(embedding, columns=['x', 'y'])
        df['set'] = y
        df['set'] = df['set'].astype('string')
        df['set'] = df['set'].map({'0': 'Setosa', '1': 'Versicolour', '2': 'Virginica'})
        df['point'] = list(zip(df.x, df.y))
        grouped = df.groupby('set')['point'].apply(list)

        resp = Response(response=grouped.to_json(orient='index'), status=200, mimetype="text/plain")
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    else:
        return "Not available method " + method
    
@app.route('/arrecadacao', methods=['GET'])
def getArrecadacao():
    df = pd.read_csv('arrecadacao-por-estado.csv', sep=';').fillna(0.0)
    columns = df.columns.drop(['Ano', 'Mes', 'UF'])
    df[columns] = df[columns].apply(pd.to_numeric, errors='coerce')
    df['Total Arrecadacao'] = df[columns].sum(axis=1)
    grouped = df.groupby(['Ano', 'UF'], as_index=False)['Total Arrecadacao'].sum()
    grouped = grouped[grouped['Ano'] != 2024]
    #grouped['Total Arrecadacao'] = grouped['Total Arrecadacao'].apply(lambda x: '{:,.2f}'.format(x))
    print(grouped)

    resp = Response(response=grouped.to_json(orient='records'), status=200, mimetype="text/plain")
    return resp