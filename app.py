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
    aggregate = request.args.get('aggregate', default=False, type = bool)
    brazilian_regions = {'Norte': ['AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO'], 'Nordeste': ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE'], 'Centro-Oeste':
                         ['DF', 'GO', 'MS', 'MT'], 'Sudeste': ['ES', 'MG', 'RJ', 'SP'], 'Sul': ['PR', 'RS', 'SC']}
    # inverse mapping
    state_region = {}
    for k,v in brazilian_regions.items():
        for x in v:
            state_region.setdefault(x, []).append(k)
    df = pd.read_csv('arrecadacao-por-estado.csv', sep=';').fillna(0.0)
    columns = df.columns.drop(['Ano', 'Mes', 'UF'])
    df[columns] = df[columns].apply(pd.to_numeric, errors='coerce')
    df['Total Arrecadacao'] = df[columns].sum(axis=1)
    df['Regiao'] = df['UF'].apply(lambda x: state_region[x][0])
    grouped = df.groupby(['Regiao', 'Ano', 'UF'], as_index=False)['Total Arrecadacao'].sum()
    grouped = grouped[grouped['Ano'] != 2024]
    grouped = grouped.rename(columns={'Regiao': 'ensemble', 'Ano': 'time', 'UF': 'name', 'Total Arrecadacao': 'value'})
    groupedStates = grouped.groupby(['ensemble', 'name'])[['time', 'value']].apply(lambda x: x.values.tolist()).to_frame()
    groupedStates.rename(columns={0: 'points'}, inplace=True)

    # Funcao para transformar o dataframe em um dict hierarquico do formato ensemble -> nome -> lista de pontos
    def nest(d: dict) -> dict:
        result = {}
        for key, value in d.items():
            target = result
            for k in key[:-1]:  # traverse all keys but the last
                target = target.setdefault(k, {})
            target[key[-1]] = value
        return result

    nested_dict = {k: nest(v) for k, v in groupedStates.to_dict().items()}

    # TODO: Definir como iremos agregar os dados (estatística? probabilidade?)
    if aggregate:
        return "WIP"
    else:
        resp = Response(response=json.dumps(nested_dict['points']), status=200, mimetype="text/plain")
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp

