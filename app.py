from flask import Flask, Response, request
from surrealdb import Surreal
import asyncio
import json
from sklearn import datasets
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import umap
import pandas as pd
import numpy as np

app = Flask(__name__)
dr_methods = ['PCA', 'UMAP']
brazilian_regions = {
    'Norte': ['AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO'], 
    'Nordeste': ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE'], 
    'Centro-Oeste': ['DF', 'GO', 'MS', 'MT'], 
    'Sudeste': ['ES', 'MG', 'RJ', 'SP'], 
    'Sul': ['PR', 'RS', 'SC']
    }

async def connect_db():
    async with Surreal("ws://localhost:8000/rpc") as db:
        #try:
            # Authentication
            await db.signin({"user": "root", "pass": "root"})
            await db.use("ensemble", "ensemble")
            # Check if there is an ensemble, case not, create ensembles
            ensemble_list = await db.select("ensemble")
            if (len(ensemble_list) == 0):
                print("Creating ensemble table")
                region_list = brazilian_regions.keys()
                for region in region_list:
                    await db.create(
                        "ensemble",
                        {
                            "name": region
                        }
                    )
            # Check if there is simulation for each ensemble, case not, create these simulations and relate them to an ensemble
            ensemble_record_list = await db.select("ensemble")
            for ensemble_record in ensemble_record_list:
                ensemble_simulation_list = await db.query("SELECT * FROM simulation WHERE ensemble = %s" % ensemble_record['id'])
                if (len(ensemble_simulation_list[0]['result']) == 0):
                    print("Creating simulations for ensemble", ensemble_record['name'])
                    for simulation in brazilian_regions[ensemble_record['name']]:
                        await db.create(
                            "simulation",
                            {
                                "name": simulation,
                                "ensemble": ensemble_record['id']
                            }
                        )
            print(await db.query(
                """
                SELECT name
                FROM simulation
                WHERE ensemble IN (
                  SELECT VALUE id
                  FROM ensemble
                  WHERE name="Sul"
                );
                """
            ))
            ensemble_data = loadBRStatesTaxRevenues()
            # Organizando, por enquanto, a questão de variáveis e tempo em duas tabelas: variables,
            # que contém a descrição de variáveis, e cell, com o valor das variáveis em um instante de tempo
            # (Depois será considerada a questão espacial, mas não para esse dataset usado de teste)
            variable_name_list = ensemble_data.columns.drop(['ensemble', 'time', 'name'])
            for variable_name in variable_name_list:
                # Check if the variable is added in the database, case not, add it
                db_variable = await db.query("SELECT * FROM variable WHERE name = \"%s\"" % variable_name)
                if (len(db_variable[0]['result']) == 0):
                    await db.create(
                        "variable",
                        {
                            "name": variable_name
                        }
                    )
            # TODO: Fazer a tabela dos dados com os seguintes atributos: simulation_id, variable_id, timestep e value.
            # Depois testar as consultas para verificar o quão fácil e custoso é fazer uma consulta.
        #except Surreal.


def loadBRStatesTaxRevenues():
    # inverse mapping
    state_region = {}
    for k,v in brazilian_regions.items():
        for x in v:
            state_region.setdefault(x, []).append(k)
    df = pd.read_csv('arrecadacao-por-estado.csv', sep=';').fillna(0.0)
    columns = df.columns.drop(['Ano', 'Mes', 'UF'])
    df[columns] = df[columns].apply(pd.to_numeric, errors='coerce')
    df['TOTAL ARRECADACAO'] = df[columns].sum(axis=1)
    df['Regiao'] = df['UF'].apply(lambda x: state_region[x][0])
    grouped = df.groupby(['Regiao', 'Ano', 'UF'], as_index=False).sum()
    print(grouped)
    grouped = grouped.drop(columns=['Mes'])
    grouped = grouped[grouped['Ano'] != 2024]
    grouped = grouped.rename(columns={'Regiao': 'ensemble', 'Ano': 'time', 'UF': 'name'})
    return grouped

asyncio.run(connect_db())
ensembleDataFrame = loadBRStatesTaxRevenues()


@app.route('/')
def hello():
    resp = Response(response=ensembleDataFrame.to_json(orient='index'), status=200, mimetype="text/plain")
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp

@app.route('/list-ensembles')
def list_ensembles():
    indexes = ['ensemble', 'name']
    df = ensembleDataFrame[ensembleDataFrame['time'] == 2023]
    df = df[indexes]
    #df['record_object'] = df['name'].apply(lambda row: json.dumps(row.to_dict()))
    grouped = df.groupby('ensemble')['name'].apply(lambda x: x.values.tolist())
    resp = Response(response=grouped.to_json(orient='index'), status=200, mimetype="text/plain")
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp

@app.route('/dr-methods', methods=['GET'])
def list_dr_methods():
    resp = Response(response=json.dumps(dr_methods), status=200, mimetype="text/plain")
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp

@app.route('/variables', methods=['GET'])
def list_variables():
    column_names = ensembleDataFrame.columns.values.tolist()
    column_names = [e for e in column_names if e not in ('ensemble', 'time', 'name')]
    resp = Response(response=json.dumps(column_names), status=200, mimetype="text/plain")
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp

@app.route('/dimensional-reduction', methods=['GET'])
def getEnsembleDR():
    method = request.args.get('method', default="PCA", type = str)
    indexes = ['ensemble', 'time', 'name']
    filtered_df = ensembleDataFrame[ensembleDataFrame['time'] == 2023]
    data = filtered_df.drop(columns=indexes)
    identifiers = filtered_df[indexes]

    if method == "PCA":
        pca = PCA(n_components=2)
        scaled_data = StandardScaler().fit_transform(data)
        df = pd.concat([identifiers, pd.DataFrame(pca.fit_transform(scaled_data), columns=['x', 'y'], index=data.index)], axis=1)
        keep_columns = ['name', 'x', 'y']
        df['record_object'] = df[keep_columns].apply(lambda row: json.dumps(row.to_dict()), axis=1)
        grouped = df.groupby('ensemble')['record_object'].apply(list)
        resp = Response(response=grouped.to_json(orient='index'), status=200, mimetype="text/plain")
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    elif method == "UMAP":
        reducer = umap.UMAP()
        scaled_data = StandardScaler().fit_transform(data)
        df = pd.concat([identifiers, pd.DataFrame(reducer.fit_transform(scaled_data), columns=['x', 'y'], index=data.index)], axis=1)
        keep_columns = ['name', 'x', 'y']
        df['record_object'] = df[keep_columns].apply(lambda row: json.dumps(row.to_dict()), axis=1)
        grouped = df.groupby('ensemble')['record_object'].apply(list)
        resp = Response(response=grouped.to_json(orient='index'), status=200, mimetype="text/plain")
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    # TODO: implement the same behavior above for UMAP

    resp = Response(response=data.to_json(orient='index'), status=200, mimetype="text/plain")
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp

@app.route('/temporal-evolution', methods=['GET'])
def temporalData():
    aggregate = request.args.get('aggregate', default=False, type = bool)
    variable = request.args.get('variable', default='', type = str)
    if variable == '':
        variable = ensembleDataFrame.columns.values.tolist()[-1]
    groupedStates = ensembleDataFrame.groupby(['ensemble', 'name'])[['time', variable]].apply(lambda x: x.values.tolist()).to_frame()
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