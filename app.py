from flask import Flask, Response, request
from dotenv import dotenv_values
from surrealdb import Surreal
import pymonetdb
import asyncio
import json
from sklearn import datasets
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import umap
import pandas as pd
import numpy as np
from model import Ensemble, Simulation, Variable, CellData

app = Flask(__name__)
dr_methods = ['PCA', 'UMAP']
brazilian_regions = {
    'Norte': ['AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO'], 
    'Nordeste': ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE'], 
    'Centro-Oeste': ['DF', 'GO', 'MS', 'MT'], 
    'Sudeste': ['ES', 'MG', 'RJ', 'SP'], 
    'Sul': ['PR', 'RS', 'SC']
    }

def create_dataframe_all_ensembles():
    dfColumns =['ensemble', 'name', 'time']
    ensembleModel = Ensemble.Ensemble()
    simulationModel = Simulation.Simulation()
    variableModel = Variable.Variable()
    cellDataModel = CellData.CellData()
    allVariableRecords = variableModel.read_all()
    variableRecordsDict = dict((x, y) for x, y in allVariableRecords)
    #print(variableRecordsDict)
    dfColumns = dfColumns + list(variableRecordsDict.values())
    allEnsembleRecords = ensembleModel.read_all()
    ensembleRecordsDict = dict((x, y) for x, y in allEnsembleRecords)
    allSimulationRecords = simulationModel.read_all()
    allSimulationList = []
    for item in allSimulationRecords:
        allSimulationList.append((item[0], item[1], ensembleRecordsDict[item[2]]))
    #print(allSimulationList)
    simulationRecordsDict = dict((x, [y, z]) for x, y, z in allSimulationList)
    #print(simulationRecordsDict)
    allCellDataRecords = cellDataModel.read_all()
    allCellDataList = []
    for item in allCellDataRecords:
        allCellDataList.append((simulationRecordsDict[item[2]][1], simulationRecordsDict[item[2]][0], float(item[1]), variableRecordsDict[item[3]], float(item[4])))
    #print(allCellDataList)
    timestepList = list(map(lambda x: float(x[0]), cellDataModel.get_timesteps()))
    columnSize = len(dfColumns)
    rowSize = len(allSimulationList) * len(timestepList)
    data = np.zeros((rowSize, columnSize), dtype=object)
    rowIdx = 0
    for simItem in list(simulationRecordsDict.values()):
        for timestep in timestepList:
            # TODO: Preencher inicialmente o data com os timesteps, estados e regioes, depois fazer um array preenchendo as variáveis
            result = cellDataModel.get_celldata_all_variables(simItem[0], timestep)
            data[rowIdx, 0] = simItem[1]
            data[rowIdx, 1] = simItem[0]
            data[rowIdx, 2] = timestep
            for cellData in result:
                data[rowIdx, dfColumns.index(cellData[2])] = float(cellData[4])
            rowIdx = rowIdx + 1
    return pd.DataFrame(data=data, columns=dfColumns)
    





ensembleDataFrame = create_dataframe_all_ensembles()
#connect_monet_db()
#asyncio.run(connect_monet_db())
#ensembleDataFrame = loadBRStatesTaxRevenues()


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
    ensemble_list = request.args.getlist('ensemble')
    simulation_list = request.args.getlist('simulation')
    print(ensemble_list)
    print(simulation_list)
    indexes = ['ensemble', 'time', 'name']
    filtered_df = ensembleDataFrame[ensembleDataFrame['time'] == 2023]
    if(len(ensemble_list) != 0):
        filtered_df = filtered_df[filtered_df['ensemble'].isin(ensemble_list)]
    if(len(simulation_list) != 0):
        filtered_df = filtered_df[filtered_df['name'].isin(simulation_list)]
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
    ensemble_list = request.args.getlist('ensemble')
    simulation_list = request.args.getlist('simulation')
    filtered_df = ensembleDataFrame
    if variable == '':
        variable = ensembleDataFrame.columns.values.tolist()[-1]
    if(len(ensemble_list) != 0):
        filtered_df = filtered_df[filtered_df['ensemble'].isin(ensemble_list)]
    if(len(simulation_list) != 0):
        filtered_df = filtered_df[filtered_df['name'].isin(simulation_list)]
    groupedStates = filtered_df.groupby(['ensemble', 'name'])[['time', variable]].apply(lambda x: x.values.tolist()).to_frame()
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