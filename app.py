from flask import Flask, Response, request
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

app = Flask(__name__)
dr_methods = ['PCA', 'UMAP']
brazilian_regions = {
    'Norte': ['AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO'], 
    'Nordeste': ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE'], 
    'Centro-Oeste': ['DF', 'GO', 'MS', 'MT'], 
    'Sudeste': ['ES', 'MG', 'RJ', 'SP'], 
    'Sul': ['PR', 'RS', 'SC']
    }

def connect_monet_db():
    try:
        with pymonetdb.connect(username="monetdb", password="monetdb", hostname="localhost", database="ensemble") as db:
            cursor = db.cursor()
            ensemble_data = loadBRStatesTaxRevenues()
            ensemble_list = ensemble_data['ensemble'].unique()
            ensemble_id_map = {}
            simulation_list = ensemble_data['name'].unique()
            simulation_id_map = {}
            variable_list = ensemble_data.columns.drop(['ensemble', 'time', 'name'])
            variable_id_map = {}
            cursor.execute("CREATE TABLE IF NOT EXISTS ensemble (id UUID NOT NULL PRIMARY KEY, name VARCHAR(200) NOT NULL)")
            cursor.execute("CREATE TABLE IF NOT EXISTS simulation (id UUID NOT NULL PRIMARY KEY, name VARCHAR(200) NOT NULL, ensemble_id UUID NOT NULL, FOREIGN KEY(ensemble_id) REFERENCES ensemble(id))")
            cursor.execute("CREATE TABLE IF NOT EXISTS variable (id UUID NOT NULL PRIMARY KEY, name VARCHAR(200) NOT NULL)")
            cursor.execute("""
                           CREATE TABLE IF NOT EXISTS cell (
                               id UUID NOT NULL PRIMARY KEY, 
                               timestep DECIMAL NOT NULL,
                               simulation_id UUID NOT NULL,
                               variable_id UUID NOT NULL,
                               value DECIMAL NOT NULL,
                               FOREIGN KEY(simulation_id) REFERENCES simulation(id),
                               FOREIGN KEY(variable_id) REFERENCES variable(id)
                           )
                           """)
            db.commit()
            for ensemble_name in ensemble_list:
                query_ensemble = cursor.execute("SELECT * FROM ensemble WHERE name = \'%s\'" % ensemble_name)
                if (query_ensemble == 0):
                    cursor.execute("SELECT sys.uuid() AS uuid")
                    uuid = cursor.fetchone()[0]
                    new_record_result = cursor.execute("INSERT INTO ensemble (id, name) VALUES (uuid\'%s\', \'%s\')" % (uuid, ensemble_name))
                    if(new_record_result == 1):
                        ensemble_id_map[ensemble_name] = uuid
                else:
                    ensemble_id = cursor.fetchone()[0]
                    ensemble_id_map[ensemble_name] = ensemble_id
            for simulation_name in simulation_list:
                query_simulation = cursor.execute("SELECT * FROM simulation WHERE name = \'%s\'" % simulation_name)
                if (query_simulation == 0):
                    ensemble_from_simulation = ensemble_data.loc[ensemble_data['name'] == simulation_name]['ensemble'].iloc[0]
                    ensemble_id = ensemble_id_map[ensemble_from_simulation]
                    cursor.execute("SELECT sys.uuid() AS uuid")
                    uuid = cursor.fetchone()[0]
                    new_record_result = cursor.execute("INSERT INTO simulation (id, name, ensemble_id) VALUES (uuid\'%s\', \'%s\', uuid\'%s\')" % (uuid, simulation_name, ensemble_id))
                    if(new_record_result == 1):
                        simulation_id_map[simulation_name] = uuid
                else:
                    simulation_id = cursor.fetchone()[0]
                    simulation_id_map[simulation_name] = simulation_id
            for variable_name in variable_list:
                query_variable = cursor.execute("SELECT * FROM variable WHERE name = \'%s\'" % variable_name)
                if (query_variable == 0):
                    cursor.execute("SELECT sys.uuid() AS uuid")
                    uuid = cursor.fetchone()[0]
                    new_record_result = cursor.execute("INSERT INTO variable (id, name) VALUES (uuid\'%s\', \'%s\')" % (uuid, variable_name))
                    if(new_record_result == 1):
                        variable_id_map[variable_name] = uuid
                else:
                    variable_id = cursor.fetchone()[0]
                    variable_id_map[variable_name] = variable_id
            for index, row in ensemble_data.iterrows():
                for variable_name in variable_list:
                    simulation_id = simulation_id_map[row['name']]
                    variable_id = variable_id_map[variable_name]
                    query_cell = cursor.execute(
                        """
                        SELECT *
                        FROM cell
                        WHERE simulation_id = uuid\'%s\'
                        AND variable_id = uuid\'%s\'
                        AND timestep = %s
                        """
                        % (simulation_id, variable_id, row['time'])
                    )
                    if (query_cell == 0):
                        cursor.execute("SELECT sys.uuid() AS uuid")
                        uuid = cursor.fetchone()[0]
                        print(uuid)
                        new_record_result = cursor.execute(
                            """
                            INSERT INTO cell (id, simulation_id, variable_id, timestep, value) 
                            VALUES (uuid\'%s\', uuid\'%s\', uuid\'%s\', %s, %s)
                            """
                            % (uuid, simulation_id, variable_id, row['time'], row[variable_name])
                        )
            db.commit()
    except Exception as e:
        print(e)

async def connect_surreal_db():
    async with Surreal("ws://localhost:8000/rpc") as db:
        #try:
            # Authentication
            await db.signin({"user": "root", "pass": "root"})
            await db.use("ensemble", "ensemble")
            ensemble_data = loadBRStatesTaxRevenues()
            ensemble_list = ensemble_data['ensemble'].unique()
            ensemble_id_map = {}
            simulation_list = ensemble_data['name'].unique()
            simulation_id_map = {}
            variable_list = ensemble_data.columns.drop(['ensemble', 'time', 'name'])
            variable_id_map = {}
            # Defining tables
            for ensemble_name in ensemble_list:
                db_ensemble = await db.query("SELECT VALUE id FROM ensemble WHERE name = \"%s\"" % ensemble_name)
                if (len(db_ensemble[0]['result']) == 0):
                    created_ensemble_record = await db.create(
                        "ensemble",
                        {
                            "name": ensemble_name
                        }
                    )
                    print(created_ensemble_record)
                    ensemble_id_map[ensemble_name] = created_ensemble_record[0]['id']
                else:
                    ensemble_id = db_ensemble[0]['result'][0]
                    ensemble_id_map[ensemble_name] = ensemble_id
            for simulation_name in simulation_list:
                db_simulation = await db.query("SELECT VALUE id FROM simulation WHERE name = \"%s\"" % simulation_name)
                if (len(db_simulation[0]['result']) == 0):
                    ensemble_from_simulation = ensemble_data.loc[ensemble_data['name'] == simulation_name]['ensemble'].iloc[0]
                    ensemble_id = ensemble_id_map[ensemble_from_simulation]
                    print(ensemble_id)
                    created_simulation_record = await db.create(
                        "simulation",
                        {
                            "name": simulation_name,
                            "ensemble": ensemble_id
                        }
                    )
                    simulation_id_map[simulation_name] = created_simulation_record[0]['id']
                else:
                    simulation_id = db_simulation[0]['result'][0]
                    simulation_id_map[simulation_name] = simulation_id
            for variable_name in variable_list:
                db_variable = await db.query("SELECT VALUE id FROM variable WHERE name = \"%s\"" % variable_name)
                if (len(db_variable[0]['result']) == 0):
                    created_variable_record = await db.create(
                        "variable",
                        {
                            "name": variable_name,
                        }
                    )
                    variable_id_map[variable_name] = created_variable_record[0]['id']
                else:
                    variable_id = db_variable[0]['result'][0]
                    variable_id_map[variable_name] = variable_id
            await db.query(
                """
                DEFINE TABLE IF NOT EXISTS ensemble;
                DEFINE TABLE IF NOT EXISTS simulation;
                DEFINE TABLE IF NOT EXISTS variable;
                DEFINE TABLE IF NOT EXISTS cell;
                DEFINE FIELD name ON TABLE ensemble TYPE string;
                DEFINE FIELD name ON TABLE simulation TYPE string;
                DEFINE FIELD ensemble_id ON TABLE simulation TYPE record<ensemble>;
                DEFINE FIELD name ON TABLE variable TYPE string;
                DEFINE FIELD simulation_id ON TABLE cell TYPE record<simulation>;
                DEFINE FIELD variable_id ON TABLE cell TYPE record<variable>;
                DEFINE FIELD timestep ON TABLE cell TYPE decimal;
                DEFINE FIELD value ON TABLE cell TYPE decimal;
                """)
            # Organizando, por enquanto, a questão de variáveis e tempo em duas tabelas: variables,
            # que contém a descrição de variáveis, e cell, com o valor das variáveis em um instante de tempo
            # (Depois será considerada a questão espacial, mas não para esse dataset usado de teste)
            for index, row in ensemble_data.iterrows():
                for variable_name in variable_list:
                    db_cell = await db.query(
                        """
                        SELECT *
                        FROM cell
                        WHERE simulation_id IN (
                          SELECT VALUE id
                          FROM simulation
                          WHERE name=\"%s\"
                        )
                        AND variable_id IN (
                          SELECT VALUE id
                          FROM variable
                          WHERE name=\"%s\"
                        )
                        AND timestep=%s;
                        """
                        % (row['name'], variable_name, row['time'])
                    )
                    if (len(db_cell[0]['result']) == 0):
                        simulation_id = simulation_id_map[simulation_name]
                        variable_id = variable_id_map[variable_name]
                        print("Adding cell with simulation_id %s, variable_id %s and timestep %s" % (row['name'], variable_name, row['time']))
                        await db.create(
                            "cell",
                            {
                                "simulation_id": simulation_id,
                                "variable_id": variable_id,
                                "timestep": row['time'],
                                "value": row[variable_name]
                            }
                        )


            #variable_name_list = ensemble_data.columns.drop(['ensemble', 'time', 'name'])
            #for index, row in ensemble_data.iterrows():
            #    for variable_name in variable_name_list:
            #        # Check if the variable is added in the database, case not, add it
            #        db_variable = await db.query("SELECT * FROM variable WHERE name = \"%s\"" % variable_name)
            #        if (len(db_variable[0]['result']) == 0):
            #            await db.create(
            #                "variable",
            #                {
            #                    "name": variable_name
            #                }
            #            )
            #        db_cell = await db.query(
            #            """
            #            SELECT *
            #            FROM cell
            #            WHERE simulation_id IN (
            #              SELECT VALUE id
            #              FROM simulation
            #              WHERE name=\"%s\"
            #            )
            #            AND variable_id IN (
            #              SELECT VALUE id
            #              FROM variable
            #              WHERE name=\"%s\"
            #            )
            #            AND timestep=%s;
            #            """
            #            % (row['name'], variable_name, row['time'])
            #        )
            #        if (len(db_cell[0]['result']) == 0):
            #            simulation_id_query = await db.query("SELECT VALUE id FROM simulation WHERE name=\"%s\"" % row['name'])
            #            variable_id_query = await db.query("SELECT VALUE id FROM variable WHERE name=\"%s\"" % variable_name)
            #            simulation_id = simulation_id_query[0]['result'][0]
            #            variable_id = variable_id_query[0]['result'][0]
            #            print("Adding cell with simulation_id %s, variable_id %s and timestep %s" % (row['name'], variable_name, row['time']))
            #            await db.create(
            #                "cell",
            #                {
            #                    "simulation_id": simulation_id,
            #                    "variable_id": variable_id,
            #                    "timestep": row['time'],
            #                    "value": row[variable_name]
            #                }
            #            )

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

connect_monet_db()
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