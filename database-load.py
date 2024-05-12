from dotenv import dotenv_values
from model.Ensemble import Ensemble
from surrealdb import Surreal
import pymonetdb
import asyncio
import model
import pandas as pd
import numpy as np

config = dotenv_values(".env")
print(config)
print(config["DB_DRIVER"])

brazilian_regions = {
    'Norte': ['AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO'], 
    'Nordeste': ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE'], 
    'Centro-Oeste': ['DF', 'GO', 'MS', 'MT'], 
    'Sudeste': ['ES', 'MG', 'RJ', 'SP'], 
    'Sul': ['PR', 'RS', 'SC']
    }

def loadBRStatesTaxRevenues():
    # inverse mapping
    state_region = {}
    for k,v in brazilian_regions.items():
        for x in v:
            state_region.setdefault(x, []).append(k)
    df = pd.read_csv(config["DATA_FILENAME"], sep=';').fillna(0.0)
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

def loadDataIntoDatabase(ensemble_data):
    ensemble_list = ensemble_data['ensemble'].unique()
    ensemble_id_map = {}
    simulation_list = ensemble_data['name'].unique()
    simulation_id_map = {}
    variable_list = ensemble_data.columns.drop(['ensemble', 'time', 'name'])
    variable_id_map = {}
    ensemble_model = Ensemble()
    # TODO: Create one model for each table, after remove these drops
    #ensemble_model.get_cursor().execute("DROP TABLE IF EXISTS cell")
    #ensemble_model.get_cursor().execute("DROP TABLE IF EXISTS variable")
    #ensemble_model.get_cursor().execute("DROP TABLE IF EXISTS simulation")
    ensemble_model.create_table()
    for ensemble_name in ensemble_list:
        uuid = ensemble_model.insert_one(ensemble_name)
        ensemble_id_map[ensemble_name] = uuid


data = loadBRStatesTaxRevenues()

loadDataIntoDatabase(data)
#def connect_monet_db():
#    try:
#        with pymonetdb.connect(username="monetdb", password="monetdb", hostname="localhost", database="ensemble") as db:
#            cursor = db.cursor()
#            ensemble_data = loadBRStatesTaxRevenues()
#            ensemble_list = ensemble_data['ensemble'].unique()
#            ensemble_id_map = {}
#            simulation_list = ensemble_data['name'].unique()
#            simulation_id_map = {}
#            variable_list = ensemble_data.columns.drop(['ensemble', 'time', 'name'])
#            variable_id_map = {}
#            cursor.execute("CREATE TABLE IF NOT EXISTS ensemble (id UUID NOT NULL PRIMARY KEY, name VARCHAR(200) NOT NULL)")
#            cursor.execute("CREATE TABLE IF NOT EXISTS simulation (id UUID NOT NULL PRIMARY KEY, name VARCHAR(200) NOT NULL, ensemble_id UUID NOT NULL, FOREIGN KEY(ensemble_id) REFERENCES ensemble(id))")
#            cursor.execute("CREATE TABLE IF NOT EXISTS variable (id UUID NOT NULL PRIMARY KEY, name VARCHAR(200) NOT NULL)")
#            cursor.execute("""
#                           CREATE TABLE IF NOT EXISTS cell (
#                               id UUID NOT NULL PRIMARY KEY, 
#                               timestep DECIMAL NOT NULL,
#                               simulation_id UUID NOT NULL,
#                               variable_id UUID NOT NULL,
#                               value DECIMAL NOT NULL,
#                               FOREIGN KEY(simulation_id) REFERENCES simulation(id),
#                               FOREIGN KEY(variable_id) REFERENCES variable(id)
#                           )
#                           """)
#            db.commit()
#            for ensemble_name in ensemble_list:
#                query_ensemble = cursor.execute("SELECT * FROM ensemble WHERE name = \'%s\'" % ensemble_name)
#                if (query_ensemble == 0):
#                    cursor.execute("SELECT sys.uuid() AS uuid")
#                    uuid = cursor.fetchone()[0]
#                    new_record_result = cursor.execute("INSERT INTO ensemble (id, name) VALUES (uuid\'%s\', \'%s\')" % (uuid, ensemble_name))
#                    if(new_record_result == 1):
#                        ensemble_id_map[ensemble_name] = uuid
#                else:
#                    ensemble_id = cursor.fetchone()[0]
#                    ensemble_id_map[ensemble_name] = ensemble_id
#            for simulation_name in simulation_list:
#                query_simulation = cursor.execute("SELECT * FROM simulation WHERE name = \'%s\'" % simulation_name)
#                if (query_simulation == 0):
#                    ensemble_from_simulation = ensemble_data.loc[ensemble_data['name'] == simulation_name]['ensemble'].iloc[0]
#                    ensemble_id = ensemble_id_map[ensemble_from_simulation]
#                    cursor.execute("SELECT sys.uuid() AS uuid")
#                    uuid = cursor.fetchone()[0]
#                    new_record_result = cursor.execute("INSERT INTO simulation (id, name, ensemble_id) VALUES (uuid\'%s\', \'%s\', uuid\'%s\')" % (uuid, simulation_name, ensemble_id))
#                    if(new_record_result == 1):
#                        simulation_id_map[simulation_name] = uuid
#                else:
#                    simulation_id = cursor.fetchone()[0]
#                    simulation_id_map[simulation_name] = simulation_id
#            for variable_name in variable_list:
#                query_variable = cursor.execute("SELECT * FROM variable WHERE name = \'%s\'" % variable_name)
#                if (query_variable == 0):
#                    cursor.execute("SELECT sys.uuid() AS uuid")
#                    uuid = cursor.fetchone()[0]
#                    new_record_result = cursor.execute("INSERT INTO variable (id, name) VALUES (uuid\'%s\', \'%s\')" % (uuid, variable_name))
#                    if(new_record_result == 1):
#                        variable_id_map[variable_name] = uuid
#                else:
#                    variable_id = cursor.fetchone()[0]
#                    variable_id_map[variable_name] = variable_id
#            for index, row in ensemble_data.iterrows():
#                for variable_name in variable_list:
#                    simulation_id = simulation_id_map[row['name']]
#                    variable_id = variable_id_map[variable_name]
#                    query_cell = cursor.execute(
#                        """
#                        SELECT *
#                        FROM cell
#                        WHERE simulation_id = uuid\'%s\'
#                        AND variable_id = uuid\'%s\'
#                        AND timestep = %s
#                        """
#                        % (simulation_id, variable_id, row['time'])
#                    )
#                    if (query_cell == 0):
#                        cursor.execute("SELECT sys.uuid() AS uuid")
#                        uuid = cursor.fetchone()[0]
#                        print(uuid)
#                        new_record_result = cursor.execute(
#                            """
#                            INSERT INTO cell (id, simulation_id, variable_id, timestep, value) 
#                            VALUES (uuid\'%s\', uuid\'%s\', uuid\'%s\', %s, %s)
#                            """
#                            % (uuid, simulation_id, variable_id, row['time'], row[variable_name])
#                        )
#            db.commit()
#    except Exception as e:
#        print(e)
