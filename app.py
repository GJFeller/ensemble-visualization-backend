from flask import Flask, Response, request
from dotenv import dotenv_values
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
from typing import Dict, List, Tuple
from functools import lru_cache

app = Flask(__name__)

# Constants
DR_METHODS = ['PCA', 'UMAP']
BRAZILIAN_REGIONS = {
    'Norte': ['AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO'],
    'Nordeste': ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE'],
    'Centro-Oeste': ['DF', 'GO', 'MS', 'MT'],
    'Sudeste': ['ES', 'MG', 'RJ', 'SP'],
    'Sul': ['PR', 'RS', 'SC']
}

class DataFrameManager:
    def __init__(self):
        self.ensemble_df = self._create_dataframe_all_ensembles()

    def _fetch_model_data(self) -> Tuple[Dict, Dict, List]:
        ensemble_model = Ensemble.Ensemble()
        simulation_model = Simulation.Simulation()
        variable_model = Variable.Variable()
        cell_data_model = CellData.CellData()

        # Get variables
        variable_records = dict(variable_model.read_all())

        # Get ensembles
        ensemble_records = dict(ensemble_model.read_all())

        # Get simulations
        simulation_records = simulation_model.read_all()
        simulation_list = [
            (item[0], item[1], ensemble_records[item[2]])
            for item in simulation_records
        ]

        return variable_records, dict((x, [y, z]) for x, y, z in simulation_list), cell_data_model

    def _create_dataframe_all_ensembles(self) -> pd.DataFrame:
        variable_records, simulation_records, cell_data_model = self._fetch_model_data()

        # Create column list
        df_columns = ['ensemble', 'name', 'time'] + list(variable_records.values())

        # Get timesteps - Fix for tuple return type
        timesteps = [float(ts[0]) for ts in cell_data_model.get_timesteps()]

        # Initialize data array
        row_size = len(simulation_records) * len(timesteps)
        data = np.zeros((row_size, len(df_columns)), dtype=object)

        # Fill data array
        for row_idx, (sim_name, sim_data) in enumerate(simulation_records.items()):
            base_idx = row_idx * len(timesteps)
            for t_idx, timestep in enumerate(timesteps):
                curr_idx = base_idx + t_idx

                # Fill basic information
                data[curr_idx, 0] = sim_data[1]  # ensemble
                data[curr_idx, 1] = sim_data[0]  # name
                data[curr_idx, 2] = timestep     # time

                # Fill variable data
                result = cell_data_model.get_celldata_all_variables(sim_data[0], timestep)
                for cell_data in result:
                    col_idx = df_columns.index(cell_data[2])
                    data[curr_idx, col_idx] = float(cell_data[4])

        return pd.DataFrame(data=data, columns=df_columns)

# Initialize DataFrameManager
df_manager = DataFrameManager()

def create_cors_response(data, status_code=200):
    """Helper function to create CORS-enabled responses"""
    resp = Response(
        response=json.dumps(data) if isinstance(data, (dict, list)) else data,
        status=status_code,
        mimetype="application/json"
    )
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp

@app.route('/')
def hello():
    return create_cors_response(df_manager.ensemble_df.to_json(orient='index'))

@app.route('/list-ensembles')
def list_ensembles():
    df = df_manager.ensemble_df[df_manager.ensemble_df['time'] == 2023][['ensemble', 'name']]
    grouped = df.groupby('ensemble')['name'].apply(lambda x: x.values.tolist())
    return create_cors_response(grouped.to_json(orient='index'))

@app.route('/dr-methods')
def list_dr_methods():
    return create_cors_response(DR_METHODS)

@app.route('/variables')
def list_variables():
    columns = [col for col in df_manager.ensemble_df.columns
              if col not in ('ensemble', 'time', 'name')]
    return create_cors_response(columns)

@app.route('/dimensional-reduction')
def get_ensemble_dr():
    method = request.args.get('method', default="PCA", type=str)
    ensemble_list = request.args.getlist('ensemble')
    simulation_list = request.args.getlist('simulation')

    # Filter data
    df = df_manager.ensemble_df[df_manager.ensemble_df['time'] == 2023]
    if ensemble_list:
        df = df[df['ensemble'].isin(ensemble_list)]
    if simulation_list:
        df = df[df['name'].isin(simulation_list)]

    # Prepare data
    identifiers = df[['ensemble', 'time', 'name']]
    data = df.drop(columns=['ensemble', 'time', 'name'])
    scaled_data = StandardScaler().fit_transform(data)

    # Apply dimensional reduction
    if method == "PCA":
        reduced_data = PCA(n_components=2).fit_transform(scaled_data)
    elif method == "UMAP":
        reduced_data = umap.UMAP().fit_transform(scaled_data)
    else:
        return create_cors_response({"error": "Invalid method"}, 400)

    # Format results
    result_df = pd.concat([
        identifiers,
        pd.DataFrame(reduced_data, columns=['x', 'y'], index=data.index)
    ], axis=1)

    result_df['record_object'] = result_df[['name', 'x', 'y']].to_dict('records')
    grouped = result_df.groupby('ensemble')['record_object'].apply(list).to_dict()

    return create_cors_response(grouped)

@app.route('/temporal-evolution')
def temporal_data():
    aggregate = request.args.get('aggregate', default=False, type=bool)
    variable = request.args.get('variable', default='', type=str)
    ensemble_list = request.args.getlist('ensemble')
    simulation_list = request.args.getlist('simulation')

    # Get data
    df = df_manager.ensemble_df
    if not variable:
        variable = df.columns[-1]

    # Apply filters
    if ensemble_list:
        df = df[df['ensemble'].isin(ensemble_list)]
    if simulation_list:
        df = df[df['name'].isin(simulation_list)]

    # Group data and convert to a serializable format
    result = {}
    for ensemble_name, ensemble_group in df.groupby('ensemble'):
        result[ensemble_name] = {}
        for name, group in ensemble_group.groupby('name'):
            result[ensemble_name][name] = group[['time', variable]].values.tolist()

    if aggregate:
        return create_cors_response({"message": "Aggregation not implemented"}, 501)

    return create_cors_response(result)

@app.route('/correlation-matrix')
def correlation_matrix():
    ensemble_list = request.args.getlist('ensemble')
    simulation_list = request.args.getlist('simulation')

    # Filter data
    df = df_manager.ensemble_df[df_manager.ensemble_df['time'] == 2023]
    if ensemble_list:
        df = df[df['ensemble'].isin(ensemble_list)]
    if simulation_list:
        df = df[df['name'].isin(simulation_list)]

    # Calculate correlation matrix
    data = df.drop(columns=['ensemble', 'time', 'name'])
    correlation_matrix = data.corr().dropna(axis=0, how='all').dropna(axis=1, how='all')

    return create_cors_response(correlation_matrix.to_json(orient='index'))

if __name__ == '__main__':
    app.run(debug=True)
