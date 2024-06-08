from dotenv import dotenv_values
#from surrealdb import Surreal
import pymonetdb
#import asyncio
from abc import ABC, abstractmethod

default_envs = {
    "DB_DRIVER": "monetdb",
    "DB_HOSTNAME": "localhost",
    "DB_PORT": 50000,
    "DB_DATABASE": "ensemble",
    "DB_USERNAME": "ensemble",
    "DB_PASSWORD": "ensemble",
    "DATA_FILENAME": "data.csv"
    }
config = {
    **default_envs,
    **dotenv_values(".env")
}
print(config)
print(config["DB_DRIVER"])

class Model(ABC):
    """An abstract class with some basic implementation to be a base for data models

    It implements the connection creation, returns the cursor from the database and commits the executed queries.
    Other methods are abstract methods.

    For now, it only supports pymonetdb as the database driver. In the future it may support more drivers, but it
    needs to be more general to support more different DBMS.
    """

    def __init__(self):
        """Sets the database driver according to what is in the variable DB_DRIVER in .env file and
        creates the connection to the database.
        """

        self.__driver = config["DB_DRIVER"]
        self.__con = self.__create_connection()
        self.__cur = self.__con.cursor()
    
    def __create_connection(self):
        """Creates the database connection according to the driver set
        """

        if self.__driver == "monetdb":
            return pymonetdb.connect(username=config["DB_USERNAME"], password=config["DB_PASSWORD"], hostname=config["DB_HOSTNAME"], port=config["DB_PORT"], database=config["DB_DATABASE"])
        else:
            raise Exception("Database driver %s not yet implemented" % self.__driver)
    
    def get_cursor(self):
        """Returns the database cursor to execute queries
        """

        if self.__driver == "monetdb":
            return self.__cur
        else:
            raise Exception("Database driver %s not yet implemented" % self.__driver)
        
    def commit(self):
        """Commits all executes queries to the database
        """

        if self.__driver == "monetdb":
            self.__con.commit()
        else:
            raise Exception("Database driver %s not yet implemented" % self.__driver)

    # NOTE: Maybe, in the future, we are going to make a more generic implementation of a query    
    #def execute_query(self, query):
    #    if self.__driver == "monetdb":
    #        return self.__cur.execute(query)
    #    else:
    #        raise Exception("Database driver %s not yet implemented" % self.__driver)
    
    @abstractmethod
    def create_table(self):
        """Abstract method for the model to create table in the database
        """
        
        pass

    @abstractmethod
    def insert_one(self, record):
        """Abstract method for the model to insert one record in the database

        :param record: A record which schema needs to be defined by the model
        :type record: object

        :returns: uuid from added record in the database
        :rtype: UUID 
        """

        pass
    
    @abstractmethod
    def read_all(self):
        """Abstract method for the model to return all records from a certain model

        :returns: list of all data from the model
        :rtype: list
        """

        pass

    @abstractmethod
    def read_one(self, uuid):
        """Abstract method for the model to return a specific record

        :param uuid: uuid of a record
        :type uuid: UUID

        :returns: a record from the database
        :rtype: object
        """

        pass

## Adding SurrealDB code here just if I am going to use it in future
#async def connect_surreal_db():
#    async with Surreal("ws://localhost:8000/rpc") as db:
#        #try:
#            # Authentication
#            await db.signin({"user": "root", "pass": "root"})
#            await db.use("ensemble", "ensemble")
#            ensemble_data = loadBRStatesTaxRevenues()
#            ensemble_list = ensemble_data['ensemble'].unique()
#            ensemble_id_map = {}
#            simulation_list = ensemble_data['name'].unique()
#            simulation_id_map = {}
#            variable_list = ensemble_data.columns.drop(['ensemble', 'time', 'name'])
#            variable_id_map = {}
#            # Defining tables
#            for ensemble_name in ensemble_list:
#                db_ensemble = await db.query("SELECT VALUE id FROM ensemble WHERE name = \"%s\"" % ensemble_name)
#                if (len(db_ensemble[0]['result']) == 0):
#                    created_ensemble_record = await db.create(
#                        "ensemble",
#                        {
#                            "name": ensemble_name
#                        }
#                    )
#                    print(created_ensemble_record)
#                    ensemble_id_map[ensemble_name] = created_ensemble_record[0]['id']
#                else:
#                    ensemble_id = db_ensemble[0]['result'][0]
#                    ensemble_id_map[ensemble_name] = ensemble_id
#            for simulation_name in simulation_list:
#                db_simulation = await db.query("SELECT VALUE id FROM simulation WHERE name = \"%s\"" % simulation_name)
#                if (len(db_simulation[0]['result']) == 0):
#                    ensemble_from_simulation = ensemble_data.loc[ensemble_data['name'] == simulation_name]['ensemble'].iloc[0]
#                    ensemble_id = ensemble_id_map[ensemble_from_simulation]
#                    print(ensemble_id)
#                    created_simulation_record = await db.create(
#                        "simulation",
#                        {
#                            "name": simulation_name,
#                            "ensemble": ensemble_id
#                        }
#                    )
#                    simulation_id_map[simulation_name] = created_simulation_record[0]['id']
#                else:
#                    simulation_id = db_simulation[0]['result'][0]
#                    simulation_id_map[simulation_name] = simulation_id
#            for variable_name in variable_list:
#                db_variable = await db.query("SELECT VALUE id FROM variable WHERE name = \"%s\"" % variable_name)
#                if (len(db_variable[0]['result']) == 0):
#                    created_variable_record = await db.create(
#                        "variable",
#                        {
#                            "name": variable_name,
#                        }
#                    )
#                    variable_id_map[variable_name] = created_variable_record[0]['id']
#                else:
#                    variable_id = db_variable[0]['result'][0]
#                    variable_id_map[variable_name] = variable_id
#            await db.query(
#                """
#                DEFINE TABLE IF NOT EXISTS ensemble;
#                DEFINE TABLE IF NOT EXISTS simulation;
#                DEFINE TABLE IF NOT EXISTS variable;
#                DEFINE TABLE IF NOT EXISTS cell;
#                DEFINE FIELD name ON TABLE ensemble TYPE string;
#                DEFINE FIELD name ON TABLE simulation TYPE string;
#                DEFINE FIELD ensemble_id ON TABLE simulation TYPE record<ensemble>;
#                DEFINE FIELD name ON TABLE variable TYPE string;
#                DEFINE FIELD simulation_id ON TABLE cell TYPE record<simulation>;
#                DEFINE FIELD variable_id ON TABLE cell TYPE record<variable>;
#                DEFINE FIELD timestep ON TABLE cell TYPE decimal;
#                DEFINE FIELD value ON TABLE cell TYPE decimal;
#                """)
#            # Organizando, por enquanto, a questão de variáveis e tempo em duas tabelas: variables,
#            # que contém a descrição de variáveis, e cell, com o valor das variáveis em um instante de tempo
#            # (Depois será considerada a questão espacial, mas não para esse dataset usado de teste)
#            for index, row in ensemble_data.iterrows():
#                for variable_name in variable_list:
#                    db_cell = await db.query(
#                        """
#                        SELECT *
#                        FROM cell
#                        WHERE simulation_id IN (
#                          SELECT VALUE id
#                          FROM simulation
#                          WHERE name=\"%s\"
#                        )
#                        AND variable_id IN (
#                          SELECT VALUE id
#                          FROM variable
#                          WHERE name=\"%s\"
#                        )
#                        AND timestep=%s;
#                        """
#                        % (row['name'], variable_name, row['time'])
#                    )
#                    if (len(db_cell[0]['result']) == 0):
#                        simulation_id = simulation_id_map[simulation_name]
#                        variable_id = variable_id_map[variable_name]
#                        print("Adding cell with simulation_id %s, variable_id %s and timestep %s" % (row['name'], variable_name, row['time']))
#                        await db.create(
#                            "cell",
#                            {
#                                "simulation_id": simulation_id,
#                                "variable_id": variable_id,
#                                "timestep": row['time'],
#                                "value": row[variable_name]
#                            }
#                        )
#
#
#            #variable_name_list = ensemble_data.columns.drop(['ensemble', 'time', 'name'])
#            #for index, row in ensemble_data.iterrows():
#            #    for variable_name in variable_name_list:
#            #        # Check if the variable is added in the database, case not, add it
#            #        db_variable = await db.query("SELECT * FROM variable WHERE name = \"%s\"" % variable_name)
#            #        if (len(db_variable[0]['result']) == 0):
#            #            await db.create(
#            #                "variable",
#            #                {
#            #                    "name": variable_name
#            #                }
#            #            )
#            #        db_cell = await db.query(
#            #            """
#            #            SELECT *
#            #            FROM cell
#            #            WHERE simulation_id IN (
#            #              SELECT VALUE id
#            #              FROM simulation
#            #              WHERE name=\"%s\"
#            #            )
#            #            AND variable_id IN (
#            #              SELECT VALUE id
#            #              FROM variable
#            #              WHERE name=\"%s\"
#            #            )
#            #            AND timestep=%s;
#            #            """
#            #            % (row['name'], variable_name, row['time'])
#            #        )
#            #        if (len(db_cell[0]['result']) == 0):
#            #            simulation_id_query = await db.query("SELECT VALUE id FROM simulation WHERE name=\"%s\"" % row['name'])
#            #            variable_id_query = await db.query("SELECT VALUE id FROM variable WHERE name=\"%s\"" % variable_name)
#            #            simulation_id = simulation_id_query[0]['result'][0]
#            #            variable_id = variable_id_query[0]['result'][0]
#            #            print("Adding cell with simulation_id %s, variable_id %s and timestep %s" % (row['name'], variable_name, row['time']))
#            #            await db.create(
#            #                "cell",
#            #                {
#            #                    "simulation_id": simulation_id,
#            #                    "variable_id": variable_id,
#            #                    "timestep": row['time'],
#            #                    "value": row[variable_name]
#            #                }
#            #            )
#
#        #except Surreal.