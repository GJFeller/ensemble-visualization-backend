from db.Model import Model
from schema import Schema, And, Use, Optional, SchemaError
from uuid import UUID, uuid4

schema_record = Schema(
    {
        "value": float,
        "simulation_id": UUID,
        "variable_id": UUID,
        "timestep": float,
    }
)

class CellData(Model):
    def __init__(self) -> None:
        super().__init__()

    def create_table(self):
        if self.get_driver() == "monetdb":
            self.get_cursor().execute("DROP TABLE IF EXISTS cell_data CASCADE")
            self.get_cursor().execute("""
                                CREATE TABLE IF NOT EXISTS cell_data (
                                    id UUID NOT NULL PRIMARY KEY, 
                                    timestep DECIMAL NOT NULL,
                                    simulation_id UUID NOT NULL,
                                    variable_id UUID NOT NULL,
                                    value DECIMAL NOT NULL,
                                    FOREIGN KEY(simulation_id) REFERENCES simulation(id),
                                    FOREIGN KEY(variable_id) REFERENCES variable(id)
                                )
                                      """)
        else:
            self.get_cursor().execute("DROP TABLE IF EXISTS cell_data")
            self.get_cursor().execute("""
                                CREATE TABLE IF NOT EXISTS cell_data (
                                    id TEXT NOT NULL PRIMARY KEY, 
                                    timestep DECIMAL NOT NULL,
                                    simulation_id TEXT NOT NULL,
                                    variable_id TEXT NOT NULL,
                                    value DECIMAL NOT NULL,
                                    FOREIGN KEY(simulation_id) REFERENCES simulation(id),
                                    FOREIGN KEY(variable_id) REFERENCES variable(id)
                                )
                                      """)
        self.commit()

    def insert_one(self, record):
        if (schema_record.is_valid(record)):
            uuid = uuid4()
            if self.get_driver() == "monetdb":
                self.get_cursor().execute("""
                                INSERT INTO cell_data (id, simulation_id, variable_id, timestep, value) 
                                VALUES (uuid\'%s\', uuid\'%s\', uuid\'%s\', %s, %s)
                                                              """
                                % (uuid, record["simulation_id"], record["variable_id"], record['timestep'], record["value"])
                                )
            else:
                self.get_cursor().execute("""
                                INSERT INTO cell_data (id, simulation_id, variable_id, timestep, value) 
                                VALUES (\"%s\", \"%s\", \"%s\", %s, %s)
                                                              """
                                % (uuid, record["simulation_id"], record["variable_id"], record['timestep'], record["value"])
                                )
            self.commit()
            return uuid
        else:
            schema_record.validate(record)
            raise Exception("ERROR: record structure is not valid to be inserted in the database.")
    
    def read_all(self):
        self.get_cursor().execute("SELECT * FROM cell_data")
        return self.get_cursor().fetchall()

    def read_one(self, uuid):
        if self.get_driver() == "monetdb":
            self.get_cursor().execute("SELECT * FROM cell_data WHERE id = uuid\'%s\'" % uuid)
        else:
            self.get_cursor().execute("SELECT * FROM cell_data WHERE id = \"%s\"" % uuid)
        return self.get_cursor().fetchone()
    
    def get_celldata_all_variables(self, simulation, timestep):
        self.get_cursor().execute("SELECT cd.id, s.name, v.name, cd.timestep, cd.value FROM cell_data AS cd, simulation AS s, variable AS v WHERE s.id = cd.simulation_id AND v.id = cd.variable_id AND s.name = \'%s\' AND cd.timestep = %s"
                                  % (simulation, timestep))
        return self.get_cursor().fetchall()
    
    def get_timesteps(self):
        self.get_cursor().execute("SELECT DISTINCT timestep FROM cell_data")
        return self.get_cursor().fetchall()
