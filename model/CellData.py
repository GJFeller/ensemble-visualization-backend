from db.database import Database
from schema import Schema, And, Use, Optional, SchemaError
from uuid import UUID

schema_record = Schema(
    {
        "value": float,
        "simulation_id": UUID,
        "variable_id": UUID,
        "timestep": float,
    }
)

class CellData(Database):
    def __init__(self) -> None:
        super().__init__()

    def create_table(self):
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
        self.commit()

    def insert_one(self, record):
        if (schema_record.is_valid(record)):
            self.get_cursor().execute("SELECT sys.uuid() AS uuid")
            uuid = self.get_cursor().fetchone()[0]
            new_record_result = self.get_cursor().execute("""
                            INSERT INTO cell_data (id, simulation_id, variable_id, timestep, value) 
                            VALUES (uuid\'%s\', uuid\'%s\', uuid\'%s\', %s, %s)
                                                          """
                            % (uuid, record["simulation_id"], record["variable_id"], record['timestep'], record["value"])
                            )
            self.commit()
            if(new_record_result == 1):
                return uuid
            else:
                raise Exception("Error adding record %s into table cell_data" % record["name"])
        else:
            schema_record.validate(record)
            raise Exception("ERROR: record structure is not valid to be inserted in the database.")
    
    def read_all(self):
        self.get_cursor().execute("SELECT * FROM cell_data")
        return self.get_cursor().fetchall()

    def read_one(self, uuid):
        self.get_cursor().execute("SELECT * FROM cell_data WHERE id = uuid\'%s\'" % uuid)
        return self.get_cursor().fetchone()
