from db.database import Database
from schema import Schema, And, Use, Optional, SchemaError
from uuid import UUID

schema_record = Schema(
    {
        "name": str,
        "ensemble_id": UUID,
    }
)

class Simulation(Database):
    def __init__(self) -> None:
        super().__init__()

    def create_table(self):
        self.get_cursor().execute("DROP TABLE IF EXISTS simulation CASCADE")
        self.get_cursor().execute("CREATE TABLE IF NOT EXISTS simulation (id UUID NOT NULL PRIMARY KEY, name VARCHAR(200) NOT NULL, ensemble_id UUID NOT NULL, FOREIGN KEY(ensemble_id) REFERENCES ensemble(id))")
        self.commit()

    def insert_one(self, record):
        if (schema_record.is_valid(record)):
            self.get_cursor().execute("SELECT sys.uuid() AS uuid")
            uuid = self.get_cursor().fetchone()[0]
            new_record_result = self.get_cursor().execute("INSERT INTO simulation (id, name, ensemble_id) VALUES (uuid\'%s\', \'%s\', uuid\'%s\')" % (uuid, record["name"], record["ensemble_id"]))
            self.commit()
            if(new_record_result == 1):
                return uuid
            else:
                raise Exception("Error adding record %s into table simulation" % record["name"])
        else:
            raise Exception("ERROR: record structure is not valid to be inserted in the database.")
    
    def read_all(self):
        self.get_cursor().execute("SELECT * FROM simulation")
        return self.get_cursor().fetchall()

    def read_one(self, uuid):
        self.get_cursor().execute("SELECT * FROM simulation WHERE id = uuid\'%s\'" % uuid)
        return self.get_cursor().fetchone()
