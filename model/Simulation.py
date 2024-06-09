from db.Model import Model
from schema import Schema, And, Use, Optional, SchemaError
from uuid import UUID, uuid4

schema_record = Schema(
    {
        "name": str,
        "ensemble_id": UUID,
    }
)

class Simulation(Model):
    def __init__(self) -> None:
        super().__init__()

    def create_table(self):
        if self.get_driver() == "monetdb":
            self.get_cursor().execute("DROP TABLE IF EXISTS simulation CASCADE")
            self.get_cursor().execute("CREATE TABLE IF NOT EXISTS simulation (id UUID NOT NULL PRIMARY KEY, name VARCHAR(200) NOT NULL, ensemble_id UUID NOT NULL, FOREIGN KEY(ensemble_id) REFERENCES ensemble(id))")
        else:
            self.get_cursor().execute("DROP TABLE IF EXISTS simulation")
            self.get_cursor().execute("CREATE TABLE IF NOT EXISTS simulation (id TEXT NOT NULL PRIMARY KEY, name VARCHAR(200) NOT NULL, ensemble_id TEXT NOT NULL, FOREIGN KEY(ensemble_id) REFERENCES ensemble(id))")
        self.commit()

    def insert_one(self, record):
        if (schema_record.is_valid(record)):
            uuid = uuid4()
            if self.get_driver() == "monetdb":
                self.get_cursor().execute("INSERT INTO simulation (id, name, ensemble_id) VALUES (uuid\'%s\', \'%s\', uuid\'%s\')" % (uuid, record["name"], record["ensemble_id"]))
            else:
                self.get_cursor().execute("INSERT INTO simulation (id, name, ensemble_id) VALUES (\"%s\", \"%s\", \"%s\")" % (uuid, record["name"], record["ensemble_id"]))
            self.commit()
            return uuid
        else:
            raise Exception("ERROR: record structure is not valid to be inserted in the database.")
    
    def read_all(self):
        self.get_cursor().execute("SELECT * FROM simulation")
        return self.get_cursor().fetchall()

    def read_one(self, uuid):
        if self.get_driver() == "monetdb":
            self.get_cursor().execute("SELECT * FROM simulation WHERE id = uuid\'%s\'" % uuid)
        else:
            self.get_cursor().execute("SELECT * FROM simulation WHERE id = %s" % uuid)
        return self.get_cursor().fetchone()
