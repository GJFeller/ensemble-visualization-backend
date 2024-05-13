from db.Model import Model
from schema import Schema, And, Use, Optional, SchemaError

schema_record = Schema(
    {
        "name": str
    }
)

class Ensemble(Model):
    def __init__(self) -> None:
        super().__init__()

    def create_table(self):
        self.get_cursor().execute("DROP TABLE IF EXISTS ensemble CASCADE")
        self.get_cursor().execute("CREATE TABLE IF NOT EXISTS ensemble (id UUID NOT NULL PRIMARY KEY, name VARCHAR(200) NOT NULL)")
        self.commit()

    def insert_one(self, record):
        if (schema_record.is_valid(record)):
            self.get_cursor().execute("SELECT sys.uuid() AS uuid")
            uuid = self.get_cursor().fetchone()[0]
            new_record_result = self.get_cursor().execute("INSERT INTO ensemble (id, name) VALUES (uuid\'%s\', \'%s\')" % (uuid, record["name"]))
            self.commit()
            if(new_record_result == 1):
                return uuid
            else:
                raise Exception("Error adding record %s into table ensemble" % record["name"])
        else:
            raise Exception("ERROR: record structure is not valid to be inserted in the database.")
    
    def read_all(self):
        self.get_cursor().execute("SELECT * FROM ensemble")
        return self.get_cursor().fetchall()

    def read_one(self, uuid):
        self.get_cursor().execute("SELECT * FROM ensemble WHERE id = uuid\'%s\'" % uuid)
        return self.get_cursor().fetchone()
