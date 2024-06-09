from db.Model import Model
from schema import Schema, And, Use, Optional, SchemaError
from uuid import uuid4

schema_record = Schema(
    {
        "name": str
    }
)

class Variable(Model):
    def __init__(self) -> None:
        super().__init__()

    def create_table(self):
        if self.get_driver() == "monetdb":
            self.get_cursor().execute("DROP TABLE IF EXISTS variable CASCADE")
            self.get_cursor().execute("CREATE TABLE IF NOT EXISTS variable (id UUID NOT NULL PRIMARY KEY, name VARCHAR(200) NOT NULL)")
        else:
            self.get_cursor().execute("DROP TABLE IF EXISTS variable")
            self.get_cursor().execute("CREATE TABLE IF NOT EXISTS variable (id TEXT NOT NULL PRIMARY KEY, name VARCHAR(200) NOT NULL)")
        self.commit()

    def insert_one(self, record):
        if (schema_record.is_valid(record)):
            uuid = uuid4()
            if self.get_driver() == "monetdb":
                self.get_cursor().execute("INSERT INTO variable (id, name) VALUES (uuid\'%s\', \'%s\')" % (uuid, record["name"]))
            else:
                self.get_cursor().execute("INSERT INTO variable (id, name) VALUES (\"%s\", \"%s\")" % (uuid, record["name"]))
            self.commit()
            return uuid
        else:
            raise Exception("ERROR: record structure is not valid to be inserted in the database.")
    
    def read_all(self):
        self.get_cursor().execute("SELECT * FROM variable")
        return self.get_cursor().fetchall()

    def read_one(self, uuid):
        if self.get_driver() == "monetdb":
            self.get_cursor().execute("SELECT * FROM variable WHERE id = uuid\'%s\'" % uuid)
        else:
            self.get_cursor().execute("SELECT * FROM variable WHERE id = %s" % uuid)
        return self.get_cursor().fetchone()
