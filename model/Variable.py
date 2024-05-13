from db.database import Database
from schema import Schema, And, Use, Optional, SchemaError

schema_record = Schema(
    {
        "name": str
    }
)

class Variable(Database):
    def __init__(self) -> None:
        super().__init__()

    def create_table(self):
        self.get_cursor().execute("DROP TABLE IF EXISTS variable CASCADE")
        self.get_cursor().execute("CREATE TABLE IF NOT EXISTS variable (id UUID NOT NULL PRIMARY KEY, name VARCHAR(200) NOT NULL)")
        self.commit()

    def insert_one(self, record):
        if (schema_record.is_valid(record)):
            self.get_cursor().execute("SELECT sys.uuid() AS uuid")
            uuid = self.get_cursor().fetchone()[0]
            new_record_result = self.get_cursor().execute("INSERT INTO variable (id, name) VALUES (uuid\'%s\', \'%s\')" % (uuid, record["name"]))
            self.commit()
            if(new_record_result == 1):
                return uuid
            else:
                raise Exception("Error adding record %s into table variable" % record["name"])
        else:
            raise Exception("ERROR: record structure is not valid to be inserted in the database.")
    
    def read_all(self):
        self.get_cursor().execute("SELECT * FROM variable")
        return self.get_cursor().fetchall()

    def read_one(self, uuid):
        self.get_cursor().execute("SELECT * FROM variable WHERE id = uuid\'%s\'" % uuid)
        return self.get_cursor().fetchone()
