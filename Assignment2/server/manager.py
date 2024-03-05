from helper import SQLHandler, DataHandler


class Manager:
    def __init__(self, shard_id=None, columns=None, dtypes=None):
        self.shard_id = shard_id
        self.columns = columns
        self.dtypes = dtypes
        self._setupSQL()

    def _setupSQL(self):
        self.sql_handler = SQLHandler(
            host="localhost", user="root", password="mysql1234", db=self.shard_id
        )
        self.data_handler = DataHandler(self.columns, self.dtypes, self.sql_handler)
