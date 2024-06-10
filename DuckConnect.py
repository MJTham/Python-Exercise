import duckdb

class DuckDBConnection:
    def __init__(self, database_path=':memory:'):
        """
        Initialize the DuckDB connection.
        
        :param database_path: Path to the DuckDB database file. ':memory:' for an in-memory database.
        """
        self.database_path = database_path
        self.connection = None

    def __enter__(self):
        """
        Enter the runtime context related to this object.
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the runtime context related to this object.
        """
        self.disconnect()

    def connect(self):
        """
        Establish a connection to the DuckDB database.
        """
        self.connection = duckdb.connect(database=self.database_path)
        print("Connection to DuckDB established.")

    def disconnect(self):
        """
        Close the connection to the DuckDB database.
        """
        if self.connection:
            self.connection.close()
            print("Connection to DuckDB closed.")

    def execute_query(self, query, params=None):
        """
        Execute a query on the DuckDB database.
        
        :param query: SQL query to be executed.
        :param params: Optional parameters for the SQL query.
        :return: Result of the query.
        """
        if not self.connection:
            raise Exception("Database not connected.")
        
        if params:
            return self.connection.execute(query, params).fetchall()
        else:
            return self.connection.execute(query).fetchall()

# Usage example
if __name__ == "__main__":
    with DuckDBConnection() as db:
        db.execute_query("CREATE TABLE test (id INTEGER, name VARCHAR)")
        db.execute_query("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')")
        results = db.execute_query("SELECT * FROM test")
        for row in results:
            print(row)
