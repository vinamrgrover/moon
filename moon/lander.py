import duckdb
from duckdb import IOException


class MoonBase:

    @staticmethod
    def _create_connection() -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(":memory:")
        return con


class MoonQuery(MoonBase):
    def __init__(self, host: str = "localhost", db: str = "postgres", user : str = "postgres", password : str = None):
        super().__init__()
        self.host = host
        self.port = 5432
        self.db = db
        self.user = user
        self.password = password
        self.cols = []
        self.con = self._create_connection()

    def _attach_postgres(self):

        try:
            self.con.sql(
                f"""
            ATTACH 'host = {self.host} port = {self.port} dbname = {self.db} user = {self.user} password = {self.password}' AS psql (TYPE postgres);
            """
            )
            self.con.sql("USE psql;")
        except IOException as e:
            raise IOException

    def execute(self, query : str):
        return self.con.sql(query)
