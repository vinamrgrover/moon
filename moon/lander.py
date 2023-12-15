from flask import Flask
import duckdb


class MoonBase:
    def __init__(self):
        self.cols = []
        self.count = 5

    def _create_connection(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(":memory:")
        return con


class MoonQuery(MoonBase):
    def __init__(self, host: str = "localhost", db: str = "postgres"):
        super().__init__()
        self.host = host
        self.port = 5432
        self.db = db
        self.cols = []

    def _attach_postgres(self):
        con = self._create_connection()
        res = con.sql(
            f"""
        ATTACH host = {self.host} port = {self.port} dbname = {self.db} AS psql (TYPE postgres);
        """
        )
        return res
