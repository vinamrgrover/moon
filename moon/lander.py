"""
This module demonstrates the usage of Moon Module.

Classes:
    MoonBase
    MoonQuery
"""

import duckdb
from duckdb import IOException, BinderException, ParserException
import pandas as pd


class MoonBase:
    """
    Super class for this module.
    """

    @staticmethod
    def _create_connection() -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(":memory:")
        return con


class MoonQuery(MoonBase):
    def __init__(
        self,
        host: str,
        db: str,
        user: str,
        password: str,
    ):
        super().__init__()
        self.host = host
        self.port = 5432
        self.dbname = db
        self.user = user
        self.password = password
        self._cols = []
        self._con = self._create_connection()
        self._attach_postgres()

    def _attach_postgres(self):
        conn_str = " ".join(
            [
                f"{key}={val}"
                for key, val in vars(self).items()
                if type(val) == str or type(val) == int
            ]
        )
        try:
            self._con.sql(
                f"""
            ATTACH '{conn_str}' AS psql (TYPE postgres);
            """
            )
            self._con.sql("USE psql;")

        # If PostgreSQL client is unavailable
        except IOException as e:
            self.con = None
            raise e

        # If DB is already attached
        except BinderException:
            self._con.sql(
                """
            USE memory;
            DETACH psql;
            """
            )

            self._attach_postgres()

    def execute(
        self, query: str, df: bool = None
    ) -> (pd.DataFrame, duckdb.DuckDBPyRelation):
        if df:
            try:
                return self.con.sql(query).df()
            except ParserException as e:
                raise e

        try:
            return self._con.sql(query)
        except ParserException as e:
            raise e
