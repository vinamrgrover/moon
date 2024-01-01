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

    Methods
    -------
        _create_connection()
            Initializes DuckDB connection in an in-memory DB.
    """

    @staticmethod
    def _create_connection() -> duckdb.DuckDBPyConnection:
        """
        Initializes DuckDB connection in an in-memory DB.

        Returns
        -------
            duckdb.DuckDBPyConnection
                DuckDB connection object.
        """
        con = duckdb.connect(":memory:")
        return con


class MoonQuery(MoonBase):
    """
    Sub Class for this module.

    Methods
    -------
        _attach_postgres()
            Attaches PostgreSQL DB to DuckDB's in-memory DB.

        execute()
            Executes query in our against our in-memory DB.
    """

    def __init__(
        self,
        host: str,
        db: str,
        user: str,
        password: str,
        port : int
    ):
        """
        Constructor method for MoonBase.

        Parameters
        ----------
            host : str
                specifies host for our PostgreSQL DB.
            db : str
                specifies DB name.
            user : str
                specifies the user as whom to connect to our DB.
            password : str
                specifies the password of the user.
        """
        super().__init__()
        self.host = host
        self.port = 5432
        self.dbname = db
        self.user = user
        self.password = password
        self.port = port
        self._cols = []
        self._con = self._create_connection()
        self._attach_postgres()

    def _attach_postgres(self) -> None:
        """
        Attaches PostgreSQL DB to DuckDB's in-memory DB.
        """
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
        """
        Executes query in our against our in-memory DB.

        Parameters
        ----------
        query : str
            Query to execute.
        df : bool
            boolean value that makes the method return a Pandas DataFrame.

        Returns
        -------
        pd.DataFrame
            Query result as a Pandas DataFrame.
        duckdb.DuckDBPyRelation
            Query result as DuckDB Relation.
        """
        if df:
            try:
                return self._con.sql(query).df()
            except ParserException as e:
                # Raising ParserException if there's a syntax error in Query.
                raise e

        try:
            return self._con.sql(query)
        except ParserException as e:
            # Raising ParserException if there's a syntax error in Query.
            raise e

    def query_endpoint(self, query: str):
        """
        Serves as a callback method for query() method.


        Parameters
        ----------
        query : str
            Query to execute against our DB.
        """
        df = self.execute(query, df=True)

        # Temporary workaround for dealing with NaNs (string formatted)
        df.replace("NaN", "", inplace=True)
        return df.to_json(orient="records", indent=2)

    def query_save_endpoint(self, query: str, file_path: str):
        """
        Serves as a callback method for query_save() method.


        Parameters
        ----------
        query : str
            Query to execute against our DB.
        file_path : str
            The absolute path used to write our DataFrame as csv.
        """
        df = self.execute(query, df=True)

        df.to_csv(f"{file_path}.csv", index=False)
        return df.to_json(orient="records", indent=2)

    def view_endpoint(self, query):
        df = self.execute(query, df=True)
        return df.head()
