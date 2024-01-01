from duckdb import ParserException

class Endpoints:
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

    def view_endpoint(self, query, limit : int = 50):
        try:
            df = self.execute(query, df=True)
            return df.head(limit)
        except ParserException as e:
            return e
