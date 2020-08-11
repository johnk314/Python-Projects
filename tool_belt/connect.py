import geopandas
import os
import pandas
import psycopg2
import pyodbc
import requests
import sqlalchemy


class GISODBC:
    """"""
    def __init__(self):
        # TODO: psycopg2 vs sqlalchemy: https://hackersandslackers.com/psycopg2-postgres-python/
        # TODO: sqlalchemy ORM tutorials: https://hackersandslackers.com/series/mastering-sqlalchemy/

        # TODO: Connect with sqlalchemy
        self.engine = sqlalchemy.create_engine(r'postgresql://postgres:Jck258-5049@localhost:5433/GIS')

        # TODO: Connect with psycopg2.
        # host = 'localhost'
        # port = 5433
        # database = 'GIS'
        # user = 'postgres'
        # password = 'Jck258-5049'
        # self.con = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)

        # Helper function for accessing pd.read_sql_query AND pd.read_sql_table().
        # Read query results into a DataFrame.
        # pd.read_sql(sql=None, con=self.con, index_col=None, coerce_float=None, params=None, parse_dates=None, columns=None, chunksize=None)

        # Returns a DataFrame corresponding to the result set of the query string.
        # pd.read_sql_query(sql=None, con=self.con, index_col=None, coerce_float=None, params=None, parse_dates=None, chunksize=None)

        # Given a table name and connection, returns a DataFrame.
        # pd.read_sql_table(table_name=None, con=self.con, schema=None, index_col=None, coerce_float=None,parse_dates=None, columns=None, chunksize=None)

        # pd.to_sql()

    def show_schemas(self):
        q = r'SELECT schema_name FROM information_schema.schemata;'
        return pandas.read_sql(sql=q, con=self.engine)

    def show_tables(self, schema_name):
        q = r"SELECT * FROM information_schema.tables WHERE table_schema = '{}' AND table_type = 'BASE TABLE';".format(schema_name)
        return pandas.read_sql(sql=q, con=self.engine)

    def show_views(self, schema_name):
        q = r"SELECT * FROM information_schema.tables WHERE table_schema = '{}' AND table_type = 'VIEW';".format(schema_name)
        return pandas.read_sql(sql=q, con=self.engine)

    def get_unemployment(self):
        q = r'SELECT * FROM "FRED_Unemployment";'
        return pandas.read_sql(sql=q, con=self.engine)

    def get_geo(self):
        # https://geopandas.org/reference/geopandas.read_postgis.html#geopandas.read_postgis
        geopandas.read_postgis(sql=r'', con=self.engine, geom_col='geom', crs=None, index_col=None, coerce_float=True,
                               parse_dates=None, params=None, chunksize=None)

    def write_dataframe(self, dataframe, schema_name, table_name, if_exists='fail'):
        """
        :param dataframe: Write the incoming dataframe as a table.
        :param schema_name: The schema to write the table to.
        :param table_name: The name of the table to write.
        :param if_exists: What to do it the table exists in the specified database. Options include:'fail', 'replace', 'append'
        :return: None
s        """
        # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html
        dataframe.to_sql(name=table_name, con=self.engine, if_exists=if_exists, schema=schema_name)

    def write_geodataframe(self, geodataframe, table_name, schema_name):
        """

        :param geodataframe: Input geodataframe to be written.
        :param table_name: Name of the table to be created and/or written to.
        :param schema_name: Schema of the table to be created and/or written to.
        :return: None
        """
        # https://geopandas.org/io.html
        # https://geopandas.org/reference.html#geopandas.GeoDataFrame.to_postgis
        geodataframe.to_postgis(name=table_name, con=self.engine, if_exists='fail', schema=schema_name, chunksize=5000)


class Files:
    def __init__(self):
        # TODO: List recursively into a dictionary. use pprint.pprint to show the levels.
        for f in os.listdir('data'):
            print(f)


if __name__ == '__main__':
    # Put test-harness here.
    pass
