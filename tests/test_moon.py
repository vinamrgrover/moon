import unittest
from moon import MoonBase, MoonQuery
import pytest
from testcontainers.postgres import PostgresContainer
import duckdb


class test_moon_base(unittest.TestCase):
    m = MoonBase()

    @classmethod
    def setUpClass(cls):
        cls.mb = MoonBase()

    def test_connection(self):
        response = self.mb._create_connection()


class test_moon_query(unittest.TestCase):

    # @classmethod
    # def setUpClass(cls):
    #     cls.mq = MoonQuery()

    @pytest.mark.skip('ignoring test')
    def test_attach_db(self):
        res = self.mq._attach_postgres()


class test_query(unittest.TestCase):

    @classmethod
    def setUp(cls):
        cls.postgres_container = PostgresContainer()
        cls.postgres_container.with_exposed_ports(5432)
        cls.postgres_container.start()
        container_port = cls.postgres_container.get_exposed_port(port = 5432)
        cls.mq = MoonQuery(db = 'test', user = 'test', password = 'test', host = 'localhost', port = container_port)
        cls.mq._attach_postgres()

    def tearDown(self):
        self.postgres_container.stop()

    def test_query(self):
        res = self.mq.execute("SHOW TABLES;")
        self.assertEqual(type(res), duckdb.DuckDBPyRelation)
