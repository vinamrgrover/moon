import unittest
from moon import MoonBase, MoonQuery


class test_moon_base(unittest.TestCase):
    m = MoonBase()

    @classmethod
    def setUpClass(cls):
        cls.mb = MoonBase()

    def test_connection(self):
        response = self.mb._create_connection()


class test_moon_query(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.mq = MoonQuery()

    def test_attach_db(self):
        res = self.mq._attach_postgres()


class test_query(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.mq = MoonQuery(db = 'postgres', user = 'moon')
        cls.mq._attach_postgres()

    def test_query(self):
        res = self.mq.execute("SHOW TABLES;")
        print(res)
