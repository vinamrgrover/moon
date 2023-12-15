import unittest
import pytest
from moon import MoonBase, MoonQuery


class test_moon_base(unittest.TestCase):
    m = MoonBase()

    @classmethod
    def setUpClass(cls):
        cls.mb = MoonBase()
        cls.ml = MoonQuery()

    def test_connection(self):
        response = self.mb._create_connection()
