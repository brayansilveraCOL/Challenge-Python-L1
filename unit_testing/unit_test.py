import unittest
from functions.funtions import send_request_get, make_etl
from functions.database_conection import create_database


class TestBasic(unittest.TestCase):

    def test_make_etl(self):
        sd = send_request_get()
        mk_etl = make_etl(sd)
        self.assertIsNotNone(mk_etl)

    def test_get(self):
        sd = send_request_get()
        self.assertIsNotNone(sd)


if __name__ == '__main__':
    unittest.main()
