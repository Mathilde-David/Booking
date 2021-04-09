import unittest
import pandas as pd
import os
import datetime
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())


class TestExtract(unittest.TestCase):
    def setUp(self):
        self.data = pd.read_csv(os.environ.get('FILEPATH'))
        self.nb_column_expected = 9
        self.column_names_expected = ["booking_id", "restaurant_id", "restaurant_name", "client_id", "client_name", "amount", "guests", "date", "country"]

    def test_number_of_columns(self):
        """
        Test the number of columns the input data has.
        9 columns are expected
        """
        print("RUN: Test number of columns... ", datetime.datetime.now())
        nb_column_data = len(self.data.columns)
        self.assertEqual(nb_column_data, self.nb_column_expected, "ERROR: Unexpected number of columns")

    def test_colnames(self):
        """
        Test the colnames of the input data
        Expected : "booking_id", "restaurant_id", "restaurant_name", "client_id", "client_name", "amount", "guests", "date", "country"
        """
        print("RUN: Test colnames... ", datetime.datetime.now())
        data_colnames = self.data.columns.tolist()
        for element in self.column_names_expected:
            if element in data_colnames:
                data_colnames.remove(element)
        self.assertTrue(len(data_colnames)==0, "ERROR: Unexpected colnames")


def suite_extract():
    """
    Add to the extract test suite the tests defined in the TestExtract class
    """
    suite = unittest.TestSuite()
    suite.addTest(TestExtract('test_number_of_columns'))
    suite.addTest(TestExtract('test_colnames'))
    return suite