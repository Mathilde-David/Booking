import unittest
import pandas as pd
from transform.transform_data import transform_data_unformatted
import os
import datetime
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())


class TestTransform(unittest.TestCase):
    def setUp(self):
        self.rawdata = pd.read_csv(os.environ.get('FILEPATH'))
        self.data = transform_data_unformatted(self.rawdata)

    def test_currency_consistency(self):
        '''
        Test that there's only one currency by restaurant used
        '''
        print("RUN: Test currency consistency... ", datetime.datetime.now())
        grouped_df = self.data.groupby("restaurant_id")
        grouped_df = grouped_df.agg({"currency": "nunique"})
        grouped_df = grouped_df.reset_index()

        currency_consistency = True
        for element in grouped_df["currency"]:
            if element != 1:
                currency_consistency = False
                break
        self.assertTrue(currency_consistency == True,
                        "ERROR: There's more than one currency use by restaurant. You must convert currency before sum amount")

    def test_currency_by_country(self):
        """
        Test that there's only one currency used per country
        """
        print("RUN: Test number of currency per country... ", datetime.datetime.now())
        country_currency = self.data[["country", "currency"]].drop_duplicates()
        self.assertEqual(len(country_currency), len(self.data["country"].drop_duplicates()),
                         "ERROR: Countries have more than one currency. You must convert currency before sum amount")

def suite_transform():
    """
    Add to the transform test suite the tests defined in the TestTransform class
    """
    suite = unittest.TestSuite()
    suite.addTest(TestTransform('test_currency_consistency'))
    suite.addTest(TestTransform('test_currency_by_country'))
    return suite
