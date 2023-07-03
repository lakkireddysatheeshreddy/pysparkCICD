"""
test_etl_job.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in etl_job.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.

test_etl_job.py
~~~~~~~~~~
##################################################################################################################################
## This is sample documenttion. Please remove/edit accordingly
## This script is used to run Python Script Sequentially
## Script accept four parameter as below
## --cycledate      [Required] Cycle date will be date for which data will be processed
## --inputjson      [Required] Name of the Input Json which is required to be processed
## --seqno          [Optional] Need to pass seqno when some one wants to run only one script from json
## --onetimeload    [Optional] Need to set to y if user wants to run One time loads
## --sourcename     [Required] To be passed only if user wants to run attributes within specific Product
##
## NOTE: Please note that if change_data_capture.py is run , kindly include onetimeload as parameter too to specifically.
##
## Developer: Satheesh Reddy 1152
## Version: 1.0
## Date: 2018-03-03
## Modified: 2018-04-11 (CDP-640)  - a) Added input JSON and source name as arguments to be passed to script.This feature will
##                                      allow user to run specific segments if required.
##                                   b) Removed the hard coded JSON Value passed to runner Function.
##
##
##Modified: 2018-08-16 (CDP-970)  - a) Added sequence number as argument and removed file name argument. This is optional parameter for 
##                                     specific source.
##
##Modified: 2018-08-16 (CDP-969)  - a) commented read funtion in line number 101.
##                                  b) Added one parameter in run_script function which will accept input json from runner(). 
##
##Modified: 2018-09-12 (CDP-987)  - a) Added sourcename parameter in run_script function which will get added in spark-submit 
##										for change data capture intial script.
##                                  b)Added --deploy-mode cluster --master yarn parameter to spark-submit commands.
##################################################################################################################################

__author__ = 'ABJAYON'

"""
import unittest

import json

from pyspark.sql.functions import mean

from dependencies.spark import start_spark
from jobs.etl_job import transform_data


class SparkETLTests(unittest.TestCase):
    """Test suite for transformation in etl_job.py
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        self.config = json.loads("""{"steps_per_floor": 21}""")
        self.spark, *_ = start_spark()
        self.test_data_path = 'tests/test_data/'

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def test_transform_data(self):
        """Test data transformer.

        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        # assemble
        input_data = (
            self.spark
            .read
            .parquet(self.test_data_path + 'employees'))

        expected_data = (
            self.spark
            .read
            .parquet(self.test_data_path + 'employees_report'))

        expected_cols = len(expected_data.columns)
        expected_rows = expected_data.count()
        expected_avg_steps = (
            expected_data
            .agg(mean('steps_to_desk').alias('avg_steps_to_desk'))
            .collect()[0]
            ['avg_steps_to_desk'])

        # act
        data_transformed = transform_data(input_data, 21)

        cols = len(expected_data.columns)
        rows = expected_data.count()
        avg_steps = (
            expected_data
            .agg(mean('steps_to_desk').alias('avg_steps_to_desk'))
            .collect()[0]
            ['avg_steps_to_desk'])

        # assert
        self.assertEqual(expected_cols, cols)
        self.assertEqual(expected_rows, rows)
        self.assertEqual(expected_avg_steps, avg_steps)
        self.assertTrue([col in expected_data.columns
                         for col in data_transformed.columns])


if __name__ == '__main__':
    unittest.main()
