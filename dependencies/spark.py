"""
spark.py
~~~~~~~~

Module containing helper function for use with Apache Spark

spark.py
~~~~~~~~~~
##################################################################################################################################
## NOTE: This script accepts required arguments from Spark-submit and return sparkSession as spark_sess, Logger as spark_logger, config as merged_config, 
##
## Developer: Satheesh Reddy 1152
## Version: 1.0
## Date: 2023-05-13
## Modified: 2023-05-24 (CDP-640)  - a) Added multi config file handling
##                                   b) Added Loggers
##     :param app_name: Name of Spark app.
##     :param master: Cluster connection details (defaults to local[*]).
##     :param jar_packages: List of Spark JAR package names.
##     :param files: List of files to send to Spark cluster (master and workers).
##    :param spark_config: Dictionary of config key-value pairs.
##     :return: A tuple of references to the Spark session, logger and config dict (only if available).
##################################################################################################################################

"""

__author__ = 'Satheesh Reddy'

import __main__

from os import environ, listdir, path
import json
from pyspark import SparkFiles
from pyspark.sql import SparkSession

from dependencies import logging


def start_spark(app_name='my_spark_app', master='local[*]', jar_packages=[],
                files=[], spark_config={}):

    # detect execution environment
    flag_repl = not(hasattr(__main__, '__file__'))
    flag_debug = 'DEBUG' in environ.keys()

    if not (flag_repl or flag_debug):
        # get Spark session factory
        spark_builder = (
            SparkSession
            .builder
            .appName(app_name))
    else:
        # get Spark session factory
        spark_builder = (
            SparkSession
            .builder
            .master(master)
            .appName(app_name)
            .config("spark.executor.cores", 4)
            .config("spark.executor.instances", 8)
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.shuffle.compress", "true")
            .config("spark.io.compression.codec", "snappy")
            .config("spark.yarn.submit.waitAppCompletion", "false")
            .config("spark.sql.crossJoin.enabled", "true")
            )

        # create Spark JAR packages string
        spark_jars_packages = ','.join(list(jar_packages))
        spark_builder.config('spark.jars.packages', spark_jars_packages)

        spark_files = ','.join(list(files))
        spark_builder.config('spark.files', spark_files)

        # add other config params
        for key, val in spark_config.items():
            spark_builder.config(key, val)

    # create session and retrieve Spark logger object
    spark_sess = spark_builder.getOrCreate()
    spark_logger = logging.Log4j(spark_sess)

    # get config file if sent to cluster with --files
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename
                    for filename in listdir(spark_files_dir)
                    if filename.endswith('config.json')]
    spark_logger.info("config files:"+str(config_files))
    
    # Create an empty dictionary to store the merged configurations
    merged_config = {}
    if config_files:
        for file in config_files:
            path_to_config_file = path.join(spark_files_dir, file)
            with open(path_to_config_file, 'r') as config_file:
                config_dict = json.load(config_file)
                merged_config.update(config_dict)
            spark_logger.info('loaded config from ' + file)
        spark_logger.info('config Values from all configs:' + str(merged_config))
    else:
        spark_logger.warn('no config file found')
        config_dict = None

    return spark_sess, spark_logger, merged_config
