import os
import shutil
import tempfile
import unittest
import warnings
from abc import ABC
from datetime import datetime, date

from pandas.util.testing import assert_frame_equal
# noinspection PyProtectedMember
from pyspark import SQLContext
from pyspark.sql import SparkSession


class SparkleTestCase(unittest.TestCase, ABC):
    """
        This base class creates spark session which you can use in your unit tests.
        Spark parameters are tuned for local runs.
        A unique directory is created for each run to store the Hive tables, you can find them under
        'target/warehouse/'. This also holds the derby.log file. Hive meta data is stored in memory.
    """

    @classmethod
    def setUpClass(cls):
        warnings.simplefilter("ignore", ResourceWarning)  # ignore socket warnings
        cls.spark = cls.createSparkSession(cls.jar_path())
        cls.setup_class()

    def setUp(self):
        warnings.simplefilter("ignore", ResourceWarning)  # ignore socket warnings

    @classmethod
    def setup_class(cls):
        """Override this method for code that should be executed as part of setUpClass"""
        pass

    def tearDown(self):
        SQLContext(self.spark.sparkContext).clearCache()

    @classmethod
    def jar_path(cls) -> str:
        return ""

    @staticmethod
    def createSparkSession(jar_path: str = None):

        warehouse_tmp_dir = SparkleTestCase._create_tmp_warehouse_dir()

        builder = SparkSession.builder. \
            config("spark.hadoop.javax.jdo.option.ConnectionURL",
                   'jdbc:derby:memory:databaseName=metastore_db;create=true'). \
            config("spark.hadoop.javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver"). \
            config("spark.sql.warehouse.dir", warehouse_tmp_dir). \
            config("spark.driver.extraJavaOptions", "-Dderby.system.home={}".format(warehouse_tmp_dir)). \
            config("spark.ui.enabled", "false"). \
            config("spark.default.parallelism", 1). \
            config("spark.executor.cores", 1). \
            config("spark.executor.instances", 1). \
            config("spark.sql.shuffle.partitions", 1)

        if jar_path and len(jar_path) > 0:
            builder = builder.config("spark.jars", SparkleTestCase.root(jar_path))

        spark = builder.enableHiveSupport().getOrCreate()
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        spark.sparkContext.setLogLevel("ERROR")
        return spark

    @staticmethod
    def remove(path: str):
        if os.path.exists(path):
            if os.path.isfile(path):
                os.remove(path)
            else:
                shutil.rmtree(path)

    @staticmethod
    def clean_create(path: str):
        SparkleTestCase.remove(path)
        os.mkdir(path)

    @staticmethod
    def root(path: str):
        from os.path import dirname as up
        wd = os.getcwd()
        if "unittest" in wd and "python" in wd:
            # go up 3 dirs
            wd = up((up(up(wd))))

        return os.path.join(wd, path)

    def assertColumnsAnyOrder(self, df, columns):
        set_assert_columns = set(columns)
        set_df_columns = set(map(lambda f: f.simpleString(), df.schema.fields))
        self.assertEqual(set_df_columns, set_assert_columns)

    @staticmethod
    def _create_tmp_warehouse_dir():
        warehouse_tmp_dir = os.path.join(tempfile.mkdtemp(), "sparkle-test/")
        return warehouse_tmp_dir

    @staticmethod
    def assert_frame_equal_with_sort(expected, result, by=None):
        expected = expected.toPandas()
        result = result.toPandas()
        """ Inspired by
        https://blog.cambridgespark.com/unit-testing-with-pyspark-fb31671b1ad8 """
        if by is None:
            by = list(expected.columns)
        results_sorted = result.sort_values(by=by).reset_index(drop=True).sort_index(axis=1)
        expected_sorted = expected.sort_values(by=by).reset_index(drop=True).sort_index(axis=1)
        assert_frame_equal(expected_sorted, results_sorted)

    @staticmethod
    def dd(date_str: str) -> date:
        """Create date from a string having format '%Y-%m-%d'"""
        return datetime.strptime(date_str, '%Y-%m-%d').date()

    @staticmethod
    def dt(date_time_str: str) -> datetime:
        """
        :param date_time_str: date-time format %Y-%m-%d %H:%M:%S
        :return: datetime
        """
        return datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
