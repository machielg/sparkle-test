import pathlib

from pyspark.sql.types import LongType

from sparkle_test import SparkleTestCase


class SparkleTestCaseTest(SparkleTestCase):
    jars = ['src/unittest/resources/foo.jar']
    packages = ['com.databricks:spark-csv_2.10:1.4.0']
    repositories = ['http://nexus.foo.com']
    options = {'spark.foo': 'bar'}

    def test_log_file_creation(self):
        df = self.spark.createDataFrame([('Alice', 1)])
        df.write.saveAsTable("alice_table")
        self.assertEqual(0, len(self._find_log_files_outside_target_dir()), "Log files outside target directory")

    def test_custom_settings(self):
        self.assertIn('foo.jar', self.spark.conf.get('spark.jars'))
        self.assertIn('spark-csv', self.spark.conf.get('spark.jars.packages'))
        self.assertIn('nexus.foo.com', self.spark.conf.get('spark.jars.repositories'))
        self.assertEqual('bar', self.spark.conf.get('spark.foo'))

    def _find_log_files_outside_target_dir(self) -> list:
        path = self.root('.')
        # logs are expected in the target dir
        target_dir = pathlib.Path(self.root('target'))

        log_files = [f for f in pathlib.Path(path).rglob('*.log') if target_dir not in f.parents]
        return log_files

    def test_random_df(self):
        df = self.randomDF()
        self.assertIsNotNone(df)
        self.assertGreaterEqual(df.count(), 1)
        self.assertGreaterEqual(len(df.columns), 1)

    def test_random_df_with_cols(self):
        df = self.randomDF("a", "b")
        self.assertEqual(2, len(df.columns))
        self.assertEqual("a", df.columns[0])
        self.assertEqual("b", df.columns[1])
        self.assertGreaterEqual(df.count(), 1)

    def test_random_df_with_cols_and_type(self):
        df = self.randomDF("a:bigint")
        self.assertEqual(1, len(df.columns))
        self.assertEqual("a", df.columns[0])
        self.assertEqual(LongType(), df.schema.fields[0].dataType)
        self.assertGreaterEqual(df.count(), 1)