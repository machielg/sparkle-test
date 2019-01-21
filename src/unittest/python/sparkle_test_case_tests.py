import pathlib

from sparkle_test_case import SparkleTestCase


class SparkleTestCaseTest(SparkleTestCase):

    @classmethod
    def setup_class(cls):
        pass

    def test_log_file_creation(self):
        df = self.spark.createDataFrame([('Alice', 1)])
        df.write.saveAsTable("alice_table")
        self.assertEqual(0, len(self._find_log_files_outside_target_dir()), "Log files outside target directory")

    def _find_log_files_outside_target_dir(self) -> list:
        path = self.root('.')
        # logs are expected in the target dir
        target_dir = pathlib.Path(self.root('target'))

        log_files = [f for f in pathlib.Path(path).rglob('*.log') if target_dir not in f.parents]
        return log_files
