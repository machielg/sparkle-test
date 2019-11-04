# Sparkle Test Library
Behave BDD and Unit testing utilities for easy and fast tests in pyspark

The base unit test class creates an in memory derby DB and any warehouse files 
are written to a unique directory to avoid clashes between tests

# Installation
```
pip install sparkle-test
```

# Usage 
Simply subclass the SparkleTestCase

```python
from sparkle_test import SparkleTestCase

class SparkleTestCaseTest(SparkleTestCase):
   
   def test_something(self):
      print(self.spark.version)
```

You can optionally set specific settings for custom jars, packages and repositories:
```python
class SparkleTestCaseTest(SparkleTestCase):
    jars = ['src/unittest/resources/foo.jar']
    packages = ['com.databricks:spark-csv_2.10:1.4.0']
    repositories = ['http://nexus.foo.com']
    options = {'spark.foo': 'bar'}
```
