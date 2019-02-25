"""
Unit tests for pyspark.sql; additional tests are implemented as doctests in
individual modules.
"""
import os
import sys
import subprocess
import pydoc
import shutil
import tempfile
import pickle
import functools
import time
import datetime
from collections import Set

import py4j

from pyspark.graph.graph_element_data_frame import NodeDataFrame
from pyspark.graph.spark_cypher_session import SparkCypherSession

try:
    import xmlrunner
except ImportError:
    xmlrunner = None

if sys.version_info[:2] <= (2, 6):
    try:
        import unittest2 as unittest
    except ImportError:
        sys.stderr.write('Please install unittest2 to test with Python 2.6 or earlier')
        sys.exit(1)
else:
    import unittest

_have_pandas = False
try:
    import pandas
    _have_pandas = True
except:
    # No Pandas, but that's okay, we'll skip those tests
    pass

from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext, HiveContext, Column, Row
from pyspark.sql.types import *
from pyspark.sql.types import UserDefinedType, _infer_type
from pyspark.tests import ReusedPySparkTestCase, SparkSubmitTests
from pyspark.sql.functions import UserDefinedFunction, sha2, lit
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException, ParseException, IllegalArgumentException


class GraphTests(ReusedPySparkTestCase):

    @classmethod
    def setUpClass(cls):
        ReusedPySparkTestCase.setUpClass()
        cls.tempdir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(cls.tempdir.name)
        cls.spark = SparkSession(cls.sc)
        cls.cypherSession = SparkCypherSession(cls.spark)



@classmethod
    def tearDownClass(cls):
        ReusedPySparkTestCase.tearDownClass()
        cls.spark.stop()
        shutil.rmtree(cls.tempdir.name, ignore_errors=True)

    def test_match_single_node_pattern_using_spark_graph_api(self):
        nodeData = self.spark.createDataFrame([
            (bytearray("0"), "Alice"),
            (bytearray("1"), "Bob")
        ]).toDF("id", "name")
        nodeDataFrame = NodeDataFrame(nodeData, "id", {"Person"}, {"name": "name"})

        graph = self.cypherSession.createGraph([nodeDataFrame], [])

        graph.cypher("MATCH (n) RETURN n").df.show(20)


if __name__ == "__main__":
    from pyspark.graph.tests import *
    if xmlrunner:
        unittest.main(testRunner=xmlrunner.XMLTestRunner(output='target/test-reports'))
    else:
        unittest.main()