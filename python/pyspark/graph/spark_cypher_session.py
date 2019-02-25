from pyspark.graph.property_graph import PropertyGraph


class SparkCypherSession(object):


    def __init__(self, sparkSession):
        self.sparkSession = sparkSession
        self._jvm = sparkSession._jvm
        self._jSparkCypherSession = self._jvm.org.apache.spark.graph.cypher.SparkCypherSession(sparkSession._jsparkSession)

    def createGraph(self, nodes, relationships):
        jNodeDataFrames = [self._createJNodeDataFrame(node) for node in nodes]

        jRelationshipDataFrames = [self._createJRelationshipDataFrame(relationship) for relationship in relationships]

        jGraph = self._jSparkCypherSession.createGraph(self._toSeq(jNodeDataFrames), self._toSeq(jRelationshipDataFrames))
        return PropertyGraph(jGraph, self)


    def cypher(self, graph, query):
        return graph.cypher(query)


    def _createJNodeDataFrame(self, node):
        return self._jvm.org.apache.spark.graph.api.NodeDataFrame(
            node.df._jdf,
            node.idColumn,
            self._toSet(node.labels),
            self._toMap(node.properties),
            self._toMap(node.optionalLabels)
        )

    def _createJRelationshipDataFrame(self, relationship):
        return self._jvm.org.apache.spark.graph.api.RelationshipDataFrame(
            relationship.df._jdf,
            relationship.idColumn,
            relationship.sourceIdColumn,
            relationship.targetIdColumn,
            self._toSet(relationship.labels),
            self._toMap(relationship.properties)
        )

    def _toSet(self, set):
        return self._jvm.PythonUtils.toSet(set)

    def _toMap(self, map):
        return self._jvm.PythonUtils.toScalaMap(map)

    def _toSeq(self, seq):
        return self._jvm.PythonUtils.toSeq(seq)

    def _toList(self, list):
        return self._jvm.PythonUtils.toList(list)