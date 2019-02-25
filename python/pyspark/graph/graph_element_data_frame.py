
class NodeDataFrame(object):

    def __init__(self, df, idColumn, labels = None, properties = None, optionalLabels = None):

        self.df = df
        self.idColumn = idColumn

        if labels is None:
            labels = {}
        self.labels = labels

        if properties is None:
            properties = {}
        self.properties = properties

        if optionalLabels is None:
            optionalLabels = {}
        self.optionalLabels = optionalLabels


class RelationshipDataFrame(object):

    def __init__(self, df, idColumn, sourceIdColumn, targetIdColumn, labels = None, properties = None):

        self.df = df
        self.idColumn = idColumn
        self.sourceIdColumn = sourceIdColumn
        self.targetIdColumn = targetIdColumn

        if labels is None:
            labels = {}
        self.labels = labels

        if properties is None:
            properties = {}
        self.properties = properties

