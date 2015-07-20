#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pyspark.ml.util import keyword_only
from pyspark.ml.wrapper import JavaEstimator, JavaModel
from pyspark.ml.param.shared import *
from pyspark.mllib.common import inherit_doc
from pyspark.mllib.linalg import _convert_to_vector

__all__ = ['KMeans', 'KMeansModel']


class KMeansModel(JavaModel):
    """
    Model fitted by KMeans.
    """

    def clusterCenters(self):
        """Get the cluster centers, represented as a list of NumPy arrays."""
        return [c.toArray() for c in self._call_java("clusterCenters")]


@inherit_doc
class KMeans(JavaEstimator, HasFeaturesCol, HasMaxIter, HasSeed):
    """
    K-means Clustering

    >>> from pyspark.mllib.linalg import Vectors
    >>> data = [(Vectors.dense([0.0, 0.0]),), (Vectors.dense([1.0, 1.0]),),
    ...         (Vectors.dense([9.0, 8.0]),), (Vectors.dense([8.0, 9.0]),)]
    >>> df = sqlContext.createDataFrame(data, ["features"])
    >>> kmeans = KMeans().setK(2).setSeed(1).setFeaturesCol("features")
    >>> model = kmeans.fit(df)
    >>> centers = model.clusterCenters()
    >>> len(centers)
    2
    >>> transformed = model.transform(df).select("features", "prediction")
    >>> rows = transformed.collect()
    >>> rows[0].prediction == rows[1].prediction
    True
    >>> rows[2].prediction == rows[3].prediction
    True
    """

    # a placeholder to make it appear in the generated doc
    k = Param(Params._dummy(), "k", "number of clusters to create")
    epsilon = Param(Params._dummy(), "epsilon",
                    "distance threshold within which " +
                    "we've consider centers to have converged")
    runs = Param(Params._dummy(), "runs", "number of runs of the algorithm to execute in parallel")
    initMode = Param(Params._dummy(), "initMode",
                     "the initialization algorithm. This can be either \"random\" to " +
                     "choose random points as initial cluster centers, or \"k-means||\" " +
                     "to use a parallel variant of k-means++")
    initSteps = Param(Params._dummy(), "initSteps", "steps for k-means initialization mode")

    @keyword_only
    def __init__(self, k=2, maxIter=20, runs=1, epsilon=1e-4, initMode="k-means||", initStep=5):
        super(KMeans, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.clustering.KMeans", self.uid)
        self.k = Param(self, "k", "number of clusters to create")
        self.epsilon = Param(self, "epsilon",
                             "distance threshold within which " +
                             "we've consider centers to have converged")
        self.runs = Param(self, "runs", "number of runs of the algorithm to execute in parallel")
        self.seed = Param(self, "seed", "random seed")
        self.initMode = Param(self, "initMode",
                              "the initialization algorithm. This can be either \"random\" to " +
                              "choose random points as initial cluster centers, or \"k-means||\" " +
                              "to use a parallel variant of k-means++")
        self.initSteps = Param(self, "initSteps", "steps for k-means initialization mode")
        self._setDefault(k=2, maxIter=20, runs=1, epsilon=1e-4, initMode="k-means||", initSteps=5)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    def _create_model(self, java_model):
        return KMeansModel(java_model)

    @keyword_only
    def setParams(self, k=2, maxIter=20, runs=1, epsilon=1e-4, initMode="k-means||", initSteps=5):
        """
        setParams(self, k=2, maxIter=20, runs=1, epsilon=1e-4, initMode="k-means||", initSteps=5):

        Sets params for KMeans.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def setK(self, value):
        """
        Sets the value of :py:attr:`k`.

        >>> algo = KMeans().setK(10)
        >>> algo.getK()
        10
        """
        self._paramMap[self.k] = value
        return self

    def getK(self):
        """
        Gets the value of `k`
        """
        return self.getOrDefault(self.k)

    def setEpsilon(self, value):
        """
        Sets the value of :py:attr:`epsilon`.

        >>> algo = KMeans().setEpsilon(1e-5)
        >>> abs(algo.getEpsilon() - 1e-5) < 1e-5
        True
        """
        self._paramMap[self.epsilon] = value
        return self

    def getEpsilon(self):
        """
        Gets the value of `epsilon`
        """
        return self.getOrDefault(self.epsilon)

    def setRuns(self, value):
        """
        Sets the value of :py:attr:`runs`.

        >>> algo = KMeans().setRuns(10)
        >>> algo.getRuns()
        10
        """
        self._paramMap[self.runs] = value
        return self

    def getRuns(self):
        """
        Gets the value of `runs`
        """
        return self.getOrDefault(self.runs)

    def setInitMode(self, value):
        """
        Sets the value of :py:attr:`initMode`.

        >>> algo = KMeans()
        >>> algo.getInitMode()
        'k-means||'
        >>> algo = algo.setInitMode("random")
        >>> algo.getInitMode()
        'random'
        """
        self._paramMap[self.initMode] = value
        return self

    def getInitMode(self):
        """
        Gets the value of `initMode`
        """
        return self.getOrDefault(self.initMode)

    def setInitSteps(self, value):
        """
        Sets the value of :py:attr:`initSteps`.

        >>> algo = KMeans().setInitSteps(10)
        >>> algo.getInitSteps()
        10
        """
        self._paramMap[self.initSteps] = value
        return self

    def getInitSteps(self):
        """
        Gets the value of `initSteps`
        """
        return self.getOrDefault(self.initSteps)


if __name__ == "__main__":
    import doctest
    from pyspark.context import SparkContext
    from pyspark.sql import SQLContext
    globs = globals().copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    sc = SparkContext("local[2]", "ml.clustering tests")
    sqlContext = SQLContext(sc)
    globs['sc'] = sc
    globs['sqlContext'] = sqlContext
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    sc.stop()
    if failure_count:
        exit(-1)
