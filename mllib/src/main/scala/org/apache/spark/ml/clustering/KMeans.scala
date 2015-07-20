/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml.clustering

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.param.{Param, Params, IntParam, DoubleParam, ParamMap}
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasMaxIter, HasPredictionCol, HasSeed}
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib.clustering.{KMeans => MLlibKMeans, KMeansModel => MLlibKMeansModel}
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.Utils


/**
 * Common params for KMeans and KMeansModel
 */
private[clustering] trait KMeansParams
    extends Params with HasMaxIter with HasFeaturesCol with HasSeed with HasPredictionCol {

  /**
   * Set the number of clusters to create (k). Must be > 1. Default: 2.
   * @group param
   */
  final val k = new IntParam(this, "k", "number of clusters to create", (x: Int) => x > 1)

  /** @group getParam */
  def getK: Int = $(k)

  /**
   * Param the number of runs of the algorithm to execute in parallel. We initialize the algorithm
   * this many times with random starting conditions (configured by the initialization mode), then
   * return the best clustering found over any run. Must be >= 1. Default: 1.
   * @group param
   */
  final val runs = new IntParam(this, "runs",
    "number of runs of the algorithm to execute in parallel", (value: Int) => value >= 1)

  /** @group getParam */
  def getRuns: Int = $(runs)

  /**
   * Param the distance threshold within which we've consider centers to have converged.
   * If all centers move less than this Euclidean distance, we stop iterating one run.
   * Must be >= 0.0. Default: 1e-4
   * @group param
   */
  final val epsilon = new DoubleParam(this, "epsilon",
    "distance threshold within which we've consider centers to have converge",
    (value: Double) => value >= 0.0)

  /** @group getParam */
  def getEpsilon: Double = $(epsilon)

  /**
   * Param for the initialization algorithm. This can be either "random" to choose random points as
   * initial cluster centers, or "k-means||" to use a parallel variant of k-means++
   * (Bahmani et al., Scalable K-Means++, VLDB 2012). Default: k-means||.
   * @group expertParam
   */
  final val initMode = new Param[String](this, "initMode", "initialization algorithm",
    (value: String) => MLlibKMeans.validateInitMode(value))

  /** @group expertGetParam */
  def getInitMode: String = $(initMode)

  /**
   * Param for the number of steps for the k-means|| initialization mode. This is an advanced
   * setting -- the default of 5 is almost always enough. Must be > 0. Default: 5.
   * @group expertParam
   */
  final val initSteps = new IntParam(this, "initSteps", "number of steps for k-means||",
    (value: Int) => value > 0)

  /** @group expertGetParam */
  def getInitSteps: Int = $(initSteps)

  /**
   * Validates and transforms the input schema.
   * @param schema input schema
   * @return output schema
   */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    SchemaUtils.appendColumn(schema, $(predictionCol), IntegerType)
  }
}

/**
 * :: Experimental ::
 * Model fitted by KMeans.
 *
 * @param parentModel a model trained by spark.mllib.clustering.KMeans.
 */
@Experimental
class KMeansModel private[ml] (
    override val uid: String,
    private val parentModel: MLlibKMeansModel) extends Model[KMeansModel] with KMeansParams {

  override def copy(extra: ParamMap): KMeansModel = {
    val copied = new KMeansModel(uid, parentModel)
    copyValues(copied, extra)
  }

  override def transform(dataset: DataFrame): DataFrame = {
    val predictUDF = udf((vector: Vector) => predict(vector))
    dataset.withColumn($(predictionCol), predictUDF(col($(featuresCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  private[clustering] def predict(features: Vector): Int = parentModel.predict(features)

  def clusterCenters: Array[Vector] = parentModel.clusterCenters
}

/**
 * :: Experimental ::
 * K-means clustering with support for multiple parallel runs and a k-means++ like initialization
 * mode (the k-means|| algorithm by Bahmani et al). When multiple concurrent runs are requested,
 * they are executed together with joint passes over the data for efficiency.
 */
@Experimental
class KMeans(override val uid: String) extends Estimator[KMeansModel] with KMeansParams {

  setDefault(
    k -> 2,
    maxIter -> 20,
    runs -> 1,
    initMode -> MLlibKMeans.K_MEANS_PARALLEL,
    initSteps -> 5,
    epsilon -> 1e-4)

  override def copy(extra: ParamMap): KMeans = defaultCopy(extra)

  def this() = this(Identifiable.randomUID("kmeans"))

  /** @group setParam */
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  def setK(value: Int): this.type = set(k, value)

  /** @group expertSetParam */
  def setInitMode(value: String): this.type = set(initMode, value)

  /** @group expertSetParam */
  def setInitSteps(value: Int): this.type = set(initSteps, value)

  /** @group setParam */
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  def setRuns(value: Int): this.type = set(runs, value)

  /** @group setParam */
  def setEpsilon(value: Double): this.type = set(epsilon, value)

  /** @group setParam */
  def setSeed(value: Long): this.type = set(seed, value)

  override def fit(dataset: DataFrame): KMeansModel = {
    val rdd = dataset.select(col($(featuresCol))).map { case Row(point: Vector) => point }

    val algo = new MLlibKMeans()
      .setK($(k))
      .setInitializationMode($(initMode))
      .setInitializationSteps($(initSteps))
      .setMaxIterations($(maxIter))
      .setSeed($(seed))
      .setEpsilon($(epsilon))
      .setRuns($(runs))
    val parentModel = algo.run(rdd)
    val model = new KMeansModel(uid, parentModel)
    copyValues(model)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}

