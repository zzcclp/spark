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

package org.apache.spark.ml.feature

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.BinaryAttribute
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructType}

/**
 * :: AlphaComponent ::
 * Binarize a column of continuous features given a threshold.
 */
@AlphaComponent
final class Binarizer extends Transformer with HasInputCol with HasOutputCol {

  /**
   * Param for threshold used to binarize continuous features.
   * The features greater than the threshold, will be binarized to 1.0.
   * The features equal to or less than the threshold, will be binarized to 0.0.
   * @group param
   */
  val threshold: DoubleParam =
    new DoubleParam(this, "threshold", "threshold used to binarize continuous features")

  /** @group getParam */
  def getThreshold: Double = getOrDefault(threshold)

  /** @group setParam */
  def setThreshold(value: Double): this.type = set(threshold, value)

  setDefault(threshold -> 0.0)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: DataFrame, paramMap: ParamMap): DataFrame = {
    transformSchema(dataset.schema, paramMap, logging = true)
    val map = extractParamMap(paramMap)
    val td = map(threshold)
    val binarizer = udf { in: Double => if (in > td) 1.0 else 0.0 }
    val outputColName = map(outputCol)
    val metadata = BinaryAttribute.defaultAttr.withName(outputColName).toMetadata()
    dataset.select(col("*"),
      binarizer(col(map(inputCol))).as(outputColName, metadata))
  }

  override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    val map = extractParamMap(paramMap)
    SchemaUtils.checkColumnType(schema, map(inputCol), DoubleType)

    val inputFields = schema.fields
    val outputColName = map(outputCol)

    require(inputFields.forall(_.name != outputColName),
      s"Output column $outputColName already exists.")

    val attr = BinaryAttribute.defaultAttr.withName(outputColName)
    val outputFields = inputFields :+ attr.toStructField()
    StructType(outputFields)
  }
}
