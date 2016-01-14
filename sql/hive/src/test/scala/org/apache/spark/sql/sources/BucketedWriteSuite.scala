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

package org.apache.spark.sql.sources

import java.io.File

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

class BucketedWriteSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import testImplicits._

  test("bucketed by non-existing column") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")
    intercept[AnalysisException](df.write.bucketBy(2, "k").saveAsTable("tt"))
  }

  test("numBuckets not greater than 0 or less than 100000") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")
    intercept[IllegalArgumentException](df.write.bucketBy(0, "i").saveAsTable("tt"))
    intercept[IllegalArgumentException](df.write.bucketBy(100000, "i").saveAsTable("tt"))
  }

  test("specify sorting columns without bucketing columns") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")
    intercept[IllegalArgumentException](df.write.sortBy("j").saveAsTable("tt"))
  }

  test("sorting by non-orderable column") {
    val df = Seq("a" -> Map(1 -> 1), "b" -> Map(2 -> 2)).toDF("i", "j")
    intercept[AnalysisException](df.write.bucketBy(2, "i").sortBy("j").saveAsTable("tt"))
  }

  test("write bucketed data to unsupported data source") {
    val df = Seq(Tuple1("a"), Tuple1("b")).toDF("i")
    intercept[AnalysisException](df.write.bucketBy(3, "i").format("text").saveAsTable("tt"))
  }

  test("write bucketed data to non-hive-table or existing hive table") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")
    intercept[IllegalArgumentException](df.write.bucketBy(2, "i").parquet("/tmp/path"))
    intercept[IllegalArgumentException](df.write.bucketBy(2, "i").json("/tmp/path"))
    intercept[IllegalArgumentException](df.write.bucketBy(2, "i").insertInto("tt"))
  }

  private val testFileName = """.*-(\d+)$""".r
  private val otherFileName = """.*-(\d+)\..*""".r
  private def getBucketId(fileName: String): Int = {
    fileName match {
      case testFileName(bucketId) => bucketId.toInt
      case otherFileName(bucketId) => bucketId.toInt
    }
  }

  private val df = (0 until 50).map(i => (i % 5, i % 13, i.toString)).toDF("i", "j", "k")

  private def testBucketing(
      dataDir: File,
      source: String,
      bucketCols: Seq[String],
      sortCols: Seq[String] = Nil): Unit = {
    val allBucketFiles = dataDir.listFiles().filterNot(f =>
      f.getName.startsWith(".") || f.getName.startsWith("_")
    )
    val groupedBucketFiles = allBucketFiles.groupBy(f => getBucketId(f.getName))
    assert(groupedBucketFiles.size <= 8)

    for ((bucketId, bucketFiles) <- groupedBucketFiles) {
      for (bucketFilePath <- bucketFiles.map(_.getAbsolutePath)) {
        val types = df.select((bucketCols ++ sortCols).map(col): _*).schema.map(_.dataType)
        val columns = (bucketCols ++ sortCols).zip(types).map {
          case (colName, dt) => col(colName).cast(dt)
        }
        val readBack = sqlContext.read.format(source).load(bucketFilePath).select(columns: _*)

        if (sortCols.nonEmpty) {
          checkAnswer(readBack.sort(sortCols.map(col): _*), readBack.collect())
        }

        val qe = readBack.select(bucketCols.map(col): _*).queryExecution
        val rows = qe.toRdd.map(_.copy()).collect()
        val getHashCode = UnsafeProjection.create(
          HashPartitioning(qe.analyzed.output, 8).partitionIdExpression :: Nil,
          qe.analyzed.output)

        for (row <- rows) {
          val actualBucketId = getHashCode(row).getInt(0)
          assert(actualBucketId == bucketId)
        }
      }
    }
  }

  test("write bucketed data") {
    for (source <- Seq("parquet", "json", "orc")) {
      withTable("bucketed_table") {
        df.write
          .format(source)
          .partitionBy("i")
          .bucketBy(8, "j", "k")
          .saveAsTable("bucketed_table")

        val tableDir = new File(hiveContext.warehousePath, "bucketed_table")
        for (i <- 0 until 5) {
          testBucketing(new File(tableDir, s"i=$i"), source, Seq("j", "k"))
        }
      }
    }
  }

  test("write bucketed data with sortBy") {
    for (source <- Seq("parquet", "json", "orc")) {
      withTable("bucketed_table") {
        df.write
          .format(source)
          .partitionBy("i")
          .bucketBy(8, "j")
          .sortBy("k")
          .saveAsTable("bucketed_table")

        val tableDir = new File(hiveContext.warehousePath, "bucketed_table")
        for (i <- 0 until 5) {
          testBucketing(new File(tableDir, s"i=$i"), source, Seq("j"), Seq("k"))
        }
      }
    }
  }

  test("write bucketed data without partitionBy") {
    for (source <- Seq("parquet", "json", "orc")) {
      withTable("bucketed_table") {
        df.write
          .format(source)
          .bucketBy(8, "i", "j")
          .saveAsTable("bucketed_table")

        val tableDir = new File(hiveContext.warehousePath, "bucketed_table")
        testBucketing(tableDir, source, Seq("i", "j"))
      }
    }
  }

  test("write bucketed data without partitionBy with sortBy") {
    for (source <- Seq("parquet", "json", "orc")) {
      withTable("bucketed_table") {
        df.write
          .format(source)
          .bucketBy(8, "i", "j")
          .sortBy("k")
          .saveAsTable("bucketed_table")

        val tableDir = new File(hiveContext.warehousePath, "bucketed_table")
        testBucketing(tableDir, source, Seq("i", "j"), Seq("k"))
      }
    }
  }
}
