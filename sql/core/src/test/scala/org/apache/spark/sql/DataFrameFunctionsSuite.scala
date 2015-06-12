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

package org.apache.spark.sql

import org.apache.spark.sql.TestData._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Test suite for functions in [[org.apache.spark.sql.functions]].
 */
class DataFrameFunctionsSuite extends QueryTest {

  private lazy val ctx = org.apache.spark.sql.test.TestSQLContext
  import ctx.implicits._

  test("array with column name") {
    val df = Seq((0, 1)).toDF("a", "b")
    val row = df.select(array("a", "b")).first()

    val expectedType = ArrayType(IntegerType, containsNull = false)
    assert(row.schema(0).dataType === expectedType)
    assert(row.getAs[Seq[Int]](0) === Seq(0, 1))
  }

  test("array with column expression") {
    val df = Seq((0, 1)).toDF("a", "b")
    val row = df.select(array(col("a"), col("b") + col("b"))).first()

    val expectedType = ArrayType(IntegerType, containsNull = false)
    assert(row.schema(0).dataType === expectedType)
    assert(row.getAs[Seq[Int]](0) === Seq(0, 2))
  }

  // Turn this on once we add a rule to the analyzer to throw a friendly exception
  ignore("array: throw exception if putting columns of different types into an array") {
    val df = Seq((0, "str")).toDF("a", "b")
    intercept[AnalysisException] {
      df.select(array("a", "b"))
    }
  }

  test("struct with column name") {
    val df = Seq((1, "str")).toDF("a", "b")
    val row = df.select(struct("a", "b")).first()

    val expectedType = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("b", StringType)
    ))
    assert(row.schema(0).dataType === expectedType)
    assert(row.getAs[Row](0) === Row(1, "str"))
  }

  test("struct with column expression") {
    val df = Seq((1, "str")).toDF("a", "b")
    val row = df.select(struct((col("a") * 2).as("c"), col("b"))).first()

    val expectedType = StructType(Seq(
      StructField("c", IntegerType, nullable = false),
      StructField("b", StringType)
    ))
    assert(row.schema(0).dataType === expectedType)
    assert(row.getAs[Row](0) === Row(2, "str"))
  }

  test("struct: must use named column expression") {
    intercept[IllegalArgumentException] {
      struct(col("a") * 2)
    }
  }

  test("constant functions") {
    checkAnswer(
      testData2.select(e()).limit(1),
      Row(scala.math.E)
    )
    checkAnswer(
      testData2.select(pi()).limit(1),
      Row(scala.math.Pi)
    )
    checkAnswer(
      ctx.sql("SELECT E()"),
      Row(scala.math.E)
    )
    checkAnswer(
      ctx.sql("SELECT PI()"),
      Row(scala.math.Pi)
    )
  }

  test("bitwiseNOT") {
    checkAnswer(
      testData2.select(bitwiseNOT($"a")),
      testData2.collect().toSeq.map(r => Row(~r.getInt(0))))
  }

  test("if function") {
    val df = Seq((1, 2)).toDF("a", "b")
    checkAnswer(
      df.selectExpr("if(a = 1, 'one', 'not_one')", "if(b = 1, 'one', 'not_one')"),
      Row("one", "not_one"))
  }

  test("nvl function") {
    checkAnswer(
      ctx.sql("SELECT nvl(null, 'x'), nvl('y', 'x'), nvl(null, null)"),
      Row("x", "y", null))
  }

  test("string length function") {
    checkAnswer(
      nullStrings.select(strlen($"s"), strlen("s")),
      nullStrings.collect().toSeq.map { r =>
        val v = r.getString(1)
        val l = if (v == null) null else v.length
        Row(l, l)
      })

    checkAnswer(
      nullStrings.selectExpr("length(s)"),
      nullStrings.collect().toSeq.map { r =>
        val v = r.getString(1)
        val l = if (v == null) null else v.length
        Row(l)
      })
  }
}
