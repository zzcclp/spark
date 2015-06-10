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


case class FunctionResult(f1: String, f2: String)

class UDFSuite extends QueryTest {

  private lazy val ctx = org.apache.spark.sql.test.TestSQLContext
  import ctx.implicits._

  test("built-in fixed arity expressions") {
    val df = ctx.emptyDataFrame
    df.selectExpr("rand()", "randn()", "rand(5)", "randn(50)")
  }

  test("built-in vararg expressions") {
    val df = Seq((1, 2)).toDF("a", "b")
    df.selectExpr("array(a, b)")
    df.selectExpr("struct(a, b)")
  }

  test("built-in expressions with multiple constructors") {
    val df = Seq(("abcd", 2)).toDF("a", "b")
    df.selectExpr("substr(a, 2)", "substr(a, 2, 3)").collect()
  }

  test("count") {
    val df = Seq(("abcd", 2)).toDF("a", "b")
    df.selectExpr("count(a)")
  }

  test("count distinct") {
    val df = Seq(("abcd", 2)).toDF("a", "b")
    df.selectExpr("count(distinct a)")
  }

  test("error reporting for incorrect number of arguments") {
    val df = ctx.emptyDataFrame
    val e = intercept[AnalysisException] {
      df.selectExpr("substr('abcd', 2, 3, 4)")
    }
    assert(e.getMessage.contains("arguments"))
  }

  test("error reporting for undefined functions") {
    val df = ctx.emptyDataFrame
    val e = intercept[AnalysisException] {
      df.selectExpr("a_function_that_does_not_exist()")
    }
    assert(e.getMessage.contains("undefined function"))
  }

  test("Simple UDF") {
    ctx.udf.register("strLenScala", (_: String).length)
    assert(ctx.sql("SELECT strLenScala('test')").head().getInt(0) === 4)
  }

  test("ZeroArgument UDF") {
    ctx.udf.register("random0", () => { Math.random()})
    assert(ctx.sql("SELECT random0()").head().getDouble(0) >= 0.0)
  }

  test("TwoArgument UDF") {
    ctx.udf.register("strLenScala", (_: String).length + (_: Int))
    assert(ctx.sql("SELECT strLenScala('test', 1)").head().getInt(0) === 5)
  }

  test("struct UDF") {
    ctx.udf.register("returnStruct", (f1: String, f2: String) => FunctionResult(f1, f2))

    val result =
      ctx.sql("SELECT returnStruct('test', 'test2') as ret")
        .select($"ret.f1").head().getString(0)
    assert(result === "test")
  }

  test("udf that is transformed") {
    ctx.udf.register("makeStruct", (x: Int, y: Int) => (x, y))
    // 1 + 1 is constant folded causing a transformation.
    assert(ctx.sql("SELECT makeStruct(1 + 1, 2)").first().getAs[Row](0) === Row(2, 2))
  }
}
