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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types.{IntegerType, DataType, DoubleType, LongType}

class MathFunctionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  /**
   * Used for testing leaf math expressions.
   *
   * @param e expression
   * @param c The constants in scala.math
   * @tparam T Generic type for primitives
   */
  private def testLeaf[T](
      e: () => Expression,
      c: T): Unit = {
    checkEvaluation(e(), c, EmptyRow)
    checkEvaluation(e(), c, create_row(null))
  }

  /**
   * Used for testing unary math expressions.
   *
   * @param c expression
   * @param f The functions in scala.math or elsewhere used to generate expected results
   * @param domain The set of values to run the function with
   * @param expectNull Whether the given values should return null or not
   * @tparam T Generic type for primitives
   * @tparam U Generic type for the output of the given function `f`
   */
  private def testUnary[T, U](
      c: Expression => Expression,
      f: T => U,
      domain: Iterable[T] = (-20 to 20).map(_ * 0.1),
      expectNull: Boolean = false,
      evalType: DataType = DoubleType): Unit = {
    if (expectNull) {
      domain.foreach { value =>
        checkEvaluation(c(Literal(value)), null, EmptyRow)
      }
    } else {
      domain.foreach { value =>
        checkEvaluation(c(Literal(value)), f(value), EmptyRow)
      }
    }
    checkEvaluation(c(Literal.create(null, evalType)), null, create_row(null))
  }

  /**
   * Used for testing binary math expressions.
   *
   * @param c The DataFrame function
   * @param f The functions in scala.math
   * @param domain The set of values to run the function with
   */
  private def testBinary(
      c: (Expression, Expression) => Expression,
      f: (Double, Double) => Double,
      domain: Iterable[(Double, Double)] = (-20 to 20).map(v => (v * 0.1, v * -0.1)),
      expectNull: Boolean = false): Unit = {
    if (expectNull) {
      domain.foreach { case (v1, v2) =>
        checkEvaluation(c(Literal(v1), Literal(v2)), null, create_row(null))
      }
    } else {
      domain.foreach { case (v1, v2) =>
        checkEvaluation(c(Literal(v1), Literal(v2)), f(v1 + 0.0, v2 + 0.0), EmptyRow)
        checkEvaluation(c(Literal(v2), Literal(v1)), f(v2 + 0.0, v1 + 0.0), EmptyRow)
      }
    }
    checkEvaluation(c(Literal.create(null, DoubleType), Literal(1.0)), null, create_row(null))
    checkEvaluation(c(Literal(1.0), Literal.create(null, DoubleType)), null, create_row(null))
  }

  test("e") {
    testLeaf(EulerNumber, math.E)
  }

  test("pi") {
    testLeaf(Pi, math.Pi)
  }

  test("sin") {
    testUnary(Sin, math.sin)
  }

  test("asin") {
    testUnary(Asin, math.asin, (-10 to 10).map(_ * 0.1))
    testUnary(Asin, math.asin, (11 to 20).map(_ * 0.1), expectNull = true)
  }

  test("sinh") {
    testUnary(Sinh, math.sinh)
  }

  test("cos") {
    testUnary(Cos, math.cos)
  }

  test("acos") {
    testUnary(Acos, math.acos, (-10 to 10).map(_ * 0.1))
    testUnary(Acos, math.acos, (11 to 20).map(_ * 0.1), expectNull = true)
  }

  test("cosh") {
    testUnary(Cosh, math.cosh)
  }

  test("tan") {
    testUnary(Tan, math.tan)
  }

  test("atan") {
    testUnary(Atan, math.atan)
  }

  test("tanh") {
    testUnary(Tanh, math.tanh)
  }

  test("toDegrees") {
    testUnary(ToDegrees, math.toDegrees)
  }

  test("toRadians") {
    testUnary(ToRadians, math.toRadians)
  }

  test("cbrt") {
    testUnary(Cbrt, math.cbrt)
  }

  test("ceil") {
    testUnary(Ceil, math.ceil)
  }

  test("floor") {
    testUnary(Floor, math.floor)
  }

  test("rint") {
    testUnary(Rint, math.rint)
  }

  test("exp") {
    testUnary(Exp, math.exp)
  }

  test("expm1") {
    testUnary(Expm1, math.expm1)
  }

  test("signum") {
    testUnary[Double, Double](Signum, math.signum)
  }

  test("log") {
    testUnary(Log, math.log, (0 to 20).map(_ * 0.1))
    testUnary(Log, math.log, (-5 to -1).map(_ * 0.1), expectNull = true)
  }

  test("log10") {
    testUnary(Log10, math.log10, (0 to 20).map(_ * 0.1))
    testUnary(Log10, math.log10, (-5 to -1).map(_ * 0.1), expectNull = true)
  }

  test("log1p") {
    testUnary(Log1p, math.log1p, (-1 to 20).map(_ * 0.1))
    testUnary(Log1p, math.log1p, (-10 to -2).map(_ * 1.0), expectNull = true)
  }

  test("bin") {
    testUnary(Bin, java.lang.Long.toBinaryString, (-20 to 20).map(_.toLong), evalType = LongType)

    val row = create_row(null, 12L, 123L, 1234L, -123L)
    val l1 = 'a.long.at(0)
    val l2 = 'a.long.at(1)
    val l3 = 'a.long.at(2)
    val l4 = 'a.long.at(3)
    val l5 = 'a.long.at(4)

    checkEvaluation(Bin(l1), null, row)
    checkEvaluation(Bin(l2), java.lang.Long.toBinaryString(12), row)
    checkEvaluation(Bin(l3), java.lang.Long.toBinaryString(123), row)
    checkEvaluation(Bin(l4), java.lang.Long.toBinaryString(1234), row)
    checkEvaluation(Bin(l5), java.lang.Long.toBinaryString(-123), row)
  }

  test("log2") {
    def f: (Double) => Double = (x: Double) => math.log(x) / math.log(2)
    testUnary(Log2, f, (0 to 20).map(_ * 0.1))
    testUnary(Log2, f, (-5 to -1).map(_ * 1.0), expectNull = true)
  }

  test("sqrt") {
    testUnary(Sqrt, math.sqrt, (0 to 20).map(_ * 0.1))
    testUnary(Sqrt, math.sqrt, (-5 to -1).map(_ * 1.0), expectNull = true)

    checkEvaluation(Sqrt(Literal.create(null, DoubleType)), null, create_row(null))
    checkEvaluation(Sqrt(Literal(-1.0)), null, EmptyRow)
    checkEvaluation(Sqrt(Literal(-1.5)), null, EmptyRow)
  }

  test("pow") {
    testBinary(Pow, math.pow, (-5 to 5).map(v => (v * 1.0, v * 1.0)))
    testBinary(Pow, math.pow, Seq((-1.0, 0.9), (-2.2, 1.7), (-2.2, -1.7)), expectNull = true)
  }

  test("shift left") {
    checkEvaluation(ShiftLeft(Literal.create(null, IntegerType), Literal(1)), null)
    checkEvaluation(ShiftLeft(Literal(21), Literal.create(null, IntegerType)), null)
    checkEvaluation(
      ShiftLeft(Literal.create(null, IntegerType), Literal.create(null, IntegerType)), null)
    checkEvaluation(ShiftLeft(Literal(21), Literal(1)), 42)
    checkEvaluation(ShiftLeft(Literal(21.toByte), Literal(1)), 42)
    checkEvaluation(ShiftLeft(Literal(21.toShort), Literal(1)), 42)
    checkEvaluation(ShiftLeft(Literal(21.toLong), Literal(1)), 42.toLong)

    checkEvaluation(ShiftLeft(Literal(-21.toLong), Literal(1)), -42.toLong)
  }

  test("shift right") {
    checkEvaluation(ShiftRight(Literal.create(null, IntegerType), Literal(1)), null)
    checkEvaluation(ShiftRight(Literal(42), Literal.create(null, IntegerType)), null)
    checkEvaluation(
      ShiftRight(Literal.create(null, IntegerType), Literal.create(null, IntegerType)), null)
    checkEvaluation(ShiftRight(Literal(42), Literal(1)), 21)
    checkEvaluation(ShiftRight(Literal(42.toByte), Literal(1)), 21)
    checkEvaluation(ShiftRight(Literal(42.toShort), Literal(1)), 21)
    checkEvaluation(ShiftRight(Literal(42.toLong), Literal(1)), 21.toLong)

    checkEvaluation(ShiftRight(Literal(-42.toLong), Literal(1)), -21.toLong)
  }

  test("hex") {
    checkEvaluation(Hex(Literal(28)), "1C")
    checkEvaluation(Hex(Literal(-28)), "FFFFFFFFFFFFFFE4")
    checkEvaluation(Hex(Literal(100800200404L)), "177828FED4")
    checkEvaluation(Hex(Literal(-100800200404L)), "FFFFFFE887D7012C")
    checkEvaluation(Hex(Literal("helloHex")), "68656C6C6F486578")
    checkEvaluation(Hex(Literal("helloHex".getBytes())), "68656C6C6F486578")
    // scalastyle:off
    // Turn off scala style for non-ascii chars
    checkEvaluation(Hex(Literal("三重的")), "E4B889E9878DE79A84")
    // scalastyle:on
  }

  test("unhex") {
    checkEvaluation(UnHex(Literal("737472696E67")), "string".getBytes)
    checkEvaluation(UnHex(Literal("")), new Array[Byte](0))
    checkEvaluation(UnHex(Literal("0")), Array[Byte](0))
  }

  test("hypot") {
    testBinary(Hypot, math.hypot)
  }

  test("atan2") {
    testBinary(Atan2, math.atan2)
  }

  test("binary log") {
    val f = (c1: Double, c2: Double) => math.log(c2) / math.log(c1)
    val domain = (1 to 20).map(v => (v * 0.1, v * 0.2))

    domain.foreach { case (v1, v2) =>
      checkEvaluation(Logarithm(Literal(v1), Literal(v2)), f(v1 + 0.0, v2 + 0.0), EmptyRow)
      checkEvaluation(Logarithm(Literal(v2), Literal(v1)), f(v2 + 0.0, v1 + 0.0), EmptyRow)
      checkEvaluation(new Logarithm(Literal(v1)), f(math.E, v1 + 0.0), EmptyRow)
    }
    checkEvaluation(
      Logarithm(Literal.create(null, DoubleType), Literal(1.0)),
      null,
      create_row(null))
    checkEvaluation(
      Logarithm(Literal(1.0), Literal.create(null, DoubleType)),
      null,
      create_row(null))
  }
}
