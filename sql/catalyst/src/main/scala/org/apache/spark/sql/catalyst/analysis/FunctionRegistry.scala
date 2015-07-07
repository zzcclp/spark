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

package org.apache.spark.sql.catalyst.analysis

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.StringKeyHashMap


/** A catalog for looking up user defined functions, used by an [[Analyzer]]. */
trait FunctionRegistry {

  def registerFunction(name: String, builder: FunctionBuilder): Unit

  @throws[AnalysisException]("If function does not exist")
  def lookupFunction(name: String, children: Seq[Expression]): Expression
}

class OverrideFunctionRegistry(underlying: FunctionRegistry) extends FunctionRegistry {

  private val functionBuilders = StringKeyHashMap[FunctionBuilder](caseSensitive = false)

  override def registerFunction(name: String, builder: FunctionBuilder): Unit = {
    functionBuilders.put(name, builder)
  }

  override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    functionBuilders.get(name).map(_(children)).getOrElse(underlying.lookupFunction(name, children))
  }
}

class SimpleFunctionRegistry extends FunctionRegistry {

  private val functionBuilders = StringKeyHashMap[FunctionBuilder](caseSensitive = false)

  override def registerFunction(name: String, builder: FunctionBuilder): Unit = {
    functionBuilders.put(name, builder)
  }

  override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    val func = functionBuilders.get(name).getOrElse {
      throw new AnalysisException(s"undefined function $name")
    }
    func(children)
  }
}

/**
 * A trivial catalog that returns an error when a function is requested. Used for testing when all
 * functions are already filled in and the analyzer needs only to resolve attribute references.
 */
object EmptyFunctionRegistry extends FunctionRegistry {
  override def registerFunction(name: String, builder: FunctionBuilder): Unit = {
    throw new UnsupportedOperationException
  }

  override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    throw new UnsupportedOperationException
  }
}


object FunctionRegistry {

  type FunctionBuilder = Seq[Expression] => Expression

  val expressions: Map[String, FunctionBuilder] = Map(
    // misc non-aggregate functions
    expression[Abs]("abs"),
    expression[CreateArray]("array"),
    expression[Coalesce]("coalesce"),
    expression[Explode]("explode"),
    expression[If]("if"),
    expression[IsNull]("isnull"),
    expression[IsNotNull]("isnotnull"),
    expression[Coalesce]("nvl"),
    expression[Rand]("rand"),
    expression[Randn]("randn"),
    expression[CreateStruct]("struct"),
    expression[CreateNamedStruct]("named_struct"),
    expression[Sqrt]("sqrt"),

    // math functions
    expression[Acos]("acos"),
    expression[Asin]("asin"),
    expression[Atan]("atan"),
    expression[Atan2]("atan2"),
    expression[Bin]("bin"),
    expression[Cbrt]("cbrt"),
    expression[Ceil]("ceil"),
    expression[Ceil]("ceiling"),
    expression[Cos]("cos"),
    expression[EulerNumber]("e"),
    expression[Exp]("exp"),
    expression[Expm1]("expm1"),
    expression[Floor]("floor"),
    expression[Factorial]("factorial"),
    expression[Hypot]("hypot"),
    expression[Hex]("hex"),
    expression[Logarithm]("log"),
    expression[Log]("ln"),
    expression[Log10]("log10"),
    expression[Log1p]("log1p"),
    expression[UnaryMinus]("negative"),
    expression[Pi]("pi"),
    expression[Log2]("log2"),
    expression[Pow]("pow"),
    expression[Pow]("power"),
    expression[UnaryPositive]("positive"),
    expression[Rint]("rint"),
    expression[ShiftLeft]("shiftleft"),
    expression[ShiftRight]("shiftright"),
    expression[ShiftRightUnsigned]("shiftrightunsigned"),
    expression[Signum]("sign"),
    expression[Signum]("signum"),
    expression[Sin]("sin"),
    expression[Sinh]("sinh"),
    expression[Tan]("tan"),
    expression[Tanh]("tanh"),
    expression[ToDegrees]("degrees"),
    expression[ToRadians]("radians"),

    // misc functions
    expression[Md5]("md5"),
    expression[Sha2]("sha2"),
    expression[Sha1]("sha1"),
    expression[Sha1]("sha"),
    expression[Crc32]("crc32"),

    // aggregate functions
    expression[Average]("avg"),
    expression[Count]("count"),
    expression[First]("first"),
    expression[Last]("last"),
    expression[Max]("max"),
    expression[Min]("min"),
    expression[Sum]("sum"),

    // string functions
    expression[Ascii]("ascii"),
    expression[Base64]("base64"),
    expression[Encode]("encode"),
    expression[Decode]("decode"),
    expression[Lower]("lcase"),
    expression[Lower]("lower"),
    expression[StringLength]("length"),
    expression[Levenshtein]("levenshtein"),
    expression[Substring]("substr"),
    expression[Substring]("substring"),
    expression[UnBase64]("unbase64"),
    expression[Upper]("ucase"),
    expression[Unhex]("unhex"),
    expression[Upper]("upper"),

    // datetime functions
    expression[CurrentDate]("current_date"),
    expression[CurrentTimestamp]("current_timestamp")
  )

  val builtin: FunctionRegistry = {
    val fr = new SimpleFunctionRegistry
    expressions.foreach { case (name, builder) => fr.registerFunction(name, builder) }
    fr
  }

  /** See usage above. */
  private def expression[T <: Expression](name: String)
      (implicit tag: ClassTag[T]): (String, FunctionBuilder) = {

    // See if we can find a constructor that accepts Seq[Expression]
    val varargCtor = Try(tag.runtimeClass.getDeclaredConstructor(classOf[Seq[_]])).toOption
    val builder = (expressions: Seq[Expression]) => {
      if (varargCtor.isDefined) {
        // If there is an apply method that accepts Seq[Expression], use that one.
        varargCtor.get.newInstance(expressions).asInstanceOf[Expression]
      } else {
        // Otherwise, find an ctor method that matches the number of arguments, and use that.
        val params = Seq.fill(expressions.size)(classOf[Expression])
        val f = Try(tag.runtimeClass.getDeclaredConstructor(params : _*)) match {
          case Success(e) =>
            e
          case Failure(e) =>
            throw new AnalysisException(s"Invalid number of arguments for function $name")
        }
        f.newInstance(expressions : _*).asInstanceOf[Expression]
      }
    }
    (name, builder)
  }
}
