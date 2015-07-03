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

import java.util.regex.Pattern

import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

trait StringRegexExpression extends ExpectsInputTypes {
  self: BinaryExpression =>

  def escape(v: String): String
  def matches(regex: Pattern, str: String): Boolean

  override def nullable: Boolean = left.nullable || right.nullable
  override def dataType: DataType = BooleanType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)

  // try cache the pattern for Literal
  private lazy val cache: Pattern = right match {
    case x @ Literal(value: String, StringType) => compile(value)
    case _ => null
  }

  protected def compile(str: String): Pattern = if (str == null) {
    null
  } else {
    // Let it raise exception if couldn't compile the regex string
    Pattern.compile(escape(str))
  }

  protected def pattern(str: String) = if (cache == null) compile(str) else cache

  override def eval(input: InternalRow): Any = {
    val l = left.eval(input)
    if (l == null) {
      null
    } else {
      val r = right.eval(input)
      if(r == null) {
        null
      } else {
        val regex = pattern(r.asInstanceOf[UTF8String].toString())
        if(regex == null) {
          null
        } else {
          matches(regex, l.asInstanceOf[UTF8String].toString())
        }
      }
    }
  }
}

/**
 * Simple RegEx pattern matching function
 */
case class Like(left: Expression, right: Expression)
  extends BinaryExpression with StringRegexExpression {

  // replace the _ with .{1} exactly match 1 time of any character
  // replace the % with .*, match 0 or more times with any character
  override def escape(v: String): String =
    if (!v.isEmpty) {
      "(?s)" + (' ' +: v.init).zip(v).flatMap {
        case (prev, '\\') => ""
        case ('\\', c) =>
          c match {
            case '_' => "_"
            case '%' => "%"
            case _ => Pattern.quote("\\" + c)
          }
        case (prev, c) =>
          c match {
            case '_' => "."
            case '%' => ".*"
            case _ => Pattern.quote(Character.toString(c))
          }
      }.mkString
    } else {
      v
    }

  override def matches(regex: Pattern, str: String): Boolean = regex.matcher(str).matches()

  override def toString: String = s"$left LIKE $right"
}

case class RLike(left: Expression, right: Expression)
  extends BinaryExpression with StringRegexExpression {

  override def escape(v: String): String = v
  override def matches(regex: Pattern, str: String): Boolean = regex.matcher(str).find(0)
  override def toString: String = s"$left RLIKE $right"
}

trait CaseConversionExpression extends ExpectsInputTypes {
  self: UnaryExpression =>

  def convert(v: UTF8String): UTF8String

  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType)

  override def eval(input: InternalRow): Any = {
    val evaluated = child.eval(input)
    if (evaluated == null) {
      null
    } else {
      convert(evaluated.asInstanceOf[UTF8String])
    }
  }
}

/**
 * A function that converts the characters of a string to uppercase.
 */
case class Upper(child: Expression) extends UnaryExpression with CaseConversionExpression {

  override def convert(v: UTF8String): UTF8String = v.toUpperCase

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"($c).toUpperCase()")
  }
}

/**
 * A function that converts the characters of a string to lowercase.
 */
case class Lower(child: Expression) extends UnaryExpression with CaseConversionExpression {

  override def convert(v: UTF8String): UTF8String = v.toLowerCase

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"($c).toLowerCase()")
  }
}

/** A base trait for functions that compare two strings, returning a boolean. */
trait StringComparison extends ExpectsInputTypes {
  self: BinaryExpression =>

  def compare(l: UTF8String, r: UTF8String): Boolean

  override def nullable: Boolean = left.nullable || right.nullable

  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)

  override def eval(input: InternalRow): Any = {
    val leftEval = left.eval(input)
    if(leftEval == null) {
      null
    } else {
      val rightEval = right.eval(input)
      if (rightEval == null) null
      else compare(leftEval.asInstanceOf[UTF8String], rightEval.asInstanceOf[UTF8String])
    }
  }

  override def toString: String = s"$nodeName($left, $right)"
}

/**
 * A function that returns true if the string `left` contains the string `right`.
 */
case class Contains(left: Expression, right: Expression)
    extends BinaryExpression with Predicate with StringComparison {
  override def compare(l: UTF8String, r: UTF8String): Boolean = l.contains(r)
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (c1, c2) => s"($c1).contains($c2)")
  }
}

/**
 * A function that returns true if the string `left` starts with the string `right`.
 */
case class StartsWith(left: Expression, right: Expression)
    extends BinaryExpression with Predicate with StringComparison {
  override def compare(l: UTF8String, r: UTF8String): Boolean = l.startsWith(r)
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (c1, c2) => s"($c1).startsWith($c2)")
  }
}

/**
 * A function that returns true if the string `left` ends with the string `right`.
 */
case class EndsWith(left: Expression, right: Expression)
    extends BinaryExpression with Predicate with StringComparison {
  override def compare(l: UTF8String, r: UTF8String): Boolean = l.endsWith(r)
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (c1, c2) => s"($c1).endsWith($c2)")
  }
}

/**
 * A function that takes a substring of its first argument starting at a given position.
 * Defined for String and Binary types.
 */
case class Substring(str: Expression, pos: Expression, len: Expression)
  extends Expression with ExpectsInputTypes {

  def this(str: Expression, pos: Expression) = {
    this(str, pos, Literal(Integer.MAX_VALUE))
  }

  override def foldable: Boolean = str.foldable && pos.foldable && len.foldable

  override  def nullable: Boolean = str.nullable || pos.nullable || len.nullable

  override def dataType: DataType = {
    if (!resolved) {
      throw new UnresolvedException(this, s"Cannot resolve since $children are not resolved")
    }
    if (str.dataType == BinaryType) str.dataType else StringType
  }

  override def inputTypes: Seq[DataType] = Seq(StringType, IntegerType, IntegerType)

  override def children: Seq[Expression] = str :: pos :: len :: Nil

  @inline
  def slicePos(startPos: Int, sliceLen: Int, length: () => Int): (Int, Int) = {
    // Hive and SQL use one-based indexing for SUBSTR arguments but also accept zero and
    // negative indices for start positions. If a start index i is greater than 0, it
    // refers to element i-1 in the sequence. If a start index i is less than 0, it refers
    // to the -ith element before the end of the sequence. If a start index i is 0, it
    // refers to the first element.

    val start = startPos match {
      case pos if pos > 0 => pos - 1
      case neg if neg < 0 => length() + neg
      case _ => 0
    }

    val end = sliceLen match {
      case max if max == Integer.MAX_VALUE => max
      case x => start + x
    }

    (start, end)
  }

  override def eval(input: InternalRow): Any = {
    val string = str.eval(input)
    val po = pos.eval(input)
    val ln = len.eval(input)

    if ((string == null) || (po == null) || (ln == null)) {
      null
    } else {
      val start = po.asInstanceOf[Int]
      val length = ln.asInstanceOf[Int]
      string match {
        case ba: Array[Byte] =>
          val (st, end) = slicePos(start, length, () => ba.length)
          ba.slice(st, end)
        case s: UTF8String =>
          val (st, end) = slicePos(start, length, () => s.length())
          s.substring(st, end)
      }
    }
  }
}

/**
 * A function that return the length of the given string expression.
 */
case class StringLength(child: Expression) extends UnaryExpression with ExpectsInputTypes {
  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[DataType] = Seq(StringType)

  override def eval(input: InternalRow): Any = {
    val string = child.eval(input)
    if (string == null) null else string.asInstanceOf[UTF8String].length
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"($c).length()")
  }

  override def prettyName: String = "length"
}
