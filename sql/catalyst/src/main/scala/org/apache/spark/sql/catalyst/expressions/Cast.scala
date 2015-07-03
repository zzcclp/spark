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

import java.math.{BigDecimal => JavaBigDecimal}
import java.sql.{Date, Timestamp}

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenContext, GeneratedExpressionCode}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


object Cast {

  /**
   * Returns true iff we can cast `from` type to `to` type.
   */
  def canCast(from: DataType, to: DataType): Boolean = (from, to) match {
    case (fromType, toType) if fromType == toType => true

    case (NullType, _) => true

    case (_, StringType) => true

    case (StringType, BinaryType) => true

    case (StringType, BooleanType) => true
    case (DateType, BooleanType) => true
    case (TimestampType, BooleanType) => true
    case (_: NumericType, BooleanType) => true

    case (StringType, TimestampType) => true
    case (BooleanType, TimestampType) => true
    case (DateType, TimestampType) => true
    case (_: NumericType, TimestampType) => true

    case (_, DateType) => true

    case (StringType, _: NumericType) => true
    case (BooleanType, _: NumericType) => true
    case (DateType, _: NumericType) => true
    case (TimestampType, _: NumericType) => true
    case (_: NumericType, _: NumericType) => true

    case (ArrayType(fromType, fn), ArrayType(toType, tn)) =>
      canCast(fromType, toType) &&
        resolvableNullability(fn || forceNullable(fromType, toType), tn)

    case (MapType(fromKey, fromValue, fn), MapType(toKey, toValue, tn)) =>
      canCast(fromKey, toKey) &&
        (!forceNullable(fromKey, toKey)) &&
        canCast(fromValue, toValue) &&
        resolvableNullability(fn || forceNullable(fromValue, toValue), tn)

    case (StructType(fromFields), StructType(toFields)) =>
      fromFields.length == toFields.length &&
        fromFields.zip(toFields).forall {
          case (fromField, toField) =>
            canCast(fromField.dataType, toField.dataType) &&
              resolvableNullability(
                fromField.nullable || forceNullable(fromField.dataType, toField.dataType),
                toField.nullable)
        }

    case _ => false
  }

  private def resolvableNullability(from: Boolean, to: Boolean) = !from || to

  private def forceNullable(from: DataType, to: DataType) = (from, to) match {
    case (StringType, _: NumericType) => true
    case (StringType, TimestampType) => true
    case (DoubleType, TimestampType) => true
    case (FloatType, TimestampType) => true
    case (StringType, DateType) => true
    case (_: NumericType, DateType) => true
    case (BooleanType, DateType) => true
    case (DateType, _: NumericType) => true
    case (DateType, BooleanType) => true
    case (DoubleType, _: DecimalType) => true
    case (FloatType, _: DecimalType) => true
    case (_, DecimalType.Fixed(_, _)) => true // TODO: not all upcasts here can really give null
    case _ => false
  }
}

/** Cast the child expression to the target data type. */
case class Cast(child: Expression, dataType: DataType) extends UnaryExpression with Logging {

  override def checkInputDataTypes(): TypeCheckResult = {
    if (Cast.canCast(child.dataType, dataType)) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(
        s"cannot cast ${child.dataType} to $dataType")
    }
  }

  override def foldable: Boolean = child.foldable

  override def nullable: Boolean = Cast.forceNullable(child.dataType, dataType) || child.nullable

  override def toString: String = s"CAST($child, $dataType)"

  // [[func]] assumes the input is no longer null because eval already does the null check.
  @inline private[this] def buildCast[T](a: Any, func: T => Any): Any = func(a.asInstanceOf[T])

  // UDFToString
  private[this] def castToString(from: DataType): Any => Any = from match {
    case BinaryType => buildCast[Array[Byte]](_, UTF8String.fromBytes)
    case DateType => buildCast[Int](_, d => UTF8String.fromString(DateTimeUtils.dateToString(d)))
    case TimestampType => buildCast[Long](_,
      t => UTF8String.fromString(DateTimeUtils.timestampToString(t)))
    case _ => buildCast[Any](_, o => UTF8String.fromString(o.toString))
  }

  // BinaryConverter
  private[this] def castToBinary(from: DataType): Any => Any = from match {
    case StringType => buildCast[UTF8String](_, _.getBytes)
  }

  // UDFToBoolean
  private[this] def castToBoolean(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, _.length() != 0)
    case TimestampType =>
      buildCast[Long](_, t => t != 0)
    case DateType =>
      // Hive would return null when cast from date to boolean
      buildCast[Int](_, d => null)
    case LongType =>
      buildCast[Long](_, _ != 0)
    case IntegerType =>
      buildCast[Int](_, _ != 0)
    case ShortType =>
      buildCast[Short](_, _ != 0)
    case ByteType =>
      buildCast[Byte](_, _ != 0)
    case DecimalType() =>
      buildCast[Decimal](_, _ != Decimal(0))
    case DoubleType =>
      buildCast[Double](_, _ != 0)
    case FloatType =>
      buildCast[Float](_, _ != 0)
  }

  // TimestampConverter
  private[this] def castToTimestamp(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, utfs => {
        // Throw away extra if more than 9 decimal places
        val s = utfs.toString
        val periodIdx = s.indexOf(".")
        var n = s
        if (periodIdx != -1 && n.length() - periodIdx > 9) {
          n = n.substring(0, periodIdx + 10)
        }
        try DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(n))
        catch { case _: java.lang.IllegalArgumentException => null }
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1L else 0)
    case LongType =>
      buildCast[Long](_, l => longToTimestamp(l))
    case IntegerType =>
      buildCast[Int](_, i => longToTimestamp(i.toLong))
    case ShortType =>
      buildCast[Short](_, s => longToTimestamp(s.toLong))
    case ByteType =>
      buildCast[Byte](_, b => longToTimestamp(b.toLong))
    case DateType =>
      buildCast[Int](_, d => DateTimeUtils.daysToMillis(d) * 10000)
    // TimestampWritable.decimalToTimestamp
    case DecimalType() =>
      buildCast[Decimal](_, d => decimalToTimestamp(d))
    // TimestampWritable.doubleToTimestamp
    case DoubleType =>
      buildCast[Double](_, d => try {
        decimalToTimestamp(Decimal(d))
      } catch {
        case _: NumberFormatException => null
      })
    // TimestampWritable.floatToTimestamp
    case FloatType =>
      buildCast[Float](_, f => try {
        decimalToTimestamp(Decimal(f))
      } catch {
        case _: NumberFormatException => null
      })
  }

  private[this] def decimalToTimestamp(d: Decimal): Long = {
    (d.toBigDecimal * 10000000L).longValue()
  }

  // converting milliseconds to 100ns
  private[this] def longToTimestamp(t: Long): Long = t * 10000L
  // converting 100ns to seconds
  private[this] def timestampToLong(ts: Long): Long = math.floor(ts.toDouble / 10000000L).toLong
  // converting 100ns to seconds in double
  private[this] def timestampToDouble(ts: Long): Double = {
    ts / 10000000.0
  }

  // DateConverter
  private[this] def castToDate(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, s =>
        try DateTimeUtils.fromJavaDate(Date.valueOf(s.toString))
        catch { case _: java.lang.IllegalArgumentException => null }
      )
    case TimestampType =>
      // throw valid precision more than seconds, according to Hive.
      // Timestamp.nanos is in 0 to 999,999,999, no more than a second.
      buildCast[Long](_, t => DateTimeUtils.millisToDays(t / 10000L))
    // Hive throws this exception as a Semantic Exception
    // It is never possible to compare result when hive return with exception,
    // so we can return null
    // NULL is more reasonable here, since the query itself obeys the grammar.
    case _ => _ => null
  }

  // LongConverter
  private[this] def castToLong(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, s => try s.toString.toLong catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1L else 0L)
    case DateType =>
      buildCast[Int](_, d => null)
    case TimestampType =>
      buildCast[Long](_, t => timestampToLong(t))
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toLong(b)
  }

  // IntConverter
  private[this] def castToInt(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, s => try s.toString.toInt catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1 else 0)
    case DateType =>
      buildCast[Int](_, d => null)
    case TimestampType =>
      buildCast[Long](_, t => timestampToLong(t).toInt)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toInt(b)
  }

  // ShortConverter
  private[this] def castToShort(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, s => try s.toString.toShort catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1.toShort else 0.toShort)
    case DateType =>
      buildCast[Int](_, d => null)
    case TimestampType =>
      buildCast[Long](_, t => timestampToLong(t).toShort)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toInt(b).toShort
  }

  // ByteConverter
  private[this] def castToByte(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, s => try s.toString.toByte catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1.toByte else 0.toByte)
    case DateType =>
      buildCast[Int](_, d => null)
    case TimestampType =>
      buildCast[Long](_, t => timestampToLong(t).toByte)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toInt(b).toByte
  }

  /**
   * Change the precision / scale in a given decimal to those set in `decimalType` (if any),
   * returning null if it overflows or modifying `value` in-place and returning it if successful.
   *
   * NOTE: this modifies `value` in-place, so don't call it on external data.
   */
  private[this] def changePrecision(value: Decimal, decimalType: DecimalType): Decimal = {
    decimalType match {
      case DecimalType.Unlimited =>
        value
      case DecimalType.Fixed(precision, scale) =>
        if (value.changePrecision(precision, scale)) value else null
    }
  }

  private[this] def castToDecimal(from: DataType, target: DecimalType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, s => try {
        changePrecision(Decimal(new JavaBigDecimal(s.toString)), target)
      } catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => changePrecision(if (b) Decimal(1) else Decimal(0), target))
    case DateType =>
      buildCast[Int](_, d => null) // date can't cast to decimal in Hive
    case TimestampType =>
      // Note that we lose precision here.
      buildCast[Long](_, t => changePrecision(Decimal(timestampToDouble(t)), target))
    case DecimalType() =>
      b => changePrecision(b.asInstanceOf[Decimal].clone(), target)
    case LongType =>
      b => changePrecision(Decimal(b.asInstanceOf[Long]), target)
    case x: NumericType => // All other numeric types can be represented precisely as Doubles
      b => try {
        changePrecision(Decimal(x.numeric.asInstanceOf[Numeric[Any]].toDouble(b)), target)
      } catch {
        case _: NumberFormatException => null
      }
  }

  // DoubleConverter
  private[this] def castToDouble(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, s => try s.toString.toDouble catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1d else 0d)
    case DateType =>
      buildCast[Int](_, d => null)
    case TimestampType =>
      buildCast[Long](_, t => timestampToDouble(t))
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toDouble(b)
  }

  // FloatConverter
  private[this] def castToFloat(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, s => try s.toString.toFloat catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1f else 0f)
    case DateType =>
      buildCast[Int](_, d => null)
    case TimestampType =>
      buildCast[Long](_, t => timestampToDouble(t).toFloat)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toFloat(b)
  }

  private[this] def castArray(from: ArrayType, to: ArrayType): Any => Any = {
    val elementCast = cast(from.elementType, to.elementType)
    buildCast[Seq[Any]](_, _.map(v => if (v == null) null else elementCast(v)))
  }

  private[this] def castMap(from: MapType, to: MapType): Any => Any = {
    val keyCast = cast(from.keyType, to.keyType)
    val valueCast = cast(from.valueType, to.valueType)
    buildCast[Map[Any, Any]](_, _.map {
      case (key, value) => (keyCast(key), if (value == null) null else valueCast(value))
    })
  }

  private[this] def castStruct(from: StructType, to: StructType): Any => Any = {
    val casts = from.fields.zip(to.fields).map {
      case (fromField, toField) => cast(fromField.dataType, toField.dataType)
    }
    // TODO: Could be faster?
    val newRow = new GenericMutableRow(from.fields.length)
    buildCast[InternalRow](_, row => {
      var i = 0
      while (i < row.length) {
        val v = row(i)
        newRow.update(i, if (v == null) null else casts(i)(v))
        i += 1
      }
      newRow.copy()
    })
  }

  private[this] def cast(from: DataType, to: DataType): Any => Any = to match {
    case dt if dt == child.dataType => identity[Any]
    case StringType => castToString(from)
    case BinaryType => castToBinary(from)
    case DateType => castToDate(from)
    case decimal: DecimalType => castToDecimal(from, decimal)
    case TimestampType => castToTimestamp(from)
    case BooleanType => castToBoolean(from)
    case ByteType => castToByte(from)
    case ShortType => castToShort(from)
    case IntegerType => castToInt(from)
    case FloatType => castToFloat(from)
    case LongType => castToLong(from)
    case DoubleType => castToDouble(from)
    case array: ArrayType => castArray(from.asInstanceOf[ArrayType], array)
    case map: MapType => castMap(from.asInstanceOf[MapType], map)
    case struct: StructType => castStruct(from.asInstanceOf[StructType], struct)
  }

  private[this] lazy val cast: Any => Any = cast(child.dataType, dataType)

  override def eval(input: InternalRow): Any = {
    val evaluated = child.eval(input)
    if (evaluated == null) null else cast(evaluated)
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    // TODO: Add support for more data types.
    (child.dataType, dataType) match {

      case (BinaryType, StringType) =>
        defineCodeGen (ctx, ev, c =>
          s"${ctx.stringType}.fromBytes($c)")

      case (DateType, StringType) =>
        defineCodeGen(ctx, ev, c =>
          s"""${ctx.stringType}.fromString(
                org.apache.spark.sql.catalyst.util.DateTimeUtils.dateToString($c))""")

      case (TimestampType, StringType) =>
        defineCodeGen(ctx, ev, c =>
          s"""${ctx.stringType}.fromString(
                org.apache.spark.sql.catalyst.util.DateTimeUtils.timestampToString($c))""")

      case (_, StringType) =>
        defineCodeGen(ctx, ev, c => s"${ctx.stringType}.fromString(String.valueOf($c))")

      // fallback for DecimalType, this must be before other numeric types
      case (_, dt: DecimalType) =>
        super.genCode(ctx, ev)

      case (BooleanType, dt: NumericType) =>
        defineCodeGen(ctx, ev, c => s"(${ctx.javaType(dt)})($c ? 1 : 0)")

      case (dt: DecimalType, BooleanType) =>
        defineCodeGen(ctx, ev, c => s"!$c.isZero()")

      case (dt: NumericType, BooleanType) =>
        defineCodeGen(ctx, ev, c => s"$c != 0")

      case (_: DecimalType, dt: NumericType) =>
        defineCodeGen(ctx, ev, c => s"($c).to${ctx.primitiveTypeName(dt)}()")

      case (_: NumericType, dt: NumericType) =>
        defineCodeGen(ctx, ev, c => s"(${ctx.javaType(dt)})($c)")

      case other =>
        super.genCode(ctx, ev)
    }
  }
}
