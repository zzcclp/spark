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

package org.apache.spark.sql.catalyst

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.unsafe.types.UTF8String

/**
 * An abstract class for row used internal in Spark SQL, which only contain the columns as
 * internal types.
 */
abstract class InternalRow extends Row {

  // This is only use for test
  override def getString(i: Int): String = getAs[UTF8String](i).toString

  // These expensive API should not be used internally.
  final override def getDecimal(i: Int): java.math.BigDecimal =
    throw new UnsupportedOperationException
  final override def getDate(i: Int): java.sql.Date =
    throw new UnsupportedOperationException
  final override def getTimestamp(i: Int): java.sql.Timestamp =
    throw new UnsupportedOperationException
  final override def getSeq[T](i: Int): Seq[T] = throw new UnsupportedOperationException
  final override def getList[T](i: Int): java.util.List[T] = throw new UnsupportedOperationException
  final override def getMap[K, V](i: Int): scala.collection.Map[K, V] =
    throw new UnsupportedOperationException
  final override def getJavaMap[K, V](i: Int): java.util.Map[K, V] =
    throw new UnsupportedOperationException
  final override def getStruct(i: Int): Row = throw new UnsupportedOperationException
  final override def getAs[T](fieldName: String): T = throw new UnsupportedOperationException
  final override def getValuesMap[T](fieldNames: Seq[String]): Map[String, T] =
    throw new UnsupportedOperationException

  // A default implementation to change the return type
  override def copy(): InternalRow = this
  override def apply(i: Int): Any = get(i)

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[Row]) {
      return false
    }

    val other = o.asInstanceOf[Row]
    if (length != other.length) {
      return false
    }

    var i = 0
    while (i < length) {
      if (isNullAt(i) != other.isNullAt(i)) {
        return false
      }
      if (!isNullAt(i)) {
        val o1 = apply(i)
        val o2 = other.apply(i)
        if (o1.isInstanceOf[Array[Byte]]) {
          // handle equality of Array[Byte]
          val b1 = o1.asInstanceOf[Array[Byte]]
          if (!o2.isInstanceOf[Array[Byte]] ||
            !java.util.Arrays.equals(b1, o2.asInstanceOf[Array[Byte]])) {
            return false
          }
        } else if (o1 != o2) {
          return false
        }
      }
      i += 1
    }
    true
  }

  // Custom hashCode function that matches the efficient code generated version.
  override def hashCode: Int = {
    var result: Int = 37
    var i = 0
    while (i < length) {
      val update: Int =
        if (isNullAt(i)) {
          0
        } else {
          apply(i) match {
            case b: Boolean => if (b) 0 else 1
            case b: Byte => b.toInt
            case s: Short => s.toInt
            case i: Int => i
            case l: Long => (l ^ (l >>> 32)).toInt
            case f: Float => java.lang.Float.floatToIntBits(f)
            case d: Double =>
              val b = java.lang.Double.doubleToLongBits(d)
              (b ^ (b >>> 32)).toInt
            case a: Array[Byte] => java.util.Arrays.hashCode(a)
            case other => other.hashCode()
          }
        }
      result = 37 * result + update
      i += 1
    }
    result
  }
}

object InternalRow {
  /**
   * This method can be used to construct a [[Row]] with the given values.
   */
  def apply(values: Any*): InternalRow = new GenericInternalRow(values.toArray)

  /**
   * This method can be used to construct a [[Row]] from a [[Seq]] of values.
   */
  def fromSeq(values: Seq[Any]): InternalRow = new GenericInternalRow(values.toArray)

  /** Returns an empty row. */
  val empty = apply()
}
