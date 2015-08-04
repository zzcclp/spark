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

package org.apache.spark.sql.catalyst.expressions;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.MapData;
import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A set of helper methods to write data into {@link UnsafeRow}s,
 * used by {@link org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection}.
 */
public class UnsafeRowWriters {

  /** Writer for Decimal with precision under 18. */
  public static class CompactDecimalWriter {

    public static int getSize(Decimal input) {
      return 0;
    }

    public static int write(UnsafeRow target, int ordinal, int cursor, Decimal input) {
      target.setLong(ordinal, input.toUnscaledLong());
      return 0;
    }
  }

  /** Writer for Decimal with precision larger than 18. */
  public static class DecimalWriter {

    public static int getSize(Decimal input) {
      // bounded size
      return 16;
    }

    public static int write(UnsafeRow target, int ordinal, int cursor, Decimal input) {
      final long offset = target.getBaseOffset() + cursor;
      final byte[] bytes = input.toJavaBigDecimal().unscaledValue().toByteArray();
      final int numBytes = bytes.length;
      assert(numBytes <= 16);

      // zero-out the bytes
      PlatformDependent.UNSAFE.putLong(target.getBaseObject(), offset, 0L);
      PlatformDependent.UNSAFE.putLong(target.getBaseObject(), offset + 8, 0L);

      // Write the bytes to the variable length portion.
      PlatformDependent.copyMemory(bytes, PlatformDependent.BYTE_ARRAY_OFFSET,
        target.getBaseObject(), offset, numBytes);

      // Set the fixed length portion.
      target.setLong(ordinal, (((long) cursor) << 32) | ((long) numBytes));
      return 16;
    }
  }

  /** Writer for UTF8String. */
  public static class UTF8StringWriter {

    public static int getSize(UTF8String input) {
      return ByteArrayMethods.roundNumberOfBytesToNearestWord(input.numBytes());
    }

    public static int write(UnsafeRow target, int ordinal, int cursor, UTF8String input) {
      final long offset = target.getBaseOffset() + cursor;
      final int numBytes = input.numBytes();

      // zero-out the padding bytes
      if ((numBytes & 0x07) > 0) {
        PlatformDependent.UNSAFE.putLong(
          target.getBaseObject(), offset + ((numBytes >> 3) << 3), 0L);
      }

      // Write the bytes to the variable length portion.
      input.writeToMemory(target.getBaseObject(), offset);

      // Set the fixed length portion.
      target.setLong(ordinal, (((long) cursor) << 32) | ((long) numBytes));
      return ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes);
    }
  }

  /** Writer for binary (byte array) type. */
  public static class BinaryWriter {

    public static int getSize(byte[] input) {
      return ByteArrayMethods.roundNumberOfBytesToNearestWord(input.length);
    }

    public static int write(UnsafeRow target, int ordinal, int cursor, byte[] input) {
      final long offset = target.getBaseOffset() + cursor;
      final int numBytes = input.length;

      // zero-out the padding bytes
      if ((numBytes & 0x07) > 0) {
        PlatformDependent.UNSAFE.putLong(
          target.getBaseObject(), offset + ((numBytes >> 3) << 3), 0L);
      }

      // Write the bytes to the variable length portion.
      ByteArray.writeToMemory(input, target.getBaseObject(), offset);

      // Set the fixed length portion.
      target.setLong(ordinal, (((long) cursor) << 32) | ((long) numBytes));
      return ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes);
    }
  }

  /**
   * Writer for struct type where the struct field is backed by an {@link UnsafeRow}.
   *
   * We throw UnsupportedOperationException for inputs that are not backed by {@link UnsafeRow}.
   * Non-UnsafeRow struct fields are handled directly in
   * {@link org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection}
   * by generating the Java code needed to convert them into UnsafeRow.
   */
  public static class StructWriter {
    public static int getSize(InternalRow input) {
      int numBytes = 0;
      if (input instanceof UnsafeRow) {
        numBytes = ((UnsafeRow) input).getSizeInBytes();
      } else {
        // This is handled directly in GenerateUnsafeProjection.
        throw new UnsupportedOperationException();
      }
      return ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes);
    }

    public static int write(UnsafeRow target, int ordinal, int cursor, InternalRow input) {
      int numBytes = 0;
      final long offset = target.getBaseOffset() + cursor;
      if (input instanceof UnsafeRow) {
        final UnsafeRow row = (UnsafeRow) input;
        numBytes = row.getSizeInBytes();

        // zero-out the padding bytes
        if ((numBytes & 0x07) > 0) {
          PlatformDependent.UNSAFE.putLong(
            target.getBaseObject(), offset + ((numBytes >> 3) << 3), 0L);
        }

        // Write the bytes to the variable length portion.
        row.writeToMemory(target.getBaseObject(), offset);

        // Set the fixed length portion.
        target.setLong(ordinal, (((long) cursor) << 32) | ((long) numBytes));
      } else {
        // This is handled directly in GenerateUnsafeProjection.
        throw new UnsupportedOperationException();
      }
      return ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes);
    }
  }

  /** Writer for interval type. */
  public static class IntervalWriter {

    public static int write(UnsafeRow target, int ordinal, int cursor, CalendarInterval input) {
      final long offset = target.getBaseOffset() + cursor;

      // Write the months and microseconds fields of Interval to the variable length portion.
      PlatformDependent.UNSAFE.putLong(target.getBaseObject(), offset, input.months);
      PlatformDependent.UNSAFE.putLong(target.getBaseObject(), offset + 8, input.microseconds);

      // Set the fixed length portion.
      target.setLong(ordinal, ((long) cursor) << 32);
      return 16;
    }
  }

  public static class ArrayWriter {

    public static int getSize(UnsafeArrayData input) {
      // we need extra 4 bytes the store the number of elements in this array.
      return ByteArrayMethods.roundNumberOfBytesToNearestWord(input.getSizeInBytes() + 4);
    }

    public static int write(UnsafeRow target, int ordinal, int cursor, UnsafeArrayData input) {
      final int numBytes = input.getSizeInBytes() + 4;
      final long offset = target.getBaseOffset() + cursor;

      // write the number of elements into first 4 bytes.
      PlatformDependent.UNSAFE.putInt(target.getBaseObject(), offset, input.numElements());

      // zero-out the padding bytes
      if ((numBytes & 0x07) > 0) {
        PlatformDependent.UNSAFE.putLong(
          target.getBaseObject(), offset + ((numBytes >> 3) << 3), 0L);
      }

      // Write the bytes to the variable length portion.
      input.writeToMemory(target.getBaseObject(), offset + 4);

      // Set the fixed length portion.
      target.setLong(ordinal, (((long) cursor) << 32) | ((long) numBytes));

      return ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes);
    }
  }

  public static class MapWriter {

    public static int getSize(UnsafeMapData input) {
      // we need extra 8 bytes to store number of elements and numBytes of key array.
      final int sizeInBytes = 4 + 4 + input.getSizeInBytes();
      return ByteArrayMethods.roundNumberOfBytesToNearestWord(sizeInBytes);
    }

    public static int write(UnsafeRow target, int ordinal, int cursor, UnsafeMapData input) {
      final long offset = target.getBaseOffset() + cursor;
      final UnsafeArrayData keyArray = input.keys;
      final UnsafeArrayData valueArray = input.values;
      final int keysNumBytes = keyArray.getSizeInBytes();
      final int valuesNumBytes = valueArray.getSizeInBytes();
      final int numBytes = 4 + 4 + keysNumBytes + valuesNumBytes;

      // write the number of elements into first 4 bytes.
      PlatformDependent.UNSAFE.putInt(target.getBaseObject(), offset, input.numElements());
      // write the numBytes of key array into second 4 bytes.
      PlatformDependent.UNSAFE.putInt(target.getBaseObject(), offset + 4, keysNumBytes);

      // zero-out the padding bytes
      if ((numBytes & 0x07) > 0) {
        PlatformDependent.UNSAFE.putLong(
          target.getBaseObject(), offset + ((numBytes >> 3) << 3), 0L);
      }

      // Write the bytes of key array to the variable length portion.
      keyArray.writeToMemory(target.getBaseObject(), offset + 8);

      // Write the bytes of value array to the variable length portion.
      valueArray.writeToMemory(target.getBaseObject(), offset + 8 + keysNumBytes);

      // Set the fixed length portion.
      target.setLong(ordinal, (((long) cursor) << 32) | ((long) numBytes));

      return ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes);
    }
  }
}
