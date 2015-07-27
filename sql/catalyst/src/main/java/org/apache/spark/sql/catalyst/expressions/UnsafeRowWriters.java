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

import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A set of helper methods to write data into {@link UnsafeRow}s,
 * used by {@link org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection}.
 */
public class UnsafeRowWriters {

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

      // Write the string to the variable length portion.
      input.writeToMemory(target.getBaseObject(), offset);

      // Set the fixed length portion.
      target.setLong(ordinal, (((long) cursor) << 32) | ((long) numBytes));
      return ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes);
    }
  }

  /** Writer for bianry (byte array) type. */
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

      // Write the string to the variable length portion.
      ByteArray.writeToMemory(input, target.getBaseObject(), offset);

      // Set the fixed length portion.
      target.setLong(ordinal, (((long) cursor) << 32) | ((long) numBytes));
      return ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes);
    }
  }

}
