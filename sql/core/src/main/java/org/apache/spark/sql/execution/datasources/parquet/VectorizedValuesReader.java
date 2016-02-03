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

package org.apache.spark.sql.execution.datasources.parquet;

import org.apache.spark.sql.execution.vectorized.ColumnVector;

/**
 * Interface for value decoding that supports vectorized (aka batched) decoding.
 * TODO: merge this into parquet-mr.
 */
public interface VectorizedValuesReader {
  byte readByte();
  int readInteger();
  long readLong();

  /*
   * Reads `total` values into `c` start at `c[rowId]`
   */
  void readBytes(int total, ColumnVector c, int rowId);
  void readIntegers(int total, ColumnVector c, int rowId);
  void readLongs(int total, ColumnVector c, int rowId);
  void readBinary(int total, ColumnVector c, int rowId);

  // TODO: add all the other parquet types.

  void skip(int n);
}
