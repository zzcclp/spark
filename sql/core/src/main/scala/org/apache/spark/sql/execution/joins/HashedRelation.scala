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

package org.apache.spark.sql.execution.joins

import java.io.{IOException, Externalizable, ObjectInput, ObjectOutput}
import java.nio.ByteOrder
import java.util.{HashMap => JavaHashMap}

import org.apache.spark.shuffle.ShuffleMemoryManager
import org.apache.spark.{SparkConf, SparkEnv, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.unsafe.PlatformDependent
import org.apache.spark.unsafe.map.BytesToBytesMap
import org.apache.spark.unsafe.memory.{ExecutorMemoryManager, MemoryAllocator, TaskMemoryManager}
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.CompactBuffer


/**
 * Interface for a hashed relation by some key. Use [[HashedRelation.apply]] to create a concrete
 * object.
 */
private[joins] sealed trait HashedRelation {
  def get(key: InternalRow): Seq[InternalRow]

  // This is a helper method to implement Externalizable, and is used by
  // GeneralHashedRelation and UniqueKeyHashedRelation
  protected def writeBytes(out: ObjectOutput, serialized: Array[Byte]): Unit = {
    out.writeInt(serialized.length) // Write the length of serialized bytes first
    out.write(serialized)
  }

  // This is a helper method to implement Externalizable, and is used by
  // GeneralHashedRelation and UniqueKeyHashedRelation
  protected def readBytes(in: ObjectInput): Array[Byte] = {
    val serializedSize = in.readInt() // Read the length of serialized bytes first
    val bytes = new Array[Byte](serializedSize)
    in.readFully(bytes)
    bytes
  }
}


/**
 * A general [[HashedRelation]] backed by a hash map that maps the key into a sequence of values.
 */
private[joins] final class GeneralHashedRelation(
    private var hashTable: JavaHashMap[InternalRow, CompactBuffer[InternalRow]])
  extends HashedRelation with Externalizable {

  private def this() = this(null) // Needed for serialization

  override def get(key: InternalRow): Seq[InternalRow] = hashTable.get(key)

  override def writeExternal(out: ObjectOutput): Unit = {
    writeBytes(out, SparkSqlSerializer.serialize(hashTable))
  }

  override def readExternal(in: ObjectInput): Unit = {
    hashTable = SparkSqlSerializer.deserialize(readBytes(in))
  }
}


/**
 * A specialized [[HashedRelation]] that maps key into a single value. This implementation
 * assumes the key is unique.
 */
private[joins]
final class UniqueKeyHashedRelation(private var hashTable: JavaHashMap[InternalRow, InternalRow])
  extends HashedRelation with Externalizable {

  private def this() = this(null) // Needed for serialization

  override def get(key: InternalRow): Seq[InternalRow] = {
    val v = hashTable.get(key)
    if (v eq null) null else CompactBuffer(v)
  }

  def getValue(key: InternalRow): InternalRow = hashTable.get(key)

  override def writeExternal(out: ObjectOutput): Unit = {
    writeBytes(out, SparkSqlSerializer.serialize(hashTable))
  }

  override def readExternal(in: ObjectInput): Unit = {
    hashTable = SparkSqlSerializer.deserialize(readBytes(in))
  }
}

// TODO(rxin): a version of [[HashedRelation]] backed by arrays for consecutive integer keys.


private[joins] object HashedRelation {

  def apply(
      input: Iterator[InternalRow],
      keyGenerator: Projection,
      sizeEstimate: Int = 64): HashedRelation = {

    if (keyGenerator.isInstanceOf[UnsafeProjection]) {
      return UnsafeHashedRelation(input, keyGenerator.asInstanceOf[UnsafeProjection], sizeEstimate)
    }

    // TODO: Use Spark's HashMap implementation.
    val hashTable = new JavaHashMap[InternalRow, CompactBuffer[InternalRow]](sizeEstimate)
    var currentRow: InternalRow = null

    // Whether the join key is unique. If the key is unique, we can convert the underlying
    // hash map into one specialized for this.
    var keyIsUnique = true

    // Create a mapping of buildKeys -> rows
    while (input.hasNext) {
      currentRow = input.next()
      val rowKey = keyGenerator(currentRow)
      if (!rowKey.anyNull) {
        val existingMatchList = hashTable.get(rowKey)
        val matchList = if (existingMatchList == null) {
          val newMatchList = new CompactBuffer[InternalRow]()
          hashTable.put(rowKey.copy(), newMatchList)
          newMatchList
        } else {
          keyIsUnique = false
          existingMatchList
        }
        matchList += currentRow.copy()
      }
    }

    if (keyIsUnique) {
      val uniqHashTable = new JavaHashMap[InternalRow, InternalRow](hashTable.size)
      val iter = hashTable.entrySet().iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        uniqHashTable.put(entry.getKey, entry.getValue()(0))
      }
      new UniqueKeyHashedRelation(uniqHashTable)
    } else {
      new GeneralHashedRelation(hashTable)
    }
  }
}

/**
 * A HashedRelation for UnsafeRow, which is backed by HashMap or BytesToBytesMap that maps the key
 * into a sequence of values.
 *
 * When it's created, it uses HashMap. After it's serialized and deserialized, it switch to use
 * BytesToBytesMap for better memory performance (multiple values for the same are stored as a
 * continuous byte array.
 *
 * It's serialized in the following format:
 *  [number of keys]
 *  [size of key] [size of all values in bytes] [key bytes] [bytes for all values]
 *  ...
 *
 * All the values are serialized as following:
 *   [number of fields] [number of bytes] [underlying bytes of UnsafeRow]
 *   ...
 */
private[joins] final class UnsafeHashedRelation(
    private var hashTable: JavaHashMap[UnsafeRow, CompactBuffer[UnsafeRow]])
  extends HashedRelation with Externalizable {

  private[joins] def this() = this(null)  // Needed for serialization

  // Use BytesToBytesMap in executor for better performance (it's created when deserialization)
  @transient private[this] var binaryMap: BytesToBytesMap = _

  override def get(key: InternalRow): Seq[InternalRow] = {
    val unsafeKey = key.asInstanceOf[UnsafeRow]

    if (binaryMap != null) {
      // Used in Broadcast join
      val loc = binaryMap.lookup(unsafeKey.getBaseObject, unsafeKey.getBaseOffset,
        unsafeKey.getSizeInBytes)
      if (loc.isDefined) {
        val buffer = CompactBuffer[UnsafeRow]()

        val base = loc.getValueAddress.getBaseObject
        var offset = loc.getValueAddress.getBaseOffset
        val last = loc.getValueAddress.getBaseOffset + loc.getValueLength
        while (offset < last) {
          val numFields = PlatformDependent.UNSAFE.getInt(base, offset)
          val sizeInBytes = PlatformDependent.UNSAFE.getInt(base, offset + 4)
          offset += 8

          val row = new UnsafeRow
          row.pointTo(base, offset, numFields, sizeInBytes)
          buffer += row
          offset += sizeInBytes
        }
        buffer
      } else {
        null
      }

    } else {
      // Use the JavaHashMap in Local mode or ShuffleHashJoin
      hashTable.get(unsafeKey)
    }
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    out.writeInt(hashTable.size())

    val iter = hashTable.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val key = entry.getKey
      val values = entry.getValue

      // write all the values as single byte array
      var totalSize = 0L
      var i = 0
      while (i < values.length) {
        totalSize += values(i).getSizeInBytes + 4 + 4
        i += 1
      }
      assert(totalSize < Integer.MAX_VALUE, "values are too big")

      // [key size] [values size] [key bytes] [values bytes]
      out.writeInt(key.getSizeInBytes)
      out.writeInt(totalSize.toInt)
      out.write(key.getBytes)
      i = 0
      while (i < values.length) {
        // [num of fields] [num of bytes] [row bytes]
        // write the integer in native order, so they can be read by UNSAFE.getInt()
        if (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) {
          out.writeInt(values(i).numFields())
          out.writeInt(values(i).getSizeInBytes)
        } else {
          out.writeInt(Integer.reverseBytes(values(i).numFields()))
          out.writeInt(Integer.reverseBytes(values(i).getSizeInBytes))
        }
        out.write(values(i).getBytes)
        i += 1
      }
    }
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    val nKeys = in.readInt()
    // This is used in Broadcast, shared by multiple tasks, so we use on-heap memory
    val taskMemoryManager = new TaskMemoryManager(new ExecutorMemoryManager(MemoryAllocator.HEAP))

    // Dummy shuffle memory manager which always grants all memory allocation requests.
    // We use this because it doesn't make sense count shared broadcast variables' memory usage
    // towards individual tasks' quotas. In the future, we should devise a better way of handling
    // this.
    val shuffleMemoryManager = new ShuffleMemoryManager(new SparkConf()) {
      override def tryToAcquire(numBytes: Long): Long = numBytes
      override def release(numBytes: Long): Unit = {}
    }

    val pageSizeBytes = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
      .getSizeAsBytes("spark.buffer.pageSize", "64m")

    binaryMap = new BytesToBytesMap(
      taskMemoryManager,
      shuffleMemoryManager,
      nKeys * 2, // reduce hash collision
      pageSizeBytes)

    var i = 0
    var keyBuffer = new Array[Byte](1024)
    var valuesBuffer = new Array[Byte](1024)
    while (i < nKeys) {
      val keySize = in.readInt()
      val valuesSize = in.readInt()
      if (keySize > keyBuffer.size) {
        keyBuffer = new Array[Byte](keySize)
      }
      in.readFully(keyBuffer, 0, keySize)
      if (valuesSize > valuesBuffer.size) {
        valuesBuffer = new Array[Byte](valuesSize)
      }
      in.readFully(valuesBuffer, 0, valuesSize)

      // put it into binary map
      val loc = binaryMap.lookup(keyBuffer, PlatformDependent.BYTE_ARRAY_OFFSET, keySize)
      assert(!loc.isDefined, "Duplicated key found!")
      val putSuceeded = loc.putNewKey(keyBuffer, PlatformDependent.BYTE_ARRAY_OFFSET, keySize,
        valuesBuffer, PlatformDependent.BYTE_ARRAY_OFFSET, valuesSize)
      if (!putSuceeded) {
        throw new IOException("Could not allocate memory to grow BytesToBytesMap")
      }
      i += 1
    }
  }
}

private[joins] object UnsafeHashedRelation {

  def apply(
      input: Iterator[InternalRow],
      keyGenerator: UnsafeProjection,
      sizeEstimate: Int): HashedRelation = {

    val hashTable = new JavaHashMap[UnsafeRow, CompactBuffer[UnsafeRow]](sizeEstimate)

    // Create a mapping of buildKeys -> rows
    while (input.hasNext) {
      val unsafeRow = input.next().asInstanceOf[UnsafeRow]
      val rowKey = keyGenerator(unsafeRow)
      if (!rowKey.anyNull) {
        val existingMatchList = hashTable.get(rowKey)
        val matchList = if (existingMatchList == null) {
          val newMatchList = new CompactBuffer[UnsafeRow]()
          hashTable.put(rowKey.copy(), newMatchList)
          newMatchList
        } else {
          existingMatchList
        }
        matchList += unsafeRow.copy()
      }
    }

    new UnsafeHashedRelation(hashTable)
  }
}
