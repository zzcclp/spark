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

package org.apache.spark.unsafe;

import org.apache.spark.unsafe.memory.HeapMemoryAllocator;
import org.apache.spark.unsafe.memory.MemoryAllocator;
import org.apache.spark.unsafe.memory.MemoryBlock;

import org.junit.Assert;
import org.junit.Test;

public class PlatformUtilSuite {

  @Test
  public void overlappingCopyMemory() {
    byte[] data = new byte[3 * 1024 * 1024];
    int size = 2 * 1024 * 1024;
    for (int i = 0; i < data.length; ++i) {
      data[i] = (byte)i;
    }

    Platform.copyMemory(data, Platform.BYTE_ARRAY_OFFSET, data, Platform.BYTE_ARRAY_OFFSET, size);
    for (int i = 0; i < data.length; ++i) {
      Assert.assertEquals((byte)i, data[i]);
    }

    Platform.copyMemory(
        data,
        Platform.BYTE_ARRAY_OFFSET + 1,
        data,
        Platform.BYTE_ARRAY_OFFSET,
        size);
    for (int i = 0; i < size; ++i) {
      Assert.assertEquals((byte)(i + 1), data[i]);
    }

    for (int i = 0; i < data.length; ++i) {
      data[i] = (byte)i;
    }
    Platform.copyMemory(
        data,
        Platform.BYTE_ARRAY_OFFSET,
        data,
        Platform.BYTE_ARRAY_OFFSET + 1,
        size);
    for (int i = 0; i < size; ++i) {
      Assert.assertEquals((byte)i, data[i + 1]);
    }
  }

  @Test
  public void memoryDebugFillEnabledInTest() {
    Assert.assertTrue(MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED);
    MemoryBlock onheap = MemoryAllocator.HEAP.allocate(1);
    MemoryBlock offheap = MemoryAllocator.UNSAFE.allocate(1);
    Assert.assertEquals(
      Platform.getByte(onheap.getBaseObject(), onheap.getBaseOffset()),
      MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
    Assert.assertEquals(
      Platform.getByte(offheap.getBaseObject(), offheap.getBaseOffset()),
      MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
  }

  @Test
  public void heapMemoryReuse() {
    MemoryAllocator heapMem = new HeapMemoryAllocator();
    // The size is less than `HeapMemoryAllocator.POOLING_THRESHOLD_BYTES`,
    // allocate new memory every time.
    MemoryBlock onheap1 = heapMem.allocate(513);
    Object obj1 = onheap1.getBaseObject();
    heapMem.free(onheap1);
    MemoryBlock onheap2 = heapMem.allocate(514);
    Assert.assertNotEquals(obj1, onheap2.getBaseObject());

    // The size is greater than `HeapMemoryAllocator.POOLING_THRESHOLD_BYTES`,
    // reuse the previous memory which has released.
    MemoryBlock onheap3 = heapMem.allocate(1024 * 1024 + 1);
    Assert.assertEquals(onheap3.size(), 1024 * 1024 + 1);
    Object obj3 = onheap3.getBaseObject();
    heapMem.free(onheap3);
    MemoryBlock onheap4 = heapMem.allocate(1024 * 1024 + 7);
    Assert.assertEquals(onheap4.size(), 1024 * 1024 + 7);
    Assert.assertEquals(obj3, onheap4.getBaseObject());
  }
}
