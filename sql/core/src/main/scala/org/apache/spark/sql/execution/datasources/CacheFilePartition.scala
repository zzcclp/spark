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

package org.apache.spark.sql.execution.datasources

import org.apache.kylin.cache.scheduler.LocalDataCacheManager
import org.apache.spark.Partition
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql.connector.read.InputPartition

/**
 * A collection of file blocks that should be read as a single task
 * (possibly from multiple partitioned directories).
 */
case class CacheFilePartition(index: Int, files: Array[CachePartitionedFile])
  extends Partition with InputPartition {
  override def preferredLocations(): Array[String] = {
    // Todo: Only get the first one
    files.head.locations.map { p =>
      ExecutorCacheTaskLocation(p._1, p._2).toString
    }.toArray
  }
}

object CacheFilePartition extends Logging {

  def convertFilePartitionToCache(filePartition: FilePartition): CacheFilePartition = {
    // Calculate the target executors
    val locations = LocalDataCacheManager.askExecutor(Array(filePartition.files.head.filePath))
    logInfo(s"========= Calculate ${filePartition.files.head.filePath} " +
      s"on locations ${locations.mkString}")
    CacheFilePartition(filePartition.index, filePartition.files.map(p => {
      CachePartitionedFile(p.partitionValues, p.filePath, p.start, p.length, locations)
    }))
  }
}


