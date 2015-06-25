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

package org.apache.spark.shuffle.hash

import java.io.InputStream

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.util.{Failure, Success}

import org.apache.spark._
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerId, ShuffleBlockFetcherIterator,
  ShuffleBlockId}

private[hash] object BlockStoreShuffleFetcher extends Logging {
  def fetchBlockStreams(
      shuffleId: Int,
      reduceId: Int,
      context: TaskContext,
      blockManager: BlockManager,
      mapOutputTracker: MapOutputTracker)
    : Iterator[(BlockId, InputStream)] =
  {
    logDebug("Fetching outputs for shuffle %d, reduce %d".format(shuffleId, reduceId))

    val startTime = System.currentTimeMillis
    val statuses = mapOutputTracker.getServerStatuses(shuffleId, reduceId)
    logDebug("Fetching map output location for shuffle %d, reduce %d took %d ms".format(
      shuffleId, reduceId, System.currentTimeMillis - startTime))

    val splitsByAddress = new HashMap[BlockManagerId, ArrayBuffer[(Int, Long)]]
    for (((address, size), index) <- statuses.zipWithIndex) {
      splitsByAddress.getOrElseUpdate(address, ArrayBuffer()) += ((index, size))
    }

    val blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])] = splitsByAddress.toSeq.map {
      case (address, splits) =>
        (address, splits.map(s => (ShuffleBlockId(shuffleId, s._1, reduceId), s._2)))
    }

    val blockFetcherItr = new ShuffleBlockFetcherIterator(
      context,
      blockManager.shuffleClient,
      blockManager,
      blocksByAddress,
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024)

    // Make sure that fetch failures are wrapped inside a FetchFailedException for the scheduler
    blockFetcherItr.map { blockPair =>
      val blockId = blockPair._1
      val blockOption = blockPair._2
      blockOption match {
        case Success(inputStream) => {
          (blockId, inputStream)
        }
        case Failure(e) => {
          blockId match {
            case ShuffleBlockId(shufId, mapId, _) =>
              val address = statuses(mapId.toInt)._1
              throw new FetchFailedException(address, shufId.toInt, mapId.toInt, reduceId, e)
            case _ =>
              throw new SparkException(
                "Failed to get block " + blockId + ", which is not a shuffle block", e)
          }
        }
      }
    }
  }
}
