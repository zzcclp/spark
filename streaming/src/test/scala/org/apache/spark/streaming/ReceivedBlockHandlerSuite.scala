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

package org.apache.spark.streaming

import java.io.File
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scalatest.concurrent.Eventually._

import org.apache.spark._
import org.apache.spark.network.nio.NioBlockTransferService
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.LiveListenerBus
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.shuffle.hash.HashShuffleManager
import org.apache.spark.storage._
import org.apache.spark.streaming.receiver._
import org.apache.spark.streaming.util._
import org.apache.spark.util.{ManualClock, Utils}
import WriteAheadLogBasedBlockHandler._
import WriteAheadLogSuite._

class ReceivedBlockHandlerSuite extends FunSuite with BeforeAndAfter with Matchers with Logging {

  val conf = new SparkConf().set("spark.streaming.receiver.writeAheadLog.rollingIntervalSecs", "1")
  val hadoopConf = new Configuration()
  val storageLevel = StorageLevel.MEMORY_ONLY_SER
  val streamId = 1
  val securityMgr = new SecurityManager(conf)
  val mapOutputTracker = new MapOutputTrackerMaster(conf)
  val shuffleManager = new HashShuffleManager(conf)
  val serializer = new KryoSerializer(conf)
  val manualClock = new ManualClock
  val blockManagerSize = 10000000

  var rpcEnv: RpcEnv = null
  var blockManagerMaster: BlockManagerMaster = null
  var blockManager: BlockManager = null
  var tempDirectory: File = null

  before {
    rpcEnv = RpcEnv.create("test", "localhost", 0, conf, securityMgr)
    conf.set("spark.driver.port", rpcEnv.address.port.toString)

    blockManagerMaster = new BlockManagerMaster(rpcEnv.setupEndpoint("blockmanager",
      new BlockManagerMasterEndpoint(rpcEnv, true, conf, new LiveListenerBus)), conf, true)

    blockManager = new BlockManager("bm", rpcEnv, blockManagerMaster, serializer,
      blockManagerSize, conf, mapOutputTracker, shuffleManager,
      new NioBlockTransferService(conf, securityMgr), securityMgr, 0)
    blockManager.initialize("app-id")

    tempDirectory = Utils.createTempDir()
    manualClock.setTime(0)
  }

  after {
    if (blockManager != null) {
      blockManager.stop()
      blockManager = null
    }
    if (blockManagerMaster != null) {
      blockManagerMaster.stop()
      blockManagerMaster = null
    }
    rpcEnv.shutdown()
    rpcEnv.awaitTermination()
    rpcEnv = null

    Utils.deleteRecursively(tempDirectory)
  }

  test("BlockManagerBasedBlockHandler - store blocks") {
    withBlockManagerBasedBlockHandler { handler =>
      testBlockStoring(handler) { case (data, blockIds, storeResults) =>
        // Verify the data in block manager is correct
        val storedData = blockIds.flatMap { blockId =>
          blockManager.getLocal(blockId).map(_.data.map(_.toString).toList).getOrElse(List.empty)
        }.toList
        storedData shouldEqual data

        // Verify that the store results are instances of BlockManagerBasedStoreResult
        assert(
          storeResults.forall { _.isInstanceOf[BlockManagerBasedStoreResult] },
          "Unexpected store result type"
        )
      }
    }
  }

  test("BlockManagerBasedBlockHandler - handle errors in storing block") {
    withBlockManagerBasedBlockHandler { handler =>
      testErrorHandling(handler)
    }
  }

  test("WriteAheadLogBasedBlockHandler - store blocks") {
    withWriteAheadLogBasedBlockHandler { handler =>
      testBlockStoring(handler) { case (data, blockIds, storeResults) =>
        // Verify the data in block manager is correct
        val storedData = blockIds.flatMap { blockId =>
          blockManager.getLocal(blockId).map(_.data.map(_.toString).toList).getOrElse(List.empty)
        }.toList
        storedData shouldEqual data

        // Verify that the store results are instances of WriteAheadLogBasedStoreResult
        assert(
          storeResults.forall { _.isInstanceOf[WriteAheadLogBasedStoreResult] },
          "Unexpected store result type"
        )
        // Verify the data in write ahead log files is correct
        val walSegments = storeResults.map { result =>
          result.asInstanceOf[WriteAheadLogBasedStoreResult].walRecordHandle
        }
        val loggedData = walSegments.flatMap { walSegment =>
          val fileSegment = walSegment.asInstanceOf[FileBasedWriteAheadLogSegment]
          val reader = new FileBasedWriteAheadLogRandomReader(fileSegment.path, hadoopConf)
          val bytes = reader.read(fileSegment)
          reader.close()
          blockManager.dataDeserialize(generateBlockId(), bytes).toList
        }
        loggedData shouldEqual data
      }
    }
  }

  test("WriteAheadLogBasedBlockHandler - handle errors in storing block") {
    withWriteAheadLogBasedBlockHandler { handler =>
      testErrorHandling(handler)
    }
  }

  test("WriteAheadLogBasedBlockHandler - clean old blocks") {
    withWriteAheadLogBasedBlockHandler { handler =>
      val blocks = Seq.tabulate(10) { i => IteratorBlock(Iterator(1 to i)) }
      storeBlocks(handler, blocks)

      val preCleanupLogFiles = getWriteAheadLogFiles()
      require(preCleanupLogFiles.size > 1)

      // this depends on the number of blocks inserted using generateAndStoreData()
      manualClock.getTimeMillis() shouldEqual 5000L

      val cleanupThreshTime = 3000L
      handler.cleanupOldBlocks(cleanupThreshTime)
      eventually(timeout(10000 millis), interval(10 millis)) {
        getWriteAheadLogFiles().size should be < preCleanupLogFiles.size
      }
    }
  }

  /**
   * Test storing of data using different forms of ReceivedBlocks and verify that they succeeded
   * using the given verification function
   */
  private def testBlockStoring(receivedBlockHandler: ReceivedBlockHandler)
      (verifyFunc: (Seq[String], Seq[StreamBlockId], Seq[ReceivedBlockStoreResult]) => Unit) {
    val data = Seq.tabulate(100) { _.toString }

    def storeAndVerify(blocks: Seq[ReceivedBlock]) {
      blocks should not be empty
      val (blockIds, storeResults) = storeBlocks(receivedBlockHandler, blocks)
      withClue(s"Testing with ${blocks.head.getClass.getSimpleName}s:") {
        // Verify returns store results have correct block ids
        (storeResults.map { _.blockId }) shouldEqual blockIds

        // Call handler-specific verification function
        verifyFunc(data, blockIds, storeResults)
      }
    }

    def dataToByteBuffer(b: Seq[String]) = blockManager.dataSerialize(generateBlockId, b.iterator)

    val blocks = data.grouped(10).toSeq

    storeAndVerify(blocks.map { b => IteratorBlock(b.toIterator) })
    storeAndVerify(blocks.map { b => ArrayBufferBlock(new ArrayBuffer ++= b) })
    storeAndVerify(blocks.map { b => ByteBufferBlock(dataToByteBuffer(b)) })
  }

  /** Test error handling when blocks that cannot be stored */
  private def testErrorHandling(receivedBlockHandler: ReceivedBlockHandler) {
    // Handle error in iterator (e.g. divide-by-zero error)
    intercept[Exception] {
      val iterator = (10 to (-10, -1)).toIterator.map { _ / 0 }
      receivedBlockHandler.storeBlock(StreamBlockId(1, 1), IteratorBlock(iterator))
    }

    // Handler error in block manager storing (e.g. too big block)
    intercept[SparkException] {
      val byteBuffer = ByteBuffer.wrap(new Array[Byte](blockManagerSize + 1))
      receivedBlockHandler.storeBlock(StreamBlockId(1, 1), ByteBufferBlock(byteBuffer))
    }
  }

  /** Instantiate a BlockManagerBasedBlockHandler and run a code with it */
  private def withBlockManagerBasedBlockHandler(body: BlockManagerBasedBlockHandler => Unit) {
    body(new BlockManagerBasedBlockHandler(blockManager, storageLevel))
  }

  /** Instantiate a WriteAheadLogBasedBlockHandler and run a code with it */
  private def withWriteAheadLogBasedBlockHandler(body: WriteAheadLogBasedBlockHandler => Unit) {
    require(WriteAheadLogUtils.getRollingIntervalSecs(conf, isDriver = false) === 1)
    val receivedBlockHandler = new WriteAheadLogBasedBlockHandler(blockManager, 1,
      storageLevel, conf, hadoopConf, tempDirectory.toString, manualClock)
    try {
      body(receivedBlockHandler)
    } finally {
      receivedBlockHandler.stop()
    }
  }

  /** Store blocks using a handler */
  private def storeBlocks(
      receivedBlockHandler: ReceivedBlockHandler,
      blocks: Seq[ReceivedBlock]
    ): (Seq[StreamBlockId], Seq[ReceivedBlockStoreResult]) = {
    val blockIds = Seq.fill(blocks.size)(generateBlockId())
    val storeResults = blocks.zip(blockIds).map {
      case (block, id) =>
        manualClock.advance(500) // log rolling interval set to 1000 ms through SparkConf
        logDebug("Inserting block " + id)
        receivedBlockHandler.storeBlock(id, block)
    }.toList
    logDebug("Done inserting")
    (blockIds, storeResults)
  }

  private def getWriteAheadLogFiles(): Seq[String] = {
    getLogFilesInDirectory(checkpointDirToLogDir(tempDirectory.toString, streamId))
  }

  private def generateBlockId(): StreamBlockId = StreamBlockId(streamId, scala.util.Random.nextLong)
}
