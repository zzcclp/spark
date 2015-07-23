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

package org.apache.spark.streaming.scheduler

import scala.collection.mutable.{ArrayBuffer, HashMap, SynchronizedMap}
import scala.language.existentials
import scala.math.max

import org.apache.spark.streaming.util.WriteAheadLogUtils
import org.apache.spark.{Logging, SparkEnv, SparkException}
import org.apache.spark.rpc._
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.receiver.{CleanupOldBlocks, Receiver, ReceiverSupervisorImpl,
  StopReceiver, UpdateRateLimit}
import org.apache.spark.util.SerializableConfiguration

/**
 * Messages used by the NetworkReceiver and the ReceiverTracker to communicate
 * with each other.
 */
private[streaming] sealed trait ReceiverTrackerMessage
private[streaming] case class RegisterReceiver(
    streamId: Int,
    typ: String,
    host: String,
    receiverEndpoint: RpcEndpointRef
  ) extends ReceiverTrackerMessage
private[streaming] case class AddBlock(receivedBlockInfo: ReceivedBlockInfo)
  extends ReceiverTrackerMessage
private[streaming] case class ReportError(streamId: Int, message: String, error: String)
private[streaming] case class DeregisterReceiver(streamId: Int, msg: String, error: String)
  extends ReceiverTrackerMessage

private[streaming] case object StopAllReceivers extends ReceiverTrackerMessage

/**
 * This class manages the execution of the receivers of ReceiverInputDStreams. Instance of
 * this class must be created after all input streams have been added and StreamingContext.start()
 * has been called because it needs the final set of input streams at the time of instantiation.
 *
 * @param skipReceiverLaunch Do not launch the receiver. This is useful for testing.
 */
private[streaming]
class ReceiverTracker(ssc: StreamingContext, skipReceiverLaunch: Boolean = false) extends Logging {

  private val receiverInputStreams = ssc.graph.getReceiverInputStreams()
  private val receiverInputStreamIds = receiverInputStreams.map { _.id }
  private val receiverExecutor = new ReceiverLauncher()
  private val receiverInfo = new HashMap[Int, ReceiverInfo] with SynchronizedMap[Int, ReceiverInfo]
  private val receivedBlockTracker = new ReceivedBlockTracker(
    ssc.sparkContext.conf,
    ssc.sparkContext.hadoopConfiguration,
    receiverInputStreamIds,
    ssc.scheduler.clock,
    ssc.isCheckpointPresent,
    Option(ssc.checkpointDir)
  )
  private val listenerBus = ssc.scheduler.listenerBus

  /** Enumeration to identify current state of the ReceiverTracker */
  object TrackerState extends Enumeration {
    type TrackerState = Value
    val Initialized, Started, Stopping, Stopped = Value
  }
  import TrackerState._

  /** State of the tracker. Protected by "trackerStateLock" */
  @volatile private var trackerState = Initialized

  // endpoint is created when generator starts.
  // This not being null means the tracker has been started and not stopped
  private var endpoint: RpcEndpointRef = null

  /** Start the endpoint and receiver execution thread. */
  def start(): Unit = synchronized {
    if (isTrackerStarted) {
      throw new SparkException("ReceiverTracker already started")
    }

    if (!receiverInputStreams.isEmpty) {
      endpoint = ssc.env.rpcEnv.setupEndpoint(
        "ReceiverTracker", new ReceiverTrackerEndpoint(ssc.env.rpcEnv))
      if (!skipReceiverLaunch) receiverExecutor.start()
      logInfo("ReceiverTracker started")
      trackerState = Started
    }
  }

  /** Stop the receiver execution thread. */
  def stop(graceful: Boolean): Unit = synchronized {
    if (isTrackerStarted) {
      // First, stop the receivers
      trackerState = Stopping
      if (!skipReceiverLaunch) {
        // Send the stop signal to all the receivers
        endpoint.askWithRetry[Boolean](StopAllReceivers)

        // Wait for the Spark job that runs the receivers to be over
        // That is, for the receivers to quit gracefully.
        receiverExecutor.awaitTermination(10000)

        if (graceful) {
          val pollTime = 100
          logInfo("Waiting for receiver job to terminate gracefully")
          while (receiverInfo.nonEmpty || receiverExecutor.running) {
            Thread.sleep(pollTime)
          }
          logInfo("Waited for receiver job to terminate gracefully")
        }

        // Check if all the receivers have been deregistered or not
        if (receiverInfo.nonEmpty) {
          logWarning("Not all of the receivers have deregistered, " + receiverInfo)
        } else {
          logInfo("All of the receivers have deregistered successfully")
        }
      }

      // Finally, stop the endpoint
      ssc.env.rpcEnv.stop(endpoint)
      endpoint = null
      receivedBlockTracker.stop()
      logInfo("ReceiverTracker stopped")
      trackerState = Stopped
    }
  }

  /** Allocate all unallocated blocks to the given batch. */
  def allocateBlocksToBatch(batchTime: Time): Unit = {
    if (receiverInputStreams.nonEmpty) {
      receivedBlockTracker.allocateBlocksToBatch(batchTime)
    }
  }

  /** Get the blocks for the given batch and all input streams. */
  def getBlocksOfBatch(batchTime: Time): Map[Int, Seq[ReceivedBlockInfo]] = {
    receivedBlockTracker.getBlocksOfBatch(batchTime)
  }

  /** Get the blocks allocated to the given batch and stream. */
  def getBlocksOfBatchAndStream(batchTime: Time, streamId: Int): Seq[ReceivedBlockInfo] = {
    synchronized {
      receivedBlockTracker.getBlocksOfBatchAndStream(batchTime, streamId)
    }
  }

  /**
   * Clean up the data and metadata of blocks and batches that are strictly
   * older than the threshold time. Note that this does not
   */
  def cleanupOldBlocksAndBatches(cleanupThreshTime: Time) {
    // Clean up old block and batch metadata
    receivedBlockTracker.cleanupOldBatches(cleanupThreshTime, waitForCompletion = false)

    // Signal the receivers to delete old block data
    if (WriteAheadLogUtils.enableReceiverLog(ssc.conf)) {
      logInfo(s"Cleanup old received batch data: $cleanupThreshTime")
      receiverInfo.values.flatMap { info => Option(info.endpoint) }
        .foreach { _.send(CleanupOldBlocks(cleanupThreshTime)) }
    }
  }

  /** Register a receiver */
  private def registerReceiver(
      streamId: Int,
      typ: String,
      host: String,
      receiverEndpoint: RpcEndpointRef,
      senderAddress: RpcAddress
    ): Boolean = {
    if (!receiverInputStreamIds.contains(streamId)) {
      throw new SparkException("Register received for unexpected id " + streamId)
    }

    if (isTrackerStopping || isTrackerStopped) {
      false
    } else {
      // "stopReceivers" won't happen at the same time because both "registerReceiver" and are
      // called in the event loop. So here we can assume "stopReceivers" has not yet been called. If
      // "stopReceivers" is called later, it should be able to see this receiver.
      receiverInfo(streamId) = ReceiverInfo(
        streamId, s"${typ}-${streamId}", receiverEndpoint, true, host)
      listenerBus.post(StreamingListenerReceiverStarted(receiverInfo(streamId)))
      logInfo("Registered receiver for stream " + streamId + " from " + senderAddress)
      true
    }
  }

  /** Deregister a receiver */
  private def deregisterReceiver(streamId: Int, message: String, error: String) {
    val newReceiverInfo = receiverInfo.get(streamId) match {
      case Some(oldInfo) =>
        val lastErrorTime =
          if (error == null || error == "") -1 else ssc.scheduler.clock.getTimeMillis()
        oldInfo.copy(endpoint = null, active = false, lastErrorMessage = message,
          lastError = error, lastErrorTime = lastErrorTime)
      case None =>
        logWarning("No prior receiver info")
        val lastErrorTime =
          if (error == null || error == "") -1 else ssc.scheduler.clock.getTimeMillis()
        ReceiverInfo(streamId, "", null, false, "", lastErrorMessage = message,
          lastError = error, lastErrorTime = lastErrorTime)
    }
    receiverInfo -= streamId
    listenerBus.post(StreamingListenerReceiverStopped(newReceiverInfo))
    val messageWithError = if (error != null && !error.isEmpty) {
      s"$message - $error"
    } else {
      s"$message"
    }
    logError(s"Deregistered receiver for stream $streamId: $messageWithError")
  }

  /** Update a receiver's maximum ingestion rate */
  def sendRateUpdate(streamUID: Int, newRate: Long): Unit = {
    for (info <- receiverInfo.get(streamUID); eP <- Option(info.endpoint)) {
      eP.send(UpdateRateLimit(newRate))
    }
  }

  /** Add new blocks for the given stream */
  private def addBlock(receivedBlockInfo: ReceivedBlockInfo): Boolean = {
    receivedBlockTracker.addBlock(receivedBlockInfo)
  }

  /** Report error sent by a receiver */
  private def reportError(streamId: Int, message: String, error: String) {
    val newReceiverInfo = receiverInfo.get(streamId) match {
      case Some(oldInfo) =>
        oldInfo.copy(lastErrorMessage = message, lastError = error)
      case None =>
        logWarning("No prior receiver info")
        ReceiverInfo(streamId, "", null, false, "", lastErrorMessage = message,
          lastError = error, lastErrorTime = ssc.scheduler.clock.getTimeMillis())
    }
    receiverInfo(streamId) = newReceiverInfo
    listenerBus.post(StreamingListenerReceiverError(receiverInfo(streamId)))
    val messageWithError = if (error != null && !error.isEmpty) {
      s"$message - $error"
    } else {
      s"$message"
    }
    logWarning(s"Error reported by receiver for stream $streamId: $messageWithError")
  }

  /** Check if any blocks are left to be processed */
  def hasUnallocatedBlocks: Boolean = {
    receivedBlockTracker.hasUnallocatedReceivedBlocks
  }

  /** RpcEndpoint to receive messages from the receivers. */
  private class ReceiverTrackerEndpoint(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint {

    override def receive: PartialFunction[Any, Unit] = {
      case ReportError(streamId, message, error) =>
        reportError(streamId, message, error)
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case RegisterReceiver(streamId, typ, host, receiverEndpoint) =>
        val successful =
          registerReceiver(streamId, typ, host, receiverEndpoint, context.sender.address)
        context.reply(successful)
      case AddBlock(receivedBlockInfo) =>
        context.reply(addBlock(receivedBlockInfo))
      case DeregisterReceiver(streamId, message, error) =>
        deregisterReceiver(streamId, message, error)
        context.reply(true)
      case StopAllReceivers =>
        assert(isTrackerStopping || isTrackerStopped)
        stopReceivers()
        context.reply(true)
    }

    /** Send stop signal to the receivers. */
    private def stopReceivers() {
      // Signal the receivers to stop
      receiverInfo.values.flatMap { info => Option(info.endpoint)}
        .foreach { _.send(StopReceiver) }
      logInfo("Sent stop signal to all " + receiverInfo.size + " receivers")
    }
  }

  /** This thread class runs all the receivers on the cluster.  */
  class ReceiverLauncher {
    @transient val env = ssc.env
    @volatile @transient var running = false
    @transient val thread = new Thread() {
      override def run() {
        try {
          SparkEnv.set(env)
          startReceivers()
        } catch {
          case ie: InterruptedException => logInfo("ReceiverLauncher interrupted")
        }
      }
    }

    def start() {
      thread.start()
    }

    /**
     * Get the list of executors excluding driver
     */
    private def getExecutors(ssc: StreamingContext): List[String] = {
      val executors = ssc.sparkContext.getExecutorMemoryStatus.map(_._1.split(":")(0)).toList
      val driver = ssc.sparkContext.getConf.get("spark.driver.host")
      executors.diff(List(driver))
    }

    /** Set host location(s) for each receiver so as to distribute them over
     * executors in a round-robin fashion taking into account preferredLocation if set
     */
    private[streaming] def scheduleReceivers(receivers: Seq[Receiver[_]],
      executors: List[String]): Array[ArrayBuffer[String]] = {
      val locations = new Array[ArrayBuffer[String]](receivers.length)
      var i = 0
      for (i <- 0 until receivers.length) {
        locations(i) = new ArrayBuffer[String]()
        if (receivers(i).preferredLocation.isDefined) {
          locations(i) += receivers(i).preferredLocation.get
        }
      }
      var count = 0
      for (i <- 0 until max(receivers.length, executors.length)) {
        if (!receivers(i % receivers.length).preferredLocation.isDefined) {
          locations(i % receivers.length) += executors(count)
          count += 1
          if (count == executors.length) {
            count = 0
          }
        }
      }
      locations
    }

    /**
     * Get the receivers from the ReceiverInputDStreams, distributes them to the
     * worker nodes as a parallel collection, and runs them.
     */
    private def startReceivers() {
      val receivers = receiverInputStreams.map(nis => {
        val rcvr = nis.getReceiver()
        rcvr.setReceiverId(nis.id)
        rcvr
      })

      val checkpointDirOption = Option(ssc.checkpointDir)
      val serializableHadoopConf =
        new SerializableConfiguration(ssc.sparkContext.hadoopConfiguration)

      // Function to start the receiver on the worker node
      val startReceiver = (iterator: Iterator[Receiver[_]]) => {
        if (!iterator.hasNext) {
          throw new SparkException(
            "Could not start receiver as object not found.")
        }
        val receiver = iterator.next()
        val supervisor = new ReceiverSupervisorImpl(
          receiver, SparkEnv.get, serializableHadoopConf.value, checkpointDirOption)
        supervisor.start()
        supervisor.awaitTermination()
      }

      // Run the dummy Spark job to ensure that all slaves have registered.
      // This avoids all the receivers to be scheduled on the same node.
      if (!ssc.sparkContext.isLocal) {
        ssc.sparkContext.makeRDD(1 to 50, 50).map(x => (x, 1)).reduceByKey(_ + _, 20).collect()
      }

      // Get the list of executors and schedule receivers
      val executors = getExecutors(ssc)
      val tempRDD =
        if (!executors.isEmpty) {
          val locations = scheduleReceivers(receivers, executors)
          val roundRobinReceivers = (0 until receivers.length).map(i =>
            (receivers(i), locations(i)))
          ssc.sc.makeRDD[Receiver[_]](roundRobinReceivers)
        } else {
          ssc.sc.makeRDD(receivers, receivers.size)
        }

      // Distribute the receivers and start them
      logInfo("Starting " + receivers.length + " receivers")
      running = true
      try {
        ssc.sparkContext.runJob(tempRDD, ssc.sparkContext.clean(startReceiver))
        logInfo("All of the receivers have been terminated")
      } finally {
        running = false
      }
    }

    /**
     * Wait until the Spark job that runs the receivers is terminated, or return when
     * `milliseconds` elapses
     */
    def awaitTermination(milliseconds: Long): Unit = {
      thread.join(milliseconds)
    }
  }

  /** Check if tracker has been marked for starting */
  private def isTrackerStarted(): Boolean = trackerState == Started

  /** Check if tracker has been marked for stopping */
  private def isTrackerStopping(): Boolean = trackerState == Stopping

  /** Check if tracker has been marked for stopped */
  private def isTrackerStopped(): Boolean = trackerState == Stopped

}
