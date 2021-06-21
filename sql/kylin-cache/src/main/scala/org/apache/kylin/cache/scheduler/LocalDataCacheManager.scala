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

package org.apache.kylin.cache.scheduler

import org.apache.kylin.cache.KylinCacheConstants
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorAdded, SparkListenerExecutorRemoved}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Manager for executor side data cache
 *
 * init this class after SchedulerBackend.isReady
 */
class LocalDataCacheManager extends SparkListener with Logging {

  // Todo: Calculate according to the yarn mode or standalone mode
  protected var totalExceptedExecutors = if (SparkEnv.get != null) {
    SparkEnv.get.conf.getInt(
      KylinCacheConstants.PARAMS_KEY_TOTAL_EXCEPTED_EXECUTORS_NUM, 1)
  } else {
    1
  }

  protected val totalRegisteredExecutors = new AtomicInteger(0)
  val executors = new mutable.HashSet[String]

  /**
   * Total count of executors
   *
   * @see make sure SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO is set to 1.0
   */
  def executorNum: Int = _executorNum

  var _initialized = false
  var _executorNum = 1

  def init(): Unit = {
    executors.foreach(exec => LocalDataCacheManager.fixedIdForExecutor += Option(exec))
    _executorNum = executors.size
    _initialized = true
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit =
    synchronized {
    val exeId = executorAdded.executorId
    val host = executorAdded.executorInfo.executorHost
    val set = LocalDataCacheManager.nodeExecutorMap
      .getOrElseUpdate(host, new mutable.HashSet[String]())
    set.add(exeId)
    executors.add(exeId)
    totalRegisteredExecutors.addAndGet(1)

    if (!_initialized && (totalRegisteredExecutors.get() >= totalExceptedExecutors)) {
      init()
    }

    if (_initialized) {
      var added: Boolean = false
      for (i <- 0 until executorNum if !added) {
        if (LocalDataCacheManager.fixedIdForExecutor(i).isEmpty) {
          LocalDataCacheManager.fixedIdForExecutor(i) = Option(exeId)
          added = true
        }
      }
    }
    logDebug("onExecutorAdded:" +
      LocalDataCacheManager.fixedIdForExecutor.map(x => x.getOrElse("N/A")).result().toString())
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit =
    synchronized {
    val exeId = executorRemoved.executorId
    var nodeToRemove: String = null
    executors.remove(exeId)
    totalRegisteredExecutors.addAndGet(-1)
    for (node <- LocalDataCacheManager.nodeExecutorMap.keySet) {
      val set = LocalDataCacheManager.nodeExecutorMap(node)
      set.remove(node)
      if (set.isEmpty) nodeToRemove = node
    }
    if (nodeToRemove != null) {
      LocalDataCacheManager.nodeExecutorMap.remove(nodeToRemove)
    }
    for (i <- LocalDataCacheManager.fixedIdForExecutor.indices) {
      if (LocalDataCacheManager.fixedIdForExecutor(i).isDefined
        && exeId.equals(LocalDataCacheManager.fixedIdForExecutor(i).get)) {
        LocalDataCacheManager.fixedIdForExecutor(i) = None // remove dead executor
      }
    }
    logDebug("onExecutorRemoved:" +
      LocalDataCacheManager.fixedIdForExecutor.map(x => x.getOrElse("N/A")).result().toString())
  }

  /**
   * Only for testing
   */
  def setTotalExceptedExecutorsNum(exceptedExecutorsNum: Int): Unit = {
    totalExceptedExecutors = exceptedExecutorsNum
  }
}

object LocalDataCacheManager {

  // Todo: Generate 'CacheAllocationStrategy' according to the config
  val strategy: CacheAllocationStrategy = new SoftAffinityStrategy

  /**
   * Since the fact the executor may dead and new executor may be added,
   * we want new added executor could reuse/inherit data cache from previous lost executor.
   * To achieve this, we will use local cache slot identify. Dead executor will be removed
   * from local cache slot and new added executor will be added into empty slot, and new added
   * executor will reuse/inherit data cache from previous lost executor.
   */
  val fixedIdForExecutor = new ListBuffer[Option[String]]

  val nodeExecutorMap = new mutable.HashMap[String, mutable.HashSet[String]]

  /**
   * @return Array of Tuple(Host, Executor)
   */
  def askExecutor(files: Array[String]): Array[(String, String)] = {
    // Todo: wait for all executors registering
    val execs = strategy.allocate(files, fixedIdForExecutor)
    execs.map(exe => {
      var host = ""
      for (h <- nodeExecutorMap.keySet) {
        if (nodeExecutorMap(h).contains(exe)) {
          host = h
        }
      }
      (host, exe)
    })
  }
}

trait CacheAllocationStrategy {

  protected val cacheReplicatesNum = if (SparkEnv.get != null) {
    SparkEnv.get.conf.getInt(
      KylinCacheConstants.PARAMS_KEY_CACHE_REPLACATES_NUM,
      KylinCacheConstants.PARAMS_KEY_CACHE_REPLACATES_NUM_DEFAULT_VALUE)
  } else {
    KylinCacheConstants.PARAMS_KEY_CACHE_REPLACATES_NUM_DEFAULT_VALUE
  }

  /**
   * @param files      the files which need to be cached at executor side
   * @param candidates the nodes of all executors
   * @return the two nodes which be chosen to cache files
   */
  def allocate(files: Array[String], candidates: ListBuffer[Option[String]]): Array[String]
}

class SoftAffinityStrategy extends CacheAllocationStrategy with Logging {

  def allocate(files: Array[String],
               candidates: ListBuffer[Option[String]]): Array[String] = {
    val candidatesSize = candidates.size
    val halfCandidatesSize = candidatesSize / cacheReplicatesNum
    val resultSet = new mutable.LinkedHashSet[String]

    var mod = files.head.hashCode % candidatesSize
    val c1 = if (mod < 0) (mod + candidatesSize) else mod
    if (candidates(c1).isDefined) {
      resultSet.add(candidates(c1).get)
    }
    for (i <- 1 to (cacheReplicatesNum - 1)) {
      val c2 = (c1 + halfCandidatesSize + i) % candidatesSize
      if (candidates(c2).isDefined) {
        resultSet.add(candidates(c2).get)
      }
    }
    resultSet.toArray
  }
}