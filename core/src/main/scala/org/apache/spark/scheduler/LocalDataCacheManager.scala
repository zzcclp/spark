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
package org.apache.spark.scheduler

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.spark.internal.Logging

/**
 * Manager for executor side data cache
 *
 * init this class after SchedulerBackend.isReady
 */
class LocalDataCacheManager(strategy: CacheAllocationStrategy) extends SparkListener with Logging {

  val executors = new mutable.HashSet[String]
  val nodeExecutorMap = new mutable.HashMap[String, mutable.HashSet[String]]

  /**
   * Since the fact the executor may dead and new executor may be added,
   * we want new added executor could reuse/inherit data cache from previous lost executor.
   * To achieve this, we will use local cache slot identify. Dead executor will be removed
   * from local cache slot and new added executor will be added into empty slot, and new added
   * executor will reuse/inherit data cache from previous lost executor.
   */
  val fixedIdForExecutor = new ListBuffer[Option[String]]

  /**
   * Total count of executors
   *
   * @see make sure SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO is set to 1.0
   */
  def executorNum: Int = _executorNum

  var _initialized = false
  var _executorNum = 1

  def init(): Unit = {
    executors.foreach(exec => fixedIdForExecutor += Option(exec))
    _executorNum = executors.size
    _initialized = true
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    val exeId = executorAdded.executorId
    val host = executorAdded.executorInfo.executorHost
    val set = nodeExecutorMap.getOrElseUpdate(host, new mutable.HashSet[String]())
    set.add(exeId)
    executors.add(exeId)

    if (_initialized) {
      var added: Boolean = false
      for (i <- 0 until executorNum if !added) {
        if (fixedIdForExecutor(i).isEmpty) {
          fixedIdForExecutor(i) = Option(exeId)
          added = true
        }
      }
    }
    logDebug("onExecutorAdded:" +
      fixedIdForExecutor.map(x => x.getOrElse("N/A")).result().toString())
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    val exeId = executorRemoved.executorId
    var nodeToRemove: String = null
    executors.remove(exeId)
    for (node <- nodeExecutorMap.keySet) {
      val set = nodeExecutorMap(node)
      set.remove(node)
      if (set.isEmpty) nodeToRemove = node
    }
    if (nodeToRemove != null) {
      nodeExecutorMap.remove(nodeToRemove)
    }
    for (i <- fixedIdForExecutor.indices) {
      if (fixedIdForExecutor(i).isDefined && exeId.equals(fixedIdForExecutor(i).get)) {
        fixedIdForExecutor(i) = None // remove dead executor
      }
    }
    logDebug("onExecutorRemoved:" +
      fixedIdForExecutor.map(x => x.getOrElse("N/A")).result().toString())
  }

  /**
   * @return Array of Tuple(Host, Executor)
   */
  def askExecutor(files: Array[String]): Array[(String, String)] = {
    if (!_initialized) {
      throw new IllegalStateException("LocalDataCacheManager not initialized.")
    }
    val execs = strategy.allocate(files, fixedIdForExecutor)
    logDebug("allocated:" + execs.mkString(","))
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
  /**
   * @param files      the files which need to be cached at executor side
   * @param candidates the nodes of all executors
   * @return the two nodes which be chosen to cache files
   */
  def allocate(files: Array[String], candidates: ListBuffer[Option[String]]): Array[String]
}


class SoftAffinityStrategy extends CacheAllocationStrategy with Logging {
  override def allocate(files: Array[String], candidates: ListBuffer[Option[String]]):
  Array[String] = {
    candidates.synchronized {
      val hashId = files.head.hashCode
      val c1 = hashId % candidates.size
      val c2 = (hashId % candidates.size + 1) % candidates.size

      if (candidates(c1).isDefined) {
        if (candidates(c2).isDefined) {
          if (c1 == c2) {
            Array(candidates(c1).get)
          } else {
            Array(candidates(c1).get, candidates(c2).get)
          }
        } else {
          Array(candidates(c1).get)
        }
      } else {
        if (candidates(c2).isDefined) {
          Array(candidates(c2).get)
        } else {
          Array()
        }
      }
    }
  }
}