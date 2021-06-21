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

import com.google.common.hash.Hashing
import org.apache.kylin.cache.utils.ConsistentHash
import org.apache.kylin.softaffinity.SoftAffinityManager
import org.apache.kylin.softaffinity.scheduler.SoftAffinityListener
import org.apache.spark.{LocalSparkContext, SparkContext, SparkFunSuite}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.cluster.ExecutorInfo

import scala.collection.mutable

class LocalDataCacheManagerSuite extends SparkFunSuite with LocalSparkContext{

  test("Verify soft affinity strategy when some executors added and some executors removed") {

    sc = new SparkContext("local", "test")
    val listener = new SoftAffinityListener

    val addEvent0 = SparkListenerExecutorAdded(System.currentTimeMillis(), "exec-0",
      new ExecutorInfo("host-1", 3, null, null, null, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))
    val addEvent1 = SparkListenerExecutorAdded(System.currentTimeMillis(), "exec-1",
      new ExecutorInfo("host-1", 3, null, null, null, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))
    val addEvent2 = SparkListenerExecutorAdded(System.currentTimeMillis(), "exec-2",
      new ExecutorInfo("host-2", 3, null, null, null, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))
    val addEvent3 = SparkListenerExecutorAdded(System.currentTimeMillis(), "exec-3",
      new ExecutorInfo("host-3", 3, null, null, null, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))
    val addEvent4 = SparkListenerExecutorAdded(System.currentTimeMillis(), "exec-4",
      new ExecutorInfo("host-3", 3, null, null, null, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))
    val addEvent5 = SparkListenerExecutorAdded(System.currentTimeMillis(), "exec-5",
      new ExecutorInfo("host-2", 3, null, null, null, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))
    val addEvent6 = SparkListenerExecutorAdded(System.currentTimeMillis(), "exec-6",
      new ExecutorInfo("host-4", 3, null, null, null, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))

    val removedEvent0 = SparkListenerExecutorRemoved(System.currentTimeMillis(), "exec-0", "")
    val removedEvent1 = SparkListenerExecutorRemoved(System.currentTimeMillis(), "exec-2", "")
    val removedEvent2 = SparkListenerExecutorRemoved(System.currentTimeMillis(), "exec-3", "")
    val removedEvent3 = SparkListenerExecutorRemoved(System.currentTimeMillis(), "exec-1", "")
    val removedEvent4 = SparkListenerExecutorRemoved(System.currentTimeMillis(), "exec-4", "")

    val filePath = "/path/to/file1"

    // ask cluster manager for some executors
    listener.onExecutorAdded(addEvent0)
    listener.onExecutorAdded(addEvent1)
    listener.onExecutorAdded(addEvent2)
    listener.onExecutorRemoved(removedEvent0)
    listener.onExecutorAdded(addEvent3)
    listener.onExecutorAdded(addEvent4)

    // check if init success
    assert(SoftAffinityManager.totalExecutors() == 4)
    assert(SoftAffinityManager.fixedIdForExecutors.size == 4)
    assert(SoftAffinityManager.nodesExecutorsMap.size == 3)

    // case1 : all candidates alive
    val softAffinityCandidates1 = SoftAffinityManager.askExecutors(filePath)
    assert(softAffinityCandidates1.length == 2)
    assert(softAffinityCandidates1(0)._1.startsWith("exec-3"))
    assert(softAffinityCandidates1(1)._1.startsWith("exec-4"))

    // case2 : lost first candidate for partition1
    listener.onExecutorRemoved(removedEvent2)
    assert(SoftAffinityManager.fixedIdForExecutors.count(x => x.isDefined) == 3)
    val softAffinityCandidates2 = SoftAffinityManager.askExecutors(filePath)
    assert(softAffinityCandidates2.length == 1)
    assert(softAffinityCandidates2(0)._1.startsWith("exec-4"))

    // case3 : lost all candidates for partition1
    listener.onExecutorRemoved(removedEvent1)
    assert(SoftAffinityManager.fixedIdForExecutors.count(x => x.isDefined) == 2)
    val softAffinityCandidates3 = SoftAffinityManager.askExecutors(filePath)
    assert(softAffinityCandidates3.length == 1)
    assert(SoftAffinityManager.totalExecutors() == 2)

    // case4 : add one new executor
    listener.onExecutorAdded(addEvent5)
    assert(SoftAffinityManager.fixedIdForExecutors.count(x => x.isDefined) == 3)
    val softAffinityCandidates4 = SoftAffinityManager.askExecutors(filePath)
    assert(softAffinityCandidates4.length == 2)
    assert(softAffinityCandidates4(0)._1.startsWith("exec-5"))
    assert(softAffinityCandidates4(1)._1.startsWith("exec-4"))

    // case5 : add second executor
    listener.onExecutorAdded(addEvent6)
    assert(SoftAffinityManager.fixedIdForExecutors.count(x => x.isDefined) == 4)
    val softAffinityCandidates5 = SoftAffinityManager.askExecutors(filePath)
    assert(softAffinityCandidates5.length == 2)
    assert(softAffinityCandidates5(0)._1.startsWith("exec-5"))
    assert(softAffinityCandidates5(1)._1.startsWith("exec-4"))

    // case6 : lost unrelated executors
    listener.onExecutorRemoved(removedEvent3)
    assert(SoftAffinityManager.fixedIdForExecutors.count(x => x.isDefined) == 3)
    listener.onExecutorRemoved(removedEvent4)
    assert(SoftAffinityManager.fixedIdForExecutors.count(x => x.isDefined) == 2)
    val softAffinityCandidates6 = SoftAffinityManager.askExecutors(filePath)
    assert(softAffinityCandidates6.length == 1)
    assert(softAffinityCandidates6(0)._1.startsWith("exec-5"))
  }

  test("test hash code") {

    val candidatesSize = 5
    val resultSet = new mutable.HashMap[Int, Int]()
    val executorHash = Hashing.crc32()
    val cacheReplicatesNum = 1
    val halfCandidatesSize = candidatesSize / cacheReplicatesNum
    for (i <- 0 to 74) {
      if (i != 71 && i != 73) {
        val fileName =
          //("s3a://raptorx-test/alluxio/data/raptorx_stress_data_s3/raptorx_stress_data/" +
          ("jfs://raptorx1/" +
            "part-%05d-1a66979d-d780-4d30-8390-8893914fcfe4-c000.snappy.parquet").format(i)
        println(fileName)
        // var mod = executorHash.hashBytes(fileName.getBytes).asInt % candidatesSize
        var mod = fileName.hashCode % candidatesSize
        val c1 = if (mod < 0) (mod + candidatesSize) else mod
        println(c1)
        var value = resultSet.getOrElse(c1, 0)
        resultSet(c1) = value + 1

        for (i <- 1 to (cacheReplicatesNum - 1)) {
          val c2 = (c1 + halfCandidatesSize + i) % candidatesSize
          println(c2)
          var value = resultSet.getOrElse(c2, 0)
          resultSet(c2) = value + 1
        }
      }
    }
    println(resultSet)
  }

  test("test ConsistentHash") {

    val virtualNodesNum = 1
    val consistentHash = new ConsistentHash[String](virtualNodesNum)

    val candidatesSize = 15
    for (i <- 0 until candidatesSize) {
      consistentHash.addNode(i.toString)
    }
    val resultSet = new mutable.HashMap[Int, Int]()
    val cacheReplicatesNum = 2
    val halfCandidatesSize = candidatesSize / cacheReplicatesNum
    for (i <- 0 to 73) {
      val fileName =
        "s3a://raptorx-test/alluxio/data/raptorx_stress_data_s3/raptorx_stress_data/part-%05d-1a66979d-d780-4d30-8390-8893914fcfe4-c000.snappy.parquet".format(i)
      println(fileName)
      val c1 = consistentHash.get(fileName).toInt
      var value = resultSet.getOrElse(c1, 0)
      resultSet(c1) = value + 1

      for (i <- 1 to (cacheReplicatesNum - 1)) {
        val c2 = consistentHash.get(fileName + i.toString).toInt
        println(c2)
        var value = resultSet.getOrElse(c2, 0)
        resultSet(c2) = value + 1
      }
    }
    println(resultSet)
  }
}
