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

import org.apache.spark.SparkFunSuite
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.kylin.cache.scheduler.LocalDataCacheManager

class LocalDataCacheManagerSuite extends SparkFunSuite {

  test("Verify soft affinity strategy when some executors added and some executors removed") {

    val cm: LocalDataCacheManager = new LocalDataCacheManager
    cm.setTotalExceptedExecutorsNum(4)

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

    val partition1 = Array("/path/to/file1", "/path/to/file2")

    // ask cluster manager for some executors
    cm.onExecutorAdded(addEvent0)
    cm.onExecutorAdded(addEvent1)
    cm.onExecutorAdded(addEvent2)
    cm.onExecutorRemoved(removedEvent0)
    cm.onExecutorAdded(addEvent3)
    cm.onExecutorAdded(addEvent4)

    // check if init success
    assert(cm.executors.size == 4)
    assert(LocalDataCacheManager.fixedIdForExecutor.size == 4)
    assert(LocalDataCacheManager.nodeExecutorMap.size == 3)

    // case1 : all candidates alive
    val softAffinityCandidates1 = LocalDataCacheManager.askExecutor(partition1)
    assert(softAffinityCandidates1.length == 2)
    assert(softAffinityCandidates1(0)._2.startsWith("exec-2"))
    assert(softAffinityCandidates1(1)._2.startsWith("exec-3"))

    // case2 : lost first candidate for partition1
    cm.onExecutorRemoved(removedEvent1)
    assert(LocalDataCacheManager.fixedIdForExecutor.count(x => x.isDefined) == 3)
    val softAffinityCandidates2 = LocalDataCacheManager.askExecutor(partition1)
    assert(softAffinityCandidates2.length == 1)
    assert(softAffinityCandidates2(0)._2.startsWith("exec-3"))

    // case3 : lost all candidates for partition1
    cm.onExecutorRemoved(removedEvent2)
    assert(LocalDataCacheManager.fixedIdForExecutor.count(x => x.isDefined) == 2)
    val softAffinityCandidates3 = LocalDataCacheManager.askExecutor(partition1)
    assert(softAffinityCandidates3.length == 0)
    assert(cm.executors.size == 2)

    // case4 : add one new executor
    cm.onExecutorAdded(addEvent5)
    assert(LocalDataCacheManager.fixedIdForExecutor.count(x => x.isDefined) == 3)
    val softAffinityCandidates4 = LocalDataCacheManager.askExecutor(partition1)
    assert(softAffinityCandidates4.length == 1)
    assert(softAffinityCandidates4(0)._2.startsWith("exec-5"))

    // case5 : add second executor
    cm.onExecutorAdded(addEvent6)
    assert(LocalDataCacheManager.fixedIdForExecutor.count(x => x.isDefined) == 4)
    val softAffinityCandidates5 = LocalDataCacheManager.askExecutor(partition1)
    assert(softAffinityCandidates5.length == 2)
    assert(softAffinityCandidates5(0)._2.startsWith("exec-5"))
    assert(softAffinityCandidates5(1)._2.startsWith("exec-6"))

    // case6 : lost unrelated executors
    cm.onExecutorRemoved(removedEvent3)
    assert(LocalDataCacheManager.fixedIdForExecutor.count(x => x.isDefined) == 3)
    cm.onExecutorRemoved(removedEvent4)
    assert(LocalDataCacheManager.fixedIdForExecutor.count(x => x.isDefined) == 2)
    val softAffinityCandidates6 = LocalDataCacheManager.askExecutor(partition1)
    assert(softAffinityCandidates6.length == 2)
    assert(softAffinityCandidates6(0)._2.startsWith("exec-5"))
    assert(softAffinityCandidates6(1)._2.startsWith("exec-6"))
  }
}
