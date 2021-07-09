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
package org.apache.kylin.cache;

public class KylinCacheConstants {

    private KylinCacheConstants() {
    }

    // Todo: change the param key name
    public static final String KYLIN_CACHE_FS =
            "org.apache.kylin.cache.fs.kylin.KylinCacheFileSystem";

    public static final String PARAMS_KEY_TOTAL_EXCEPTED_EXECUTORS_NUM =
            "spark.kylin.local-cache.total.excepted.executors.num";

    public static final String PARAMS_KEY_CACHE_REPLACATES_NUM =
            "spark.kylin.local-cache.cache.replacates.num";

    public static final int PARAMS_KEY_CACHE_REPLACATES_NUM_DEFAULT_VALUE = 2;
}
