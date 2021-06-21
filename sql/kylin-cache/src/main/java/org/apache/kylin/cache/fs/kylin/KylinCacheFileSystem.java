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
package org.apache.kylin.cache.fs.kylin;

import alluxio.hadoop.LocalCacheFileSystem;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kylin.cache.KylinCacheConstants;
import org.apache.kylin.cache.utils.ReflectionUtil;
import org.apache.spark.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class KylinCacheFileSystem extends FilterFileSystem {

    private static final Logger LOG = LoggerFactory.getLogger(KylinCacheFileSystem.class);

    protected URI uri;
    protected String originalScheme;
    protected int bufferSize = 4096;
    protected boolean useLocalCache = false;
    protected LocalCacheFileSystem cacheFileSystem;

    protected static Map<String, String> schemeClassMap = new HashMap<>();

    static {
        schemeClassMap.put("file", "org.apache.hadoop.fs.LocalFileSystem");
        schemeClassMap.put("viewfs", "org.apache.hadoop.fs.viewfs.ViewFileSystem");
        schemeClassMap.put("s3a", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        schemeClassMap.put("s3", "org.apache.hadoop.fs.s3.S3FileSystem");
        schemeClassMap.put("hdfs", "org.apache.hadoop.hdfs.DistributedFileSystem");
        schemeClassMap.put("wasb", "org.apache.hadoop.fs.azure.NativeAzureFileSystem");
        schemeClassMap.put("wasbs", "org.apache.hadoop.fs.azure.NativeAzureFileSystem$Secure");
        schemeClassMap.put("jfs", "io.juicefs.JuiceFileSystem");
        schemeClassMap.put("alluxio", "alluxio.hadoop.FileSystem");
    }

    /**
     * Create internal FileSystem
     */
    protected static FileSystem createInternalFS(URI uri, Configuration conf)
            throws IOException {
        if (!schemeClassMap.containsKey(uri.getScheme())) {
            throw new IOException("No FileSystem for scheme: " + uri.getScheme());
        }
        FileSystem fs = null;
        try {
            Class<? extends FileSystem> clazz =
                    (Class<? extends FileSystem>) conf.getClassByName(
                            schemeClassMap.get(uri.getScheme()));
            fs = ReflectionUtils.newInstance(clazz, conf);
            fs.initialize(uri, conf);
            LOG.info("Create filesystem {} for scheme {} .",
                    schemeClassMap.get(uri.getScheme()), uri.getScheme());
        } catch (ClassNotFoundException e) {
            throw new IOException("Can not found FileSystem Clazz for scheme: " + uri.getScheme());
        }
        return fs;
    }

    public KylinCacheFileSystem() {
        // do the init in method 'initialize'
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        this.originalScheme = name.getScheme();
        // create internal FileSystem according to the scheme
        this.fs = createInternalFS(name, conf);
        this.statistics = (FileSystem.Statistics) ReflectionUtil.getFieldValue(this.fs, "statistics");
        super.initialize(name, conf);

        this.bufferSize = conf.getInt(KylinCacheConstants.PARAMS_KEY_IO_FILE_BUFFER_SIZE,
                KylinCacheConstants.PARAMS_KEY_IO_FILE_BUFFER_SIZE_DEFAULT_VALUE);
        // when scheme is jfs, use the cache by jfs itself
        this.useLocalCache = conf.getBoolean(KylinCacheConstants.PARAMS_KEY_USE_CACHE,
                KylinCacheConstants.PARAMS_KEY_USE_CACHE_DEFAULT_VALUE) && !this.originalScheme.equals("jfs");
        // create LocalCacheFileSystem if needs
        if (this.isUseLocalCache()) {
            // Todo: Can set local cache dir here for the current executor
            this.cacheFileSystem = new LocalCacheFileSystem(this.fs);
            this.cacheFileSystem.initialize(this.getUri(), conf);
            LOG.info("Create LocalCacheFileSystem successfully .");
        }
    }

    @Override
    public String getScheme() {
        return this.originalScheme;
    }

    @Override
    public FSDataInputStream open(Path f) throws IOException {
        return open(f, bufferSize);
    }

    /**
     * Check whether needs to cache data on the current executor
     */
    public boolean isUseLocalCacheForTargetExecs() {
        String localCacheForCurrExecutor =
                TaskContext.get()
                        .getLocalProperty(KylinCacheConstants.PARAMS_KEY_LOCAL_CACHE_FOR_CURRENT_EXECUTOR);
        return (StringUtils.isNotBlank(localCacheForCurrExecutor) && Boolean.valueOf(localCacheForCurrExecutor));
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        if (this.isUseLocalCache() && this.isUseLocalCacheForTargetExecs()) {
            LOG.info("Use LocalCacheFileSystem to open file {} .", f);
            return this.cacheFileSystem.open(f, bufferSize);
        }
        LOG.info("Use original FileSystem to open file {} .", f);
        return super.open(f, bufferSize);
    }

    /**
     * Only for testing
     */
    public FSDataInputStream open(Path f, int bufferSize, boolean useLocalCacheForExec) throws IOException {
        if (this.isUseLocalCache() && useLocalCacheForExec) {
            LOG.info("Use LocalCacheFileSystem to open file {} .", f);
            return this.cacheFileSystem.open(f, bufferSize);
        }
        LOG.info("Use original FileSystem to open file {} .", f);
        return super.open(f, bufferSize);
    }

    public LocalCacheFileSystem getCacheFileSystem() {
        return this.cacheFileSystem;
    }

    public boolean isUseLocalCache() {
        return useLocalCache;
    }

    public void setUseLocalCache(boolean useLocalCache) {
        this.useLocalCache = useLocalCache;
    }
}
