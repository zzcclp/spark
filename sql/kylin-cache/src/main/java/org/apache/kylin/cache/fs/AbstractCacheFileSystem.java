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
package org.apache.kylin.cache.fs;

import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.LocalCacheFileInStream;
import alluxio.conf.AlluxioConfiguration;
import alluxio.hadoop.AlluxioHdfsInputStream;
import alluxio.hadoop.HadoopFileOpener;
import alluxio.hadoop.HadoopUtils;
import alluxio.hadoop.HdfsFileInputStream;
import alluxio.metrics.MetricsConfig;
import alluxio.metrics.MetricsSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kylin.cache.utils.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractCacheFileSystem extends FilterFileSystem {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractCacheFileSystem.class);

    protected URI uri;
    protected String originalScheme;
    protected int bufferSize = 4096;
    protected boolean useLocalCache = false;
    protected HadoopFileOpener mHadoopFileOpener;
    protected LocalCacheFileInStream.FileInStreamOpener mAlluxioFileOpener;
    protected CacheManager mCacheManager;
    protected AlluxioConfiguration mAlluxioConf;

    protected ConcurrentHashMap<Path, FileStatus> fileStatusMap =
            new ConcurrentHashMap<Path, FileStatus>();

    // put("s3", "com.amazon.ws.emr.hadoop.fs.EmrFileSystem");
    // put("s3n", "com.amazon.ws.emr.hadoop.fs.EmrFileSystem");
    // put("s3bfs", "org.apache.hadoop.fs.s3.S3FileSystem");
    protected static final Map<String, String> schemeClassMap = new HashMap<String, String>() {
        {
            put("file", "org.apache.hadoop.fs.LocalFileSystem");
            put("viewfs", "org.apache.hadoop.fs.viewfs.ViewFileSystem");
            put("s3a", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            put("s3", "org.apache.hadoop.fs.s3.S3FileSystem");
            put("s3n", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
            put("hdfs", "org.apache.hadoop.hdfs.DistributedFileSystem");
            put("wasb", "org.apache.hadoop.fs.azure.NativeAzureFileSystem");
            put("wasbs", "org.apache.hadoop.fs.azure.NativeAzureFileSystem$Secure");
            put("jfs", "io.juicefs.JuiceFileSystem");
            put("alluxio", "alluxio.hadoop.FileSystem");
        }
    };

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

    protected void createLocalCacheManager(URI name, Configuration conf) throws IOException{
        mHadoopFileOpener = uriStatus -> this.fs.open(new Path(uriStatus.getPath()));
        mAlluxioFileOpener = status -> new AlluxioHdfsInputStream(mHadoopFileOpener.open(status));

        mAlluxioConf = HadoopUtils.toAlluxioConf(conf);
        // Handle metrics
        Properties metricsProperties = new Properties();
        for (Map.Entry<String, String> entry : conf) {
            metricsProperties.setProperty(entry.getKey(), entry.getValue());
        }
        MetricsSystem.startSinksFromConfig(new MetricsConfig(metricsProperties));
        mCacheManager = CacheManager.Factory.get(mAlluxioConf);
        if (mCacheManager == null) {
            throw new IOException("CacheManager is null !");
        }
    }

    @Override
    public synchronized void initialize(URI name, Configuration conf) throws IOException {
        this.originalScheme = name.getScheme();
        // create internal FileSystem according to the scheme
        this.fs = createInternalFS(name, conf);
        this.statistics = (FileSystem.Statistics) ReflectionUtil.getFieldValue(this.fs, "statistics");
        LOG.error("======= before {} {} ", this.statistics.getScheme(), this.statistics.toString());
        super.initialize(name, conf);
        this.setConf(conf);
        LOG.error("======= after  {} {} ", this.statistics.getScheme(), this.statistics.toString());

        this.bufferSize = conf.getInt(CacheFileSystemConstants.PARAMS_KEY_IO_FILE_BUFFER_SIZE,
                CacheFileSystemConstants.PARAMS_KEY_IO_FILE_BUFFER_SIZE_DEFAULT_VALUE);
        // when scheme is jfs, use the cache by jfs itself
        this.useLocalCache = conf.getBoolean(CacheFileSystemConstants.PARAMS_KEY_USE_CACHE,
                CacheFileSystemConstants.PARAMS_KEY_USE_CACHE_DEFAULT_VALUE)
                && !this.originalScheme.equals(CacheFileSystemConstants.JUICEFS_SCHEME);
        // create LocalCacheFileSystem if needs
        if (this.isUseLocalCache()) {
            // Todo: Can set local cache dir here for the current executor
            this.createLocalCacheManager(this.getUri(), conf);
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
    public abstract boolean isUseLocalCacheForTargetExecs();

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        if (this.mCacheManager != null
                && this.isUseLocalCache() && this.isUseLocalCacheForTargetExecs()) {
            URIStatus status = HadoopUtils.toAlluxioUriStatus(this.getFileStatus(f));
            LOG.error("Use LocalCacheFileSystem to open file {} .", f);
            return new FSDataInputStream(new HdfsFileInputStream(
                    new LocalCacheFileInStream(status, mAlluxioFileOpener, mCacheManager, mAlluxioConf),
                    statistics));
        }
        LOG.error("Use original FileSystem to open file {} .", f);
        return super.open(f, bufferSize);
    }

    /**
     * Only for testing
     */
    public FSDataInputStream open(Path f, int bufferSize, boolean useLocalCacheForExec) throws IOException {
        if (this.mCacheManager != null
                && this.isUseLocalCache() && useLocalCacheForExec) {
            URIStatus status = HadoopUtils.toAlluxioUriStatus(this.getFileStatus(f));
            LOG.error("Use LocalCacheFileSystem to open file {} .", f);
            return new FSDataInputStream(new HdfsFileInputStream(
                    new LocalCacheFileInStream(status, mAlluxioFileOpener, mCacheManager, mAlluxioConf),
                    statistics));
        }
        LOG.error("Use original FileSystem to open file {} .", f);
        return super.open(f, bufferSize);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        long start = System.currentTimeMillis();
        FileStatus fileStatus = null;
        if (fileStatusMap.containsKey(f)) {
            fileStatus = fileStatusMap.get(f);
            LOG.error("Get file {} status from cache took: {}", f,
                    (System.currentTimeMillis() - start));
        } else {
            fileStatus = this.fs.getFileStatus(f);
            fileStatusMap.put(f, fileStatus);
            LOG.error("Get file {} status took: {}", f, (System.currentTimeMillis() - start));
        }
        return fileStatus;
    }

    public CacheManager getmCacheManager() {
        return mCacheManager;
    }

    public void setmCacheManager(CacheManager mCacheManager) {
        this.mCacheManager = mCacheManager;
    }

    public AlluxioConfiguration getmAlluxioConf() {
        return mAlluxioConf;
    }

    public void setmAlluxioConf(AlluxioConfiguration mAlluxioConf) {
        this.mAlluxioConf = mAlluxioConf;
    }

    public boolean isUseLocalCache() {
        return useLocalCache;
    }

    public void setUseLocalCache(boolean useLocalCache) {
        this.useLocalCache = useLocalCache;
    }
}
