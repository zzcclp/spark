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
package test.org.apache.kylin.cache.fs;

import alluxio.client.file.cache.CacheManager;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.cache.KylinCacheConstants;
import org.apache.kylin.cache.fs.CacheFileSystemConstants;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.cache.fs.kylin.KylinCacheFileSystem;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public class TestKylinCacheFileSystem {

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    private void readFile(KylinCacheFileSystem kylinCacheFileSystem, FileStatus fileStatus,
                          boolean isLocalCache) throws IOException {
        FSDataInputStream testInputStream =
                kylinCacheFileSystem.open(fileStatus.getPath(), 65536, isLocalCache);
        int readLen = 3 * 1024 * 1024;
        byte[] buf = new byte[readLen];
        testInputStream.read(buf, 0, readLen);
    }

    @Test
    public void testLocalCacheFileSystem() throws Exception {
        String cacheDir = "/tmp/local-cache-test";
        File cacheFiles = new File(cacheDir + "/LOCAL/1048576");
        if (cacheFiles.exists()) {
            FileUtils.deleteDirectory(cacheFiles);
        }
        String currPath =
                this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        Configuration conf = new Configuration();
        conf.set("alluxio.user.client.cache.dir", cacheDir);
        conf.set(CacheFileSystemConstants.PARAMS_KEY_USE_CACHE, "true");
        Path path = new Path(
        //        new URI("hdfs://mydocker:9000/raptorx_stress_data/"));
                new URI("file://" + currPath + "/cache-data-test/"));
        FileSystem fs = path.getFileSystem(conf);
        Assert.assertTrue(fs instanceof KylinCacheFileSystem);
        KylinCacheFileSystem kylinCacheFileSystem = (KylinCacheFileSystem) fs;
        Assert.assertEquals(kylinCacheFileSystem.getRawFileSystem().getUri(),
                kylinCacheFileSystem.getUri());

        Assert.assertEquals(kylinCacheFileSystem.getRawFileSystem().getScheme(),
                kylinCacheFileSystem.getScheme());

        FileStatus[] fileStatuses = fs.listStatus(path);
        Assert.assertEquals(4, fileStatuses.length);

        readFile(kylinCacheFileSystem, fileStatuses[0], true);
        readFile(kylinCacheFileSystem, fileStatuses[1], false);
        readFile(kylinCacheFileSystem, fileStatuses[2], false);
        readFile(kylinCacheFileSystem, fileStatuses[3], true);
        readFile(kylinCacheFileSystem, fileStatuses[3], true);
        readFile(kylinCacheFileSystem, fileStatuses[0], true);
        readFile(kylinCacheFileSystem, fileStatuses[1], false);
        kylinCacheFileSystem.close();

        Assert.assertTrue(cacheFiles.isDirectory() && cacheFiles.exists());
        String[] files = cacheFiles.list();
        Assert.assertEquals(2, files.length);
        FileUtils.deleteDirectory(cacheFiles);
    }
}
