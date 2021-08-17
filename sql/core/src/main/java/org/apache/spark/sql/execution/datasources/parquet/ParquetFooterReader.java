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

package org.apache.spark.sql.execution.datasources.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.apache.parquet.bytes.BytesUtils.readIntLittleEndian;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.ParquetFileWriter.MAGIC;

public class ParquetFooterReader {

    private final static Logger logger = LoggerFactory.getLogger(ParquetFooterReader.class);

    /**
     * Reads the meta data block in the footer of the file
     * @param configuration a configuration
     * @param file the parquet File
     * @return the metadata blocks in the footer
     * @throws IOException if an error occurs while reading the file
     * @deprecated will be removed in 2.0.0;
     *             use {@link ParquetFileReader#open(InputFile, ParquetReadOptions)}
     */
    @Deprecated
    public static final ParquetMetadata readFooter(Configuration configuration, Path file) throws IOException {
        return readFooter(configuration, file, NO_FILTER);
    }

    /**
     * Reads the meta data in the footer of the file.
     * Skipping row groups (or not) based on the provided filter
     * @param configuration a configuration
     * @param file the Parquet File
     * @param filter the filter to apply to row groups
     * @return the metadata with row groups filtered.
     * @throws IOException  if an error occurs while reading the file
     * @deprecated will be removed in 2.0.0;
     *             use {@link ParquetFileReader#open(InputFile, ParquetReadOptions)}
     */
    public static ParquetMetadata readFooter(Configuration configuration, Path file, ParquetMetadataConverter.MetadataFilter filter) throws IOException {
        return readFooter(HadoopInputFile.fromPath(file, configuration), filter);
    }

    /**
     * @param configuration a configuration
     * @param file the Parquet File
     * @return the metadata with row groups.
     * @throws IOException  if an error occurs while reading the file
     * @deprecated will be removed in 2.0.0;
     *             use {@link ParquetFileReader#open(InputFile, ParquetReadOptions)}
     */
    @Deprecated
    public static final ParquetMetadata readFooter(Configuration configuration, FileStatus file) throws IOException {
        return readFooter(configuration, file, NO_FILTER);
    }

    /**
     * Reads the meta data block in the footer of the file
     * @param configuration a configuration
     * @param file the parquet File
     * @param filter the filter to apply to row groups
     * @return the metadata blocks in the footer
     * @throws IOException if an error occurs while reading the file
     * @deprecated will be removed in 2.0.0;
     *             use {@link ParquetFileReader#open(InputFile, ParquetReadOptions)}
     */
    @Deprecated
    public static final ParquetMetadata readFooter(Configuration configuration, FileStatus file, ParquetMetadataConverter.MetadataFilter filter) throws IOException {
        return readFooter(HadoopInputFile.fromStatus(file, configuration), filter);
    }

    /**
     * Reads the meta data block in the footer of the file using provided input stream
     * @param file a {@link InputFile} to read
     * @param filter the filter to apply to row groups
     * @return the metadata blocks in the footer
     * @throws IOException if an error occurs while reading the file
     * @deprecated will be removed in 2.0.0;
     *             use {@link ParquetFileReader#open(InputFile, ParquetReadOptions)}
     */
    @Deprecated
    public static final ParquetMetadata readFooter(InputFile file, ParquetMetadataConverter.MetadataFilter filter) throws IOException {
        ParquetReadOptions options;
        if (file instanceof HadoopInputFile) {
            options = HadoopReadOptions.builder(((HadoopInputFile) file).getConfiguration())
                    .withMetadataFilter(filter).build();
        } else {
            options = ParquetReadOptions.builder().withMetadataFilter(filter).build();
        }

        try (SeekableInputStream in = file.newStream()) {
            return readFooter(file, options, in);
        }
    }

    private static final ParquetMetadata readFooter(InputFile file, ParquetReadOptions options, SeekableInputStream f) throws IOException {
        ParquetMetadataConverter converter = new ParquetMetadataConverter(options);
        return readFooter(file, options, f, converter);
    }

    private static final ParquetMetadata readFooter(InputFile file, ParquetReadOptions options, SeekableInputStream f, ParquetMetadataConverter converter) throws IOException {
        long fileLen = file.getLength();
        String filePath = file.toString();
        int FOOTER_LENGTH_SIZE = 4;
        if (fileLen < MAGIC.length + FOOTER_LENGTH_SIZE + MAGIC.length) { // MAGIC + data + footer + footerIndex + MAGIC
            throw new RuntimeException(filePath + " is not a Parquet file (too small length: " + fileLen + ")");
        }
        long footerLengthIndex = fileLen - FOOTER_LENGTH_SIZE - MAGIC.length;

        f.seek(footerLengthIndex);
        int footerLength = readIntLittleEndian(f);
        byte[] magic = new byte[MAGIC.length];
        f.readFully(magic);
        if (!Arrays.equals(MAGIC, magic)) {
            throw new RuntimeException(filePath + " is not a Parquet file. expected magic number at tail " + Arrays.toString(MAGIC) + " but found " + Arrays.toString(magic));
        }
        long footerIndex = footerLengthIndex - footerLength;
        if (footerIndex < MAGIC.length || footerIndex >= footerLengthIndex) {
            throw new RuntimeException("corrupted file: the footer index is not within the file: " + footerIndex);
        }
        f.seek(footerIndex);
        // Read all the footer bytes in one time to avoid multiple read operations,
        // since it can be pretty time consuming for a single read operation in HDFS.
        ByteBuffer footerBytesBuffer = ByteBuffer.allocate(footerLength);
        f.readFully(footerBytesBuffer);
        footerBytesBuffer.flip();
        InputStream footerBytesStream = ByteBufferInputStream.wrap(footerBytesBuffer);
        // return converter.readParquetMetadata(f, options.getMetadataFilter());
        return converter.readParquetMetadata(footerBytesStream, options.getMetadataFilter());
    }
}