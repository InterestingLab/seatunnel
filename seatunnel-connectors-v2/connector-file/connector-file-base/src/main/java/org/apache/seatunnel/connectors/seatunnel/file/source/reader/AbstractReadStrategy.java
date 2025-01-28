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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.ArchiveCompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipParameters;
import org.apache.hadoop.fs.FileStatus;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Slf4j
public abstract class AbstractReadStrategy implements ReadStrategy {
    protected static final String[] TYPE_ARRAY_STRING = new String[0];
    protected static final Boolean[] TYPE_ARRAY_BOOLEAN = new Boolean[0];
    protected static final Byte[] TYPE_ARRAY_BYTE = new Byte[0];
    protected static final Short[] TYPE_ARRAY_SHORT = new Short[0];
    protected static final Integer[] TYPE_ARRAY_INTEGER = new Integer[0];
    protected static final Long[] TYPE_ARRAY_LONG = new Long[0];
    protected static final Float[] TYPE_ARRAY_FLOAT = new Float[0];
    protected static final Double[] TYPE_ARRAY_DOUBLE = new Double[0];
    protected static final BigDecimal[] TYPE_ARRAY_BIG_DECIMAL = new BigDecimal[0];
    protected static final LocalDate[] TYPE_ARRAY_LOCAL_DATE = new LocalDate[0];
    protected static final LocalDateTime[] TYPE_ARRAY_LOCAL_DATETIME = new LocalDateTime[0];

    protected HadoopConf hadoopConf;
    protected SeaTunnelRowType seaTunnelRowType;
    protected SeaTunnelRowType seaTunnelRowTypeWithPartition;
    protected Config pluginConfig;
    protected List<String> fileNames = new ArrayList<>();
    protected List<String> readPartitions = new ArrayList<>();
    protected List<String> readColumns = new ArrayList<>();
    protected boolean isMergePartition = true;
    protected long skipHeaderNumber = BaseSourceConfigOptions.SKIP_HEADER_ROW_NUMBER.defaultValue();
    protected transient boolean isKerberosAuthorization = false;
    protected HadoopFileSystemProxy hadoopFileSystemProxy;
    protected ArchiveCompressFormat archiveCompressFormat =
            BaseSourceConfigOptions.ARCHIVE_COMPRESS_CODEC.defaultValue();

    protected Pattern pattern;

    @Override
    public void init(HadoopConf conf) {
        this.hadoopConf = conf;
        this.hadoopFileSystemProxy = new HadoopFileSystemProxy(hadoopConf);
    }

    @Override
    public void setCatalogTable(CatalogTable catalogTable) {
        this.seaTunnelRowType = catalogTable.getSeaTunnelRowType();
        this.seaTunnelRowTypeWithPartition =
                mergePartitionTypes(fileNames.get(0), catalogTable.getSeaTunnelRowType());
    }

    boolean checkFileType(String path) {
        return true;
    }

    public void read(FileSourceSplit split, Collector<SeaTunnelRow> output)
            throws IOException, FileConnectorException {
        read(split.getFilePath(), split.getTableId(), output);
    }

    public Set<FileSourceSplit> getFileSourceSplits(String path) {
        FileSourceSplit fileSourceSplit = new FileSourceSplit(path);
        Set<FileSourceSplit> set = new HashSet<>();
        set.add(fileSourceSplit);
        return set;
    }

    @Override
    public List<String> getFileNamesByPath(String path) throws IOException {
        ArrayList<String> fileNames = new ArrayList<>();
        FileStatus[] stats = hadoopFileSystemProxy.listStatus(path);
        for (FileStatus fileStatus : stats) {
            if (fileStatus.isDirectory()) {
                // skip hidden tmp directory, such as .hive-staging_hive
                if (!fileStatus.getPath().getName().startsWith(".")) {
                    fileNames.addAll(getFileNamesByPath(fileStatus.getPath().toString()));
                }
                continue;
            }
            if (fileStatus.isFile() && filterFileByPattern(fileStatus) && fileStatus.getLen() > 0) {
                // filter '_SUCCESS' file
                if (!fileStatus.getPath().getName().equals("_SUCCESS")
                        && !fileStatus.getPath().getName().startsWith(".")) {
                    String filePath = fileStatus.getPath().toString();
                    if (!readPartitions.isEmpty()) {
                        for (String readPartition : readPartitions) {
                            if (filePath.contains(readPartition)) {
                                fileNames.add(filePath);
                                this.fileNames.add(filePath);
                                break;
                            }
                        }
                    } else {
                        fileNames.add(filePath);
                        this.fileNames.add(filePath);
                    }
                }
            }
        }

        return fileNames;
    }

    @Override
    public void setPluginConfig(Config pluginConfig) {
        this.pluginConfig = pluginConfig;
        // Determine whether it is a compressed file
        if (pluginConfig.hasPath(BaseSourceConfigOptions.ARCHIVE_COMPRESS_CODEC.key())) {
            String archiveCompressCodec =
                    pluginConfig.getString(BaseSourceConfigOptions.ARCHIVE_COMPRESS_CODEC.key());
            archiveCompressFormat =
                    ArchiveCompressFormat.valueOf(archiveCompressCodec.toUpperCase());
        }
        if (pluginConfig.hasPath(BaseSourceConfigOptions.PARSE_PARTITION_FROM_PATH.key())) {
            isMergePartition =
                    pluginConfig.getBoolean(
                            BaseSourceConfigOptions.PARSE_PARTITION_FROM_PATH.key());
        }
        if (pluginConfig.hasPath(BaseSourceConfigOptions.SKIP_HEADER_ROW_NUMBER.key())) {
            skipHeaderNumber =
                    pluginConfig.getLong(BaseSourceConfigOptions.SKIP_HEADER_ROW_NUMBER.key());
        }
        if (pluginConfig.hasPath(BaseSourceConfigOptions.READ_PARTITIONS.key())) {
            readPartitions.addAll(
                    pluginConfig.getStringList(BaseSourceConfigOptions.READ_PARTITIONS.key()));
        }
        if (pluginConfig.hasPath(BaseSourceConfigOptions.READ_COLUMNS.key())) {
            readColumns.addAll(
                    pluginConfig.getStringList(BaseSourceConfigOptions.READ_COLUMNS.key()));
        }
        if (pluginConfig.hasPath(BaseSourceConfigOptions.FILE_FILTER_PATTERN.key())) {
            String filterPattern =
                    pluginConfig.getString(BaseSourceConfigOptions.FILE_FILTER_PATTERN.key());
            this.pattern = Pattern.compile(Matcher.quoteReplacement(filterPattern));
        }
    }

    @Override
    public SeaTunnelRowType getActualSeaTunnelRowTypeInfo() {
        return isMergePartition ? seaTunnelRowTypeWithPartition : seaTunnelRowType;
    }

    protected void resolveArchiveCompressedInputStream(
            String path,
            String tableId,
            Collector<SeaTunnelRow> output,
            Map<String, String> partitionsMap,
            FileFormat fileFormat)
            throws IOException {
        switch (archiveCompressFormat) {
            case ZIP:
                try (ZipInputStream zis =
                        new ZipInputStream(hadoopFileSystemProxy.getInputStream(path))) {
                    ZipEntry entry;
                    while ((entry = zis.getNextEntry()) != null) {
                        if (!entry.isDirectory() && checkFileType(entry.getName(), fileFormat)) {
                            readProcess(
                                    path,
                                    tableId,
                                    output,
                                    copyInputStream(zis),
                                    partitionsMap,
                                    entry.getName());
                        }
                        zis.closeEntry();
                    }
                }
                break;
            case TAR:
                try (TarArchiveInputStream tarInput =
                        new TarArchiveInputStream(hadoopFileSystemProxy.getInputStream(path))) {
                    TarArchiveEntry entry;
                    while ((entry = tarInput.getNextTarEntry()) != null) {
                        if (!entry.isDirectory() && checkFileType(entry.getName(), fileFormat)) {
                            readProcess(
                                    path,
                                    tableId,
                                    output,
                                    copyInputStream(tarInput),
                                    partitionsMap,
                                    entry.getName());
                        }
                    }
                }
                break;
            case TAR_GZ:
                try (GzipCompressorInputStream gzipIn =
                                new GzipCompressorInputStream(
                                        hadoopFileSystemProxy.getInputStream(path));
                        TarArchiveInputStream tarIn = new TarArchiveInputStream(gzipIn)) {

                    TarArchiveEntry entry;
                    while ((entry = tarIn.getNextTarEntry()) != null) {
                        if (!entry.isDirectory() && checkFileType(entry.getName(), fileFormat)) {
                            readProcess(
                                    path,
                                    tableId,
                                    output,
                                    copyInputStream(tarIn),
                                    partitionsMap,
                                    entry.getName());
                        }
                    }
                }
                break;
            case GZ:
                GzipCompressorInputStream gzipIn =
                        new GzipCompressorInputStream(hadoopFileSystemProxy.getInputStream(path));
                GzipParameters parameters = gzipIn.getMetaData();
                String fileName = parameters.getFilename();
                if (fileName == null) {
                    // remove file suffix
                    // eg: excel need full compressed name
                    if (fileFormat == FileFormat.EXCEL) {
                        if (path.endsWith(".gz")) {
                            fileName = path.substring(0, path.length() - 3);
                        } else {
                            throw new IllegalArgumentException(
                                    "Excel file must have a .gz extension. File: " + path);
                        }
                    } else {
                        fileName = path;
                    }
                }
                readProcess(
                        path, tableId, output, copyInputStream(gzipIn), partitionsMap, fileName);
                break;
            case NONE:
                readProcess(
                        path,
                        tableId,
                        output,
                        hadoopFileSystemProxy.getInputStream(path),
                        partitionsMap,
                        path);
                break;
            default:
                log.warn(
                        "The file does not support this archive compress type: {}",
                        archiveCompressFormat);
                readProcess(
                        path,
                        tableId,
                        output,
                        hadoopFileSystemProxy.getInputStream(path),
                        partitionsMap,
                        path);
        }
    }

    protected void readProcess(
            String path,
            String tableId,
            Collector<SeaTunnelRow> output,
            InputStream inputStream,
            Map<String, String> partitionsMap,
            String currentFileName)
            throws IOException {
        throw new UnsupportedOperationException(
                "The file does not support the compressed file reading");
    }

    protected Map<String, String> parsePartitionsByPath(String path) {
        LinkedHashMap<String, String> partitions = new LinkedHashMap<>();
        Arrays.stream(path.split("/", -1))
                .filter(split -> split.contains("="))
                .map(split -> split.split("=", -1))
                .forEach(kv -> partitions.put(kv[0], kv[1]));
        return partitions;
    }

    protected SeaTunnelRowType mergePartitionTypes(String path, SeaTunnelRowType seaTunnelRowType) {
        Map<String, String> partitionsMap = parsePartitionsByPath(path);
        if (partitionsMap.isEmpty()) {
            return seaTunnelRowType;
        }
        // get all names of partitions fields
        String[] partitionNames = partitionsMap.keySet().toArray(TYPE_ARRAY_STRING);
        // initialize data type for partition fields
        SeaTunnelDataType<?>[] partitionTypes = new SeaTunnelDataType<?>[partitionNames.length];
        Arrays.fill(partitionTypes, BasicType.STRING_TYPE);
        // get origin field names
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        // get origin data types
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        // create new array to merge partition fields and origin fields
        String[] newFieldNames = new String[fieldNames.length + partitionNames.length];
        // create new array to merge partition fields' data type and origin fields' data type
        SeaTunnelDataType<?>[] newFieldTypes =
                new SeaTunnelDataType<?>[fieldTypes.length + partitionTypes.length];
        // copy origin field names to new array
        System.arraycopy(fieldNames, 0, newFieldNames, 0, fieldNames.length);
        // copy partitions field name to new array
        System.arraycopy(
                partitionNames, 0, newFieldNames, fieldNames.length, partitionNames.length);
        // copy origin field types to new array
        System.arraycopy(fieldTypes, 0, newFieldTypes, 0, fieldTypes.length);
        // copy partition field types to new array
        System.arraycopy(
                partitionTypes, 0, newFieldTypes, fieldTypes.length, partitionTypes.length);
        // return merge row type
        return new SeaTunnelRowType(newFieldNames, newFieldTypes);
    }

    protected boolean filterFileByPattern(FileStatus fileStatus) {
        if (Objects.nonNull(pattern)) {
            return pattern.matcher(fileStatus.getPath().getName()).matches();
        }
        return true;
    }

    protected static InputStream copyInputStream(InputStream inputStream) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int bytesRead;

        while ((bytesRead = inputStream.read(buffer)) != -1) {
            byteArrayOutputStream.write(buffer, 0, bytesRead);
        }

        return new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
    }

    protected boolean checkFileType(String fileName, FileFormat fileFormat) {
        for (String suffix : fileFormat.getAllSuffix()) {
            if (fileName.endsWith(suffix)) {
                return true;
            }
        }

        log.warn(
                "The {} file format is incorrect. Please check the format in the compressed file.",
                fileName);
        return false;
    }

    @Override
    public void close() throws IOException {
        try {
            if (hadoopFileSystemProxy != null) {
                hadoopFileSystemProxy.close();
            }
        } catch (Exception ignore) {
        }
    }
}
