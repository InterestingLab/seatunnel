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
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;

import org.apache.avro.Conversions;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@Slf4j
public class ParquetReadStrategy extends AbstractReadStrategy {
    private static final byte[] PARQUET_MAGIC =
            new byte[] {(byte) 'P', (byte) 'A', (byte) 'R', (byte) '1'};
    private static final long NANOS_PER_MILLISECOND = 1000000;
    private static final long MILLIS_PER_DAY = TimeUnit.DAYS.toMillis(1L);
    private static final long JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH = 2440588;
    private static final String PARQUET = "Parquet";

    private int[] indexes;

    private boolean whetherSplitFile = BaseSourceConfigOptions.WHETHER_SPLIT_FILE.defaultValue();

    @Override
    public void setPluginConfig(Config pluginConfig) {
        super.setPluginConfig(pluginConfig);
        if (pluginConfig.hasPath(BaseSourceConfigOptions.WHETHER_SPLIT_FILE.key())) {
            whetherSplitFile =
                    pluginConfig.getBoolean(BaseSourceConfigOptions.WHETHER_SPLIT_FILE.key());
        }
    }

    /**
     * parquet file can only split by file block now. <br>
     * user cannot customise the split count and size. <br>
     *
     * @param path file path
     * @return splits
     */
    @Override
    public Set<FileSourceSplit> getFileSourceSplits(String path) {
        Set<FileSourceSplit> fileSourceSplits = new HashSet<>();
        if (!whetherSplitFile) {
            fileSourceSplits.add(new FileSourceSplit(path));
            return fileSourceSplits;
        }
        ParquetMetadata metadata;
        try (ParquetFileReader reader =
                hadoopFileSystemProxy.doWithHadoopAuth(
                        ((configuration, userGroupInformation) -> {
                            HadoopInputFile hadoopInputFile =
                                    HadoopInputFile.fromPath(new Path(path), configuration);
                            return ParquetFileReader.open(hadoopInputFile);
                        }))) {
            metadata = reader.getFooter();
        } catch (IOException e) {
            String errorMsg =
                    String.format("Create parquet reader for this file [%s] failed", path);
            throw new FileConnectorException(
                    CommonErrorCodeDeprecated.READER_OPERATION_FAILED, errorMsg, e);
        }
        if (metadata == null || CollectionUtils.isEmpty(metadata.getBlocks())) {
            log.warn("cannot get meta or blocks for path:{}", path);
            fileSourceSplits.add(new FileSourceSplit(path));
            return fileSourceSplits;
        }

        long low = 0;
        long high = low;
        long splitCountAll = 0;
        for (int i = 0; i < metadata.getBlocks().size(); i++) {
            high = low + metadata.getBlocks().get(i).getCompressedSize();
            FileSourceSplit split = new FileSourceSplit(path, low, high);
            fileSourceSplits.add(split);
            low = high;
        }
        log.info("generate parquet split count:{} for this file:{}", splitCountAll, path);
        return fileSourceSplits;
    }

    /**
     * todo: <br>
     * In theory, batch column reading can improve reading performance.
     */
    @Override
    public void read(FileSourceSplit split, Collector<SeaTunnelRow> output)
            throws IOException, FileConnectorException {
        String path = split.getFilePath();
        String tableId = split.getTableId();
        if (split.getMinRowIndex() == null || split.getMaxRowIndex() == null) {
            log.debug(
                    "minRowIndex or maxRowIndex is null, use fileBaseRead. fileSourceSplit:{}",
                    split);
        }
        readInner(path, tableId, split.getMinRowIndex(), split.getMaxRowIndex(), output);
    }

    @Override
    public void read(String path, String tableId, Collector<SeaTunnelRow> output)
            throws FileConnectorException, IOException {
        readInner(path, tableId, null, null, output);
    }

    public GenericData getDataModel() {
        GenericData dataModel = new GenericData();
        dataModel.addLogicalTypeConversion(new Conversions.DecimalConversion());
        dataModel.addLogicalTypeConversion(new TimeConversions.DateConversion());
        dataModel.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        return dataModel;
    }

    private void readInner(
            String path,
            String tableId,
            Long minRowIndex,
            Long maxRowIndex,
            Collector<SeaTunnelRow> output)
            throws FileConnectorException, IOException {
        if (Boolean.FALSE.equals(checkFileType(path))) {
            String errorMsg =
                    String.format(
                            "This file [%s] is not a parquet file, please check the format of this file",
                            path);
            throw new FileConnectorException(FileConnectorErrorCode.FILE_TYPE_INVALID, errorMsg);
        }
        Path filePath = new Path(path);
        Map<String, String> partitionsMap = parsePartitionsByPath(path);
        HadoopInputFile hadoopInputFile =
                hadoopFileSystemProxy.doWithHadoopAuth(
                        (configuration, userGroupInformation) ->
                                HadoopInputFile.fromPath(filePath, configuration));
        int fieldsCount = seaTunnelRowType.getTotalFields();
        GenericRecord record;
        boolean splitFile = minRowIndex != null && maxRowIndex != null;
        try (ParquetReader<GenericData.Record> reader =
                splitFile
                        ? AvroParquetReader.<GenericData.Record>builder(hadoopInputFile)
                                .withDataModel(getDataModel())
                                .withFileRange(minRowIndex, maxRowIndex)
                                .build()
                        : AvroParquetReader.<GenericData.Record>builder(hadoopInputFile)
                                .withDataModel(getDataModel())
                                .build()) {
            while ((record = reader.read()) != null) {
                Object[] fields;
                if (isMergePartition) {
                    int index = fieldsCount;
                    fields = new Object[fieldsCount + partitionsMap.size()];
                    for (String value : partitionsMap.values()) {
                        fields[index++] = value;
                    }
                } else {
                    fields = new Object[fieldsCount];
                }
                for (int i = 0; i < fieldsCount; i++) {
                    Object data = record.get(indexes[i]);
                    fields[i] = resolveObject(data, seaTunnelRowType.getFieldType(i));
                }
                SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fields);
                seaTunnelRow.setTableId(tableId);
                output.collect(seaTunnelRow);
            }
        }
    }

    private Object resolveObject(Object field, SeaTunnelDataType<?> fieldType) {
        if (field == null) {
            return null;
        }
        switch (fieldType.getSqlType()) {
            case ARRAY:
                ArrayList<Object> origArray = new ArrayList<>();
                ((GenericData.Array<?>) field)
                        .iterator()
                        .forEachRemaining(
                                ele -> {
                                    if (ele instanceof Utf8) {
                                        origArray.add(ele.toString());
                                    } else {
                                        origArray.add(ele);
                                    }
                                });
                SeaTunnelDataType<?> elementType = ((ArrayType<?, ?>) fieldType).getElementType();
                switch (elementType.getSqlType()) {
                    case STRING:
                        return origArray.toArray(TYPE_ARRAY_STRING);
                    case BOOLEAN:
                        return origArray.toArray(TYPE_ARRAY_BOOLEAN);
                    case TINYINT:
                        return origArray.toArray(TYPE_ARRAY_BYTE);
                    case SMALLINT:
                        return origArray.toArray(TYPE_ARRAY_SHORT);
                    case INT:
                        return origArray.toArray(TYPE_ARRAY_INTEGER);
                    case BIGINT:
                        return origArray.toArray(TYPE_ARRAY_LONG);
                    case FLOAT:
                        return origArray.toArray(TYPE_ARRAY_FLOAT);
                    case DOUBLE:
                        return origArray.toArray(TYPE_ARRAY_DOUBLE);
                    default:
                        String errorMsg =
                                String.format(
                                        "SeaTunnel array type not support this type [%s] now",
                                        fieldType.getSqlType());
                        throw new FileConnectorException(
                                CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE, errorMsg);
                }
            case MAP:
                HashMap<Object, Object> dataMap = new HashMap<>();
                SeaTunnelDataType<?> keyType = ((MapType<?, ?>) fieldType).getKeyType();
                SeaTunnelDataType<?> valueType = ((MapType<?, ?>) fieldType).getValueType();
                HashMap<Object, Object> origDataMap = (HashMap<Object, Object>) field;
                origDataMap.forEach(
                        (key, value) ->
                                dataMap.put(
                                        resolveObject(key, keyType),
                                        resolveObject(value, valueType)));
                return dataMap;
            case BOOLEAN:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case DATE:
                return field;
            case STRING:
                return field.toString();
            case TINYINT:
                return Byte.parseByte(field.toString());
            case SMALLINT:
                return Short.parseShort(field.toString());
            case NULL:
                return null;
            case BYTES:
                ByteBuffer buffer = (ByteBuffer) field;
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes, 0, bytes.length);
                return bytes;
            case TIMESTAMP:
                if (field instanceof GenericData.Fixed) {
                    Binary binary =
                            Binary.fromConstantByteArray(((GenericData.Fixed) field).bytes());
                    NanoTime nanoTime = NanoTime.fromBinary(binary);
                    int julianDay = nanoTime.getJulianDay();
                    long nanosOfDay = nanoTime.getTimeOfDayNanos();
                    long timestamp =
                            (julianDay - JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH) * MILLIS_PER_DAY
                                    + nanosOfDay / NANOS_PER_MILLISECOND;
                    return new Timestamp(timestamp).toLocalDateTime();
                }
                Instant instant = Instant.ofEpochMilli((long) field);
                return LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
            case ROW:
                SeaTunnelRowType rowType = (SeaTunnelRowType) fieldType;
                Object[] objects = new Object[rowType.getTotalFields()];
                for (int i = 0; i < rowType.getTotalFields(); i++) {
                    SeaTunnelDataType<?> dataType = rowType.getFieldType(i);
                    objects[i] = resolveObject(((GenericRecord) field).get(i), dataType);
                }
                return new SeaTunnelRow(objects);
            default:
                // do nothing
                // never got in there
                throw new FileConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        "SeaTunnel not support this data type now");
        }
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(String path) throws FileConnectorException {
        return getSeaTunnelRowTypeInfo(TablePath.DEFAULT, path);
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(TablePath tablePath, String path)
            throws FileConnectorException {
        ParquetMetadata metadata;
        try (ParquetFileReader reader =
                hadoopFileSystemProxy.doWithHadoopAuth(
                        ((configuration, userGroupInformation) -> {
                            HadoopInputFile hadoopInputFile =
                                    HadoopInputFile.fromPath(new Path(path), configuration);
                            return ParquetFileReader.open(hadoopInputFile);
                        }))) {
            metadata = reader.getFooter();
        } catch (IOException e) {
            String errorMsg =
                    String.format("Create parquet reader for this file [%s] failed", path);
            throw new FileConnectorException(
                    CommonErrorCodeDeprecated.READER_OPERATION_FAILED, errorMsg, e);
        }
        FileMetaData fileMetaData = metadata.getFileMetaData();
        MessageType originalSchema = fileMetaData.getSchema();
        if (readColumns.isEmpty()) {
            for (int i = 0; i < originalSchema.getFieldCount(); i++) {
                readColumns.add(originalSchema.getFieldName(i));
            }
        }
        String[] fields = new String[readColumns.size()];
        SeaTunnelDataType<?>[] types = new SeaTunnelDataType[readColumns.size()];
        indexes = new int[readColumns.size()];
        buildColumnsWithErrorCheck(
                tablePath,
                IntStream.range(0, readColumns.size()).iterator(),
                i -> {
                    fields[i] = readColumns.get(i);
                    Type type = originalSchema.getType(fields[i]);
                    int fieldIndex = originalSchema.getFieldIndex(fields[i]);
                    indexes[i] = fieldIndex;
                    types[i] = parquetType2SeaTunnelType(type, fields[i]);
                });
        seaTunnelRowType = new SeaTunnelRowType(fields, types);
        seaTunnelRowTypeWithPartition = mergePartitionTypes(path, seaTunnelRowType);
        return getActualSeaTunnelRowTypeInfo();
    }

    private SeaTunnelDataType<?> parquetType2SeaTunnelType(Type type, String name) {
        if (type.isPrimitive()) {
            switch (type.asPrimitiveType().getPrimitiveTypeName()) {
                case INT32:
                    OriginalType originalType = type.asPrimitiveType().getOriginalType();
                    if (originalType == null) {
                        return BasicType.INT_TYPE;
                    }
                    switch (type.asPrimitiveType().getOriginalType()) {
                        case INT_8:
                            return BasicType.BYTE_TYPE;
                        case INT_16:
                            return BasicType.SHORT_TYPE;
                        case DATE:
                            return LocalTimeType.LOCAL_DATE_TYPE;
                        default:
                            throw CommonError.convertToSeaTunnelTypeError(
                                    PARQUET, type.toString(), name);
                    }
                case INT64:
                    if (type.asPrimitiveType().getOriginalType() == OriginalType.TIMESTAMP_MILLIS) {
                        return LocalTimeType.LOCAL_DATE_TIME_TYPE;
                    }
                    return BasicType.LONG_TYPE;
                case INT96:
                    return LocalTimeType.LOCAL_DATE_TIME_TYPE;
                case BINARY:
                    if (type.asPrimitiveType().getOriginalType() == null) {
                        return PrimitiveByteArrayType.INSTANCE;
                    }
                    return BasicType.STRING_TYPE;
                case FLOAT:
                    return BasicType.FLOAT_TYPE;
                case DOUBLE:
                    return BasicType.DOUBLE_TYPE;
                case BOOLEAN:
                    return BasicType.BOOLEAN_TYPE;
                case FIXED_LEN_BYTE_ARRAY:
                    if (type.getLogicalTypeAnnotation() == null) {
                        return LocalTimeType.LOCAL_DATE_TIME_TYPE;
                    }
                    String typeInfo =
                            type.getLogicalTypeAnnotation()
                                    .toString()
                                    .replaceAll(SqlType.DECIMAL.toString(), "")
                                    .replaceAll("\\(", "")
                                    .replaceAll("\\)", "");
                    String[] splits = typeInfo.split(",");
                    int precision = Integer.parseInt(splits[0]);
                    int scale = Integer.parseInt(splits[1]);
                    return new DecimalType(precision, scale);
                default:
                    throw CommonError.convertToSeaTunnelTypeError("Parquet", type.toString(), name);
            }
        } else {
            LogicalTypeAnnotation logicalTypeAnnotation =
                    type.asGroupType().getLogicalTypeAnnotation();
            if (logicalTypeAnnotation == null) {
                // struct type
                List<Type> fields = type.asGroupType().getFields();
                String[] fieldNames = new String[fields.size()];
                SeaTunnelDataType<?>[] seaTunnelDataTypes = new SeaTunnelDataType<?>[fields.size()];
                for (int i = 0; i < fields.size(); i++) {
                    Type fieldType = fields.get(i);
                    SeaTunnelDataType<?> seaTunnelDataType =
                            parquetType2SeaTunnelType(fields.get(i), name);
                    fieldNames[i] = fieldType.getName();
                    seaTunnelDataTypes[i] = seaTunnelDataType;
                }
                return new SeaTunnelRowType(fieldNames, seaTunnelDataTypes);
            } else {
                switch (logicalTypeAnnotation.toOriginalType()) {
                    case MAP:
                        GroupType groupType = type.asGroupType().getType(0).asGroupType();
                        SeaTunnelDataType<?> keyType =
                                parquetType2SeaTunnelType(groupType.getType(0), name);
                        SeaTunnelDataType<?> valueType =
                                parquetType2SeaTunnelType(groupType.getType(1), name);
                        return new MapType<>(keyType, valueType);
                    case LIST:
                        Type elementType;
                        try {
                            elementType = type.asGroupType().getType(0).asGroupType().getType(0);
                        } catch (Exception e) {
                            elementType = type.asGroupType().getType(0);
                        }
                        SeaTunnelDataType<?> fieldType =
                                parquetType2SeaTunnelType(elementType, name);
                        switch (fieldType.getSqlType()) {
                            case STRING:
                                return ArrayType.STRING_ARRAY_TYPE;
                            case BOOLEAN:
                                return ArrayType.BOOLEAN_ARRAY_TYPE;
                            case TINYINT:
                                return ArrayType.BYTE_ARRAY_TYPE;
                            case SMALLINT:
                                return ArrayType.SHORT_ARRAY_TYPE;
                            case INT:
                                return ArrayType.INT_ARRAY_TYPE;
                            case BIGINT:
                                return ArrayType.LONG_ARRAY_TYPE;
                            case FLOAT:
                                return ArrayType.FLOAT_ARRAY_TYPE;
                            case DOUBLE:
                                return ArrayType.DOUBLE_ARRAY_TYPE;
                            default:
                                throw CommonError.convertToSeaTunnelTypeError(
                                        PARQUET, type.toString(), name);
                        }
                    default:
                        throw CommonError.convertToSeaTunnelTypeError(
                                PARQUET, type.toString(), name);
                }
            }
        }
    }

    @Override
    boolean checkFileType(String path) {
        boolean checkResult;
        byte[] magic = new byte[PARQUET_MAGIC.length];
        try {
            FSDataInputStream in = hadoopFileSystemProxy.getInputStream(path);
            // try to get header information in a parquet file
            in.seek(0);
            in.readFully(magic);
            checkResult = Arrays.equals(magic, PARQUET_MAGIC);
            in.close();
            return checkResult;
        } catch (IOException e) {
            String errorMsg = String.format("Check parquet file [%s] failed", path);
            throw new FileConnectorException(FileConnectorErrorCode.FILE_TYPE_INVALID, errorMsg);
        }
    }
}
