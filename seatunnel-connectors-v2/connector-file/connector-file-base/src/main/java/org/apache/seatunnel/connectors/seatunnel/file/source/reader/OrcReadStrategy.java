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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
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
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.DecimalColumnVector;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.ListColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.MapColumnVector;
import org.apache.orc.storage.ql.exec.vector.StructColumnVector;
import org.apache.orc.storage.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.storage.ql.exec.vector.UnionColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.seatunnel.api.table.type.TypeUtil.canConvert;
import static org.apache.seatunnel.connectors.seatunnel.file.sink.writer.OrcWriteStrategy.buildFieldWithRowType;

@Slf4j
public class OrcReadStrategy extends AbstractReadStrategy {
    private static final long MIN_SIZE = 16 * 1024;

    private int batchReadSize = 1024;

    /** user can specified row count per split */
    private long rowCountPerSplitByUser = 0;

    private final long DEFAULT_ROW_COUNT = 100000;
    private final long DEFAULT_FILE_SIZE_PER_SPLIT = 1024 * 1024 * 30;
    private long fileSizePerSplitByUser = DEFAULT_FILE_SIZE_PER_SPLIT;
    private boolean whetherSplitFile = BaseSourceConfigOptions.WHETHER_SPLIT_FILE.defaultValue();

    @Override
    public void setPluginConfig(Config pluginConfig) {
        super.setPluginConfig(pluginConfig);
        if (pluginConfig.hasPath(BaseSourceConfigOptions.WHETHER_SPLIT_FILE.key())) {
            whetherSplitFile =
                    pluginConfig.getBoolean(BaseSourceConfigOptions.WHETHER_SPLIT_FILE.key());
        }
        if (pluginConfig.hasPath(BaseSourceConfigOptions.ROW_COUNT_PER_SPLIT.key())) {
            rowCountPerSplitByUser =
                    pluginConfig.getLong(BaseSourceConfigOptions.ROW_COUNT_PER_SPLIT.key());
        }
        if (pluginConfig.hasPath(BaseSourceConfigOptions.FILE_SIZE_PER_SPLIT.key())) {
            fileSizePerSplitByUser =
                    pluginConfig.getLong(BaseSourceConfigOptions.FILE_SIZE_PER_SPLIT.key());
        }
        if (pluginConfig.hasPath(BaseSourceConfigOptions.BATCH_READ_ROWS.key())) {
            batchReadSize = pluginConfig.getInt(BaseSourceConfigOptions.BATCH_READ_ROWS.key());
        }
    }

    /**
     * split a file into many splits: <br>
     * good: <br>
     * 1. lower memory occupy. split read end, the memory can recycle. <br>
     * 2. lower checkpoint ack delay <br>
     * 3. support fine-grained concurrency. <br>
     * bad: <br>
     * 1. cannot guarantee the order of the data. <br>
     *
     * @param path file path
     * @return FileSourceSplit set
     */
    @Override
    public Set<FileSourceSplit> getFileSourceSplits(String path) {
        if (Boolean.FALSE.equals(checkFileType(path))) {
            String errorMsg =
                    String.format(
                            "This file [%s] is not a orc file, please check the format of this file",
                            path);
            throw new FileConnectorException(FileConnectorErrorCode.FILE_TYPE_INVALID, errorMsg);
        }
        Set<FileSourceSplit> fileSourceSplits = new HashSet<>();
        if (!whetherSplitFile) {
            fileSourceSplits.add(new FileSourceSplit(path));
            return fileSourceSplits;
        }
        try (Reader reader =
                hadoopFileSystemProxy.doWithHadoopAuth(
                        ((configuration, userGroupInformation) -> {
                            OrcFile.ReaderOptions readerOptions =
                                    OrcFile.readerOptions(configuration);
                            return OrcFile.createReader(new Path(path), readerOptions);
                        }))) {
            log.info(
                    "path:{}, rowCountPerSplitByUser:{}, fileSizePerSplitByUser:{}, fileSize:{}, stripCount:{}, rowCount:{}",
                    path,
                    rowCountPerSplitByUser,
                    fileSizePerSplitByUser,
                    reader.getContentLength(),
                    reader.getStripes().size(),
                    reader.getNumberOfRows());
            long rowCountPerSplit = rowCountPerSplitByUser;
            if (rowCountPerSplit <= 0) {
                // auto split by file size
                long fileSize = reader.getContentLength();
                long rowCount = reader.getNumberOfRows();
                long splitCount =
                        fileSize
                                / (fileSizePerSplitByUser <= 0
                                        ? DEFAULT_FILE_SIZE_PER_SPLIT
                                        : fileSizePerSplitByUser);
                if (rowCount == 0 || splitCount == 0) {
                    log.warn("cannot get file size or row count for orc file:{}", path);
                    rowCountPerSplit = DEFAULT_ROW_COUNT;
                } else {
                    rowCountPerSplit = rowCount / splitCount;
                }
            }
            long low = 0;
            long high = low;
            int splitCountAll = 0;
            if (reader.getStripes() == null) {
                log.warn("cannot get stripes for orc file:{}", path);
                fileSourceSplits.add(new FileSourceSplit(path));
                return fileSourceSplits;
            }
            for (int i = 0; i < reader.getStripes().size(); i++) {
                StripeInformation stripe = reader.getStripes().get(i);
                long leftOverCount = stripe.getNumberOfRows();
                int splitCount4Strip = 0;
                long startRow = low;
                while (leftOverCount > 0) {
                    if (leftOverCount > rowCountPerSplit) {
                        high = low + rowCountPerSplit;
                    } else {
                        high = low + leftOverCount;
                    }
                    FileSourceSplit split = new FileSourceSplit(path, low, high - 1);
                    fileSourceSplits.add(split);
                    leftOverCount = leftOverCount - rowCountPerSplit;
                    low = high;
                    splitCountAll++;
                    splitCount4Strip++;
                }
                log.debug(
                        "generate split count:{} for this strip:{}, startRow:{}, endRow:{},",
                        splitCount4Strip,
                        i,
                        startRow,
                        high - 1);
            }
            log.info("generate split count:{} for this file:{}", splitCountAll, path);
            return fileSourceSplits;
        } catch (IOException e) {
            String errorMsg = String.format("Create orc reader for this file [%s] failed", path);
            throw new FileConnectorException(
                    CommonErrorCodeDeprecated.READER_OPERATION_FAILED, errorMsg);
        }
    }

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

    private TypeDescription getTypeDescription() {
        TypeDescription schema = TypeDescription.createStruct();
        for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
            TypeDescription typeDescription =
                    buildFieldWithRowType(seaTunnelRowType.getFieldType(i));
            schema.addField(seaTunnelRowType.getFieldName(i), typeDescription);
        }
        return schema;
    }

    private void readInner(
            String path, String tableId, Long min, Long max, Collector<SeaTunnelRow> output)
            throws IOException, FileConnectorException {
        if (Boolean.FALSE.equals(checkFileType(path))) {
            String errorMsg =
                    String.format(
                            "This file [%s] is not a orc file, please check the format of this file",
                            path);
            throw new FileConnectorException(FileConnectorErrorCode.FILE_TYPE_INVALID, errorMsg);
        }
        Map<String, String> partitionsMap = parsePartitionsByPath(path);
        try (Reader reader =
                hadoopFileSystemProxy.doWithHadoopAuth(
                        (configuration, userGroupInformation) -> {
                            OrcFile.ReaderOptions readerOptions =
                                    OrcFile.readerOptions(configuration);
                            return OrcFile.createReader(new Path(path), readerOptions);
                        })) {
            TypeDescription schema = getTypeDescription();

            List<TypeDescription> children = schema.getChildren();
            RecordReader rows = reader.rows(reader.options().schema(schema));
            long curRowCount = 0;
            if (min != null) {
                rows.seekToRow(min);
                curRowCount = min;
            }
            VectorizedRowBatch rowBatch = schema.createRowBatch(batchReadSize);
            while (rows.nextBatch(rowBatch)) {
                int num = 0;
                if (max != null && curRowCount > max) {
                    break;
                }
                for (int i = 0; i < rowBatch.size; i++) {
                    int numCols = rowBatch.numCols;
                    Object[] fields;
                    if (isMergePartition) {
                        int index = numCols;
                        fields = new Object[numCols + partitionsMap.size()];
                        for (String value : partitionsMap.values()) {
                            fields[index++] = value;
                        }
                    } else {
                        fields = new Object[numCols];
                    }
                    ColumnVector[] cols = rowBatch.cols;
                    for (int j = 0; j < numCols; j++) {
                        if (cols[j] == null) {
                            fields[j] = null;
                        } else {
                            fields[j] =
                                    readColumn(
                                            cols[j],
                                            children.get(j),
                                            seaTunnelRowType.getFieldType(j),
                                            num);
                        }
                    }
                    SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fields);
                    seaTunnelRow.setTableId(tableId);
                    output.collect(seaTunnelRow);
                    num++;
                    curRowCount++;
                    if (max != null && curRowCount > max) {
                        break;
                    }
                }
            }
            log.info(
                    "orc file read end. path:{}, fileSize:{}, totalRows:{}",
                    path,
                    reader.getContentLength(),
                    reader.getNumberOfRows());
        }
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(String path) throws FileConnectorException {
        return getSeaTunnelRowTypeInfoWithUserConfigRowType(path, null);
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfoWithUserConfigRowType(
            String path, SeaTunnelRowType configRowType) throws FileConnectorException {
        try (Reader reader =
                hadoopFileSystemProxy.doWithHadoopAuth(
                        ((configuration, userGroupInformation) -> {
                            OrcFile.ReaderOptions readerOptions =
                                    OrcFile.readerOptions(configuration);
                            return OrcFile.createReader(new Path(path), readerOptions);
                        }))) {
            TypeDescription schema = reader.getSchema();
            List<String> fieldNames = schema.getFieldNames();
            if (readColumns.isEmpty()) {
                readColumns.addAll(fieldNames);
            }
            String[] fields = new String[readColumns.size()];
            SeaTunnelDataType<?>[] types = new SeaTunnelDataType[readColumns.size()];
            for (int i = 0; i < readColumns.size(); i++) {
                fields[i] = readColumns.get(i);
                int index = fieldNames.indexOf(readColumns.get(i));
                if (index == -1) {
                    throw new FileConnectorException(
                            CommonErrorCodeDeprecated.TABLE_SCHEMA_GET_FAILED,
                            String.format(
                                    "Column [%s] does not exists in table schema [%s]",
                                    readColumns.get(i), String.join(",", fieldNames)));
                }
                types[i] =
                        orcDataType2SeaTunnelDataType(
                                schema.getChildren().get(index),
                                configRowType != null && configRowType.getTotalFields() > i
                                        ? configRowType.getFieldType(i)
                                        : null);
            }
            seaTunnelRowType = new SeaTunnelRowType(fields, types);
            seaTunnelRowTypeWithPartition = mergePartitionTypes(path, seaTunnelRowType);
            return getActualSeaTunnelRowTypeInfo();
        } catch (IOException e) {
            String errorMsg = String.format("Create orc reader for this file [%s] failed", path);
            throw new FileConnectorException(
                    CommonErrorCodeDeprecated.READER_OPERATION_FAILED, errorMsg);
        }
    }

    @Override
    boolean checkFileType(String path) {
        try {
            boolean checkResult;
            FSDataInputStream in = hadoopFileSystemProxy.getInputStream(path);
            // try to get Postscript in orc file
            long size = hadoopFileSystemProxy.getFileStatus(path).getLen();
            int readSize = (int) Math.min(size, MIN_SIZE);
            in.seek(size - readSize);
            ByteBuffer buffer = ByteBuffer.allocate(readSize);
            in.readFully(
                    buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
            int psLen = buffer.get(readSize - 1) & 0xff;
            int len = OrcFile.MAGIC.length();
            if (psLen < len + 1) {
                in.close();
                return false;
            }
            int offset = buffer.arrayOffset() + buffer.position() + buffer.limit() - 1 - len;
            byte[] array = buffer.array();
            if (Text.decode(array, offset, len).equals(OrcFile.MAGIC)) {
                checkResult = true;
            } else {
                // If it isn't there, this may be the 0.11.0 version of ORC.
                // Read the first 3 bytes of the file to check for the header
                in.seek(0);
                byte[] header = new byte[len];
                in.readFully(header, 0, len);
                // if it isn't there, this isn't an ORC file
                checkResult = Text.decode(header, 0, len).equals(OrcFile.MAGIC);
            }
            in.close();
            return checkResult;
        } catch (IOException e) {
            String errorMsg = String.format("Check orc file [%s] failed", path);
            throw new FileConnectorException(FileConnectorErrorCode.FILE_TYPE_INVALID, errorMsg, e);
        }
    }

    private SeaTunnelDataType<?> getFinalType(
            SeaTunnelDataType<?> fileType, SeaTunnelDataType<?> configType) {
        if (configType == null) {
            return fileType;
        }
        return canConvert(fileType, configType) ? configType : fileType;
    }

    private SeaTunnelDataType<?> orcDataType2SeaTunnelDataType(
            TypeDescription typeDescription, SeaTunnelDataType<?> configType) {
        switch (typeDescription.getCategory()) {
            case BOOLEAN:
                return getFinalType(BasicType.BOOLEAN_TYPE, configType);
            case INT:
                return getFinalType(BasicType.INT_TYPE, configType);
            case BYTE:
                return getFinalType(BasicType.BYTE_TYPE, configType);
            case SHORT:
                return getFinalType(BasicType.SHORT_TYPE, configType);
            case LONG:
                return getFinalType(BasicType.LONG_TYPE, configType);
            case FLOAT:
                return getFinalType(BasicType.FLOAT_TYPE, configType);
            case DOUBLE:
                return getFinalType(BasicType.DOUBLE_TYPE, configType);
            case BINARY:
                return getFinalType(PrimitiveByteArrayType.INSTANCE, configType);
            case STRING:
            case VARCHAR:
            case CHAR:
                return getFinalType(BasicType.STRING_TYPE, configType);
            case DATE:
                return getFinalType(LocalTimeType.LOCAL_DATE_TYPE, configType);
            case TIMESTAMP:
                // Support only return time when the type is timestamps
                if (configType != null && configType.getSqlType().equals(SqlType.TIME)) {
                    return LocalTimeType.LOCAL_TIME_TYPE;
                }
                return getFinalType(LocalTimeType.LOCAL_DATE_TIME_TYPE, configType);
            case DECIMAL:
                int precision = typeDescription.getPrecision();
                int scale = typeDescription.getScale();
                return getFinalType(new DecimalType(precision, scale), configType);
            case LIST:
                TypeDescription listType = typeDescription.getChildren().get(0);
                SeaTunnelDataType<?> seaTunnelDataType =
                        orcDataType2SeaTunnelDataType(listType, null);
                if (configType instanceof ArrayType) {
                    SeaTunnelDataType<?> elementType = ((ArrayType) configType).getElementType();
                    seaTunnelDataType = orcDataType2SeaTunnelDataType(listType, elementType);
                }
                switch (seaTunnelDataType.getSqlType()) {
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
                        String errorMsg =
                                String.format(
                                        "SeaTunnel array type not supported this genericType [%s] yet",
                                        seaTunnelDataType);
                        throw new FileConnectorException(
                                CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE, errorMsg);
                }
            case MAP:
                TypeDescription keyType = typeDescription.getChildren().get(0);
                TypeDescription valueType = typeDescription.getChildren().get(1);
                if (configType instanceof MapType) {
                    SeaTunnelDataType<?> keyDataType = ((MapType<?, ?>) configType).getKeyType();
                    SeaTunnelDataType<?> valueDataType =
                            ((MapType<?, ?>) configType).getValueType();
                    keyDataType = orcDataType2SeaTunnelDataType(keyType, keyDataType);
                    valueDataType = orcDataType2SeaTunnelDataType(valueType, valueDataType);
                    return new MapType<>(keyDataType, valueDataType);
                } else {
                    return new MapType<>(
                            orcDataType2SeaTunnelDataType(keyType, null),
                            orcDataType2SeaTunnelDataType(valueType, null));
                }
            case STRUCT:
                List<TypeDescription> children = typeDescription.getChildren();
                String[] fieldNames = typeDescription.getFieldNames().toArray(TYPE_ARRAY_STRING);
                SeaTunnelDataType<?>[] fieldTypes = new SeaTunnelDataType[children.size()];
                if (configType instanceof SeaTunnelRowType) {
                    for (int i = 0; i < children.size(); i++) {
                        fieldTypes[i] =
                                orcDataType2SeaTunnelDataType(
                                        children.get(i),
                                        ((SeaTunnelRowType) configType).getFieldType(i));
                    }
                } else {
                    fieldTypes =
                            children.stream()
                                    .map(f -> orcDataType2SeaTunnelDataType(f, null))
                                    .toArray(SeaTunnelDataType<?>[]::new);
                }
                return new SeaTunnelRowType(fieldNames, fieldTypes);
            default:
                // do nothing
                // never get in there
                String errorMsg =
                        String.format(
                                "SeaTunnel file connector not supported this orc type [%s] yet",
                                typeDescription.getCategory());
                throw new FileConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE, errorMsg);
        }
    }

    private Object readColumn(
            ColumnVector colVec,
            TypeDescription colType,
            @Nullable SeaTunnelDataType<?> dataType,
            int rowNum) {
        Object columnObj = null;
        if (!colVec.isNull[rowNum]) {
            switch (colVec.type) {
                case LONG:
                    columnObj = readLongVal(colVec, colType, dataType, rowNum);
                    break;
                case DOUBLE:
                    columnObj = ((DoubleColumnVector) colVec).vector[rowNum];
                    if (colType.getCategory() == TypeDescription.Category.FLOAT) {
                        columnObj = ((Double) columnObj).floatValue();
                    }
                    if (dataType != null && dataType.getSqlType().equals(SqlType.STRING)) {
                        columnObj = columnObj.toString();
                    }
                    break;
                case BYTES:
                    columnObj = readBytesVal(colVec, colType, dataType, rowNum);
                    break;
                case DECIMAL:
                    columnObj = readDecimalVal(colVec, dataType, rowNum);
                    break;
                case TIMESTAMP:
                    columnObj = readTimestampVal(colVec, colType, dataType, rowNum);
                    break;
                case STRUCT:
                    columnObj = readStructVal(colVec, colType, dataType, rowNum);
                    break;
                case LIST:
                    columnObj = readListVal(colVec, colType, rowNum);
                    break;
                case MAP:
                    columnObj = readMapVal(colVec, colType, rowNum);
                    break;
                case UNION:
                    columnObj = readUnionVal(colVec, colType, rowNum);
                    break;
                default:
                    throw new FileConnectorException(
                            CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                            "ReadColumn: unsupported ORC file column type: " + colVec.type.name());
            }
        }
        return columnObj;
    }

    private Object readLongVal(
            ColumnVector colVec,
            TypeDescription colType,
            SeaTunnelDataType<?> dataType,
            int rowNum) {
        Object colObj = null;
        if (!colVec.isNull[rowNum]) {
            LongColumnVector longVec = (LongColumnVector) colVec;
            long longVal = longVec.vector[rowNum];
            colObj = longVal;
            if (colType.getCategory() == TypeDescription.Category.INT) {
                colObj = (int) longVal;
            } else if (colType.getCategory() == TypeDescription.Category.BOOLEAN) {
                colObj = longVal == 1 ? Boolean.TRUE : Boolean.FALSE;
            } else if (colType.getCategory() == TypeDescription.Category.DATE) {
                colObj = LocalDate.ofEpochDay(longVal);
            } else if (colType.getCategory() == TypeDescription.Category.BYTE) {
                colObj = (byte) longVal;
            } else if (colType.getCategory() == TypeDescription.Category.SHORT) {
                colObj = (short) longVal;
            }
            if (dataType != null && dataType.getSqlType().equals(SqlType.STRING)) {
                colObj = colObj.toString();
            }
        }
        return colObj;
    }

    private Object readBytesVal(
            ColumnVector colVec,
            TypeDescription typeDescription,
            SeaTunnelDataType<?> dataType,
            int rowNum) {
        Charset charset = StandardCharsets.UTF_8;
        if (pluginConfig != null) {
            charset =
                    ReadonlyConfig.fromConfig(pluginConfig)
                            .getOptional(BaseSourceConfigOptions.ENCODING)
                            .map(Charset::forName)
                            .orElse(StandardCharsets.UTF_8);
        }

        Object bytesObj = null;
        if (!colVec.isNull[rowNum]) {
            BytesColumnVector bytesVector = (BytesColumnVector) colVec;
            bytesObj = this.bytesVectorToString(bytesVector, rowNum, charset);
            if (typeDescription.getCategory() == TypeDescription.Category.BINARY
                    && bytesObj != null) {
                bytesObj = ((String) bytesObj).getBytes(charset);
            }
            if (dataType != null
                    && dataType.getSqlType().equals(SqlType.STRING)
                    && bytesObj != null) {
                bytesObj = bytesObj.toString();
            }
        }
        return bytesObj;
    }

    /**
     * copied from {@link BytesColumnVector#toString(int)}
     *
     * @param bytesVector the BytesColumnVector
     * @param row rowNum
     * @param charset read charset
     */
    private Object bytesVectorToString(BytesColumnVector bytesVector, int row, Charset charset) {
        if (bytesVector.isRepeating) {
            row = 0;
        }

        return !bytesVector.noNulls && bytesVector.isNull[row]
                ? null
                : new String(
                        bytesVector.vector[row],
                        bytesVector.start[row],
                        bytesVector.length[row],
                        charset);
    }

    private Object readDecimalVal(ColumnVector colVec, SeaTunnelDataType<?> dataType, int rowNum) {
        Object decimalObj = null;
        if (!colVec.isNull[rowNum]) {
            DecimalColumnVector decimalVec = (DecimalColumnVector) colVec;
            decimalObj = decimalVec.vector[rowNum].getHiveDecimal().bigDecimalValue();
            if (dataType != null
                    && dataType.getSqlType().equals(SqlType.STRING)
                    && decimalObj != null) {
                decimalObj = decimalObj.toString();
            }
        }
        return decimalObj;
    }

    private Object readTimestampVal(
            ColumnVector colVec,
            TypeDescription colType,
            SeaTunnelDataType<?> dataType,
            int rowNum) {
        Object timestampVal = null;
        if (!colVec.isNull[rowNum]) {
            TimestampColumnVector timestampVec = (TimestampColumnVector) colVec;
            int nanos = timestampVec.nanos[rowNum];
            long millis = timestampVec.time[rowNum];
            Timestamp timestamp = new Timestamp(millis);
            timestamp.setNanos(nanos);
            timestampVal = timestamp.toLocalDateTime();
            if (colType.getCategory() == TypeDescription.Category.DATE) {
                timestampVal = LocalDate.ofEpochDay(timestamp.getTime());
            } else if (dataType != null && dataType.getSqlType() == SqlType.TIME) {
                timestampVal =
                        LocalTime.of(
                                ((LocalDateTime) timestampVal).getHour(),
                                ((LocalDateTime) timestampVal).getMinute(),
                                ((LocalDateTime) timestampVal).getSecond(),
                                ((LocalDateTime) timestampVal).getNano());
            }
            if (dataType != null
                    && dataType.getSqlType().equals(SqlType.STRING)
                    && timestampVal != null) {
                timestampVal = timestampVal.toString();
            }
        }
        return timestampVal;
    }

    private Object readStructVal(
            ColumnVector colVec,
            TypeDescription colType,
            SeaTunnelDataType<?> dataType,
            int rowNum) {
        Object structObj = null;
        if (!colVec.isNull[rowNum]) {
            StructColumnVector structVector = (StructColumnVector) colVec;
            ColumnVector[] fieldVec = structVector.fields;
            Object[] fieldValues = new Object[fieldVec.length];
            List<TypeDescription> fieldTypes = colType.getChildren();
            for (int i = 0; i < fieldVec.length; i++) {
                if (dataType instanceof SeaTunnelRowType) {
                    SeaTunnelDataType<?> fieldType = ((SeaTunnelRowType) dataType).getFieldType(i);
                    fieldValues[i] = readColumn(fieldVec[i], fieldTypes.get(i), fieldType, rowNum);
                } else {
                    fieldValues[i] = readColumn(fieldVec[i], fieldTypes.get(i), null, rowNum);
                }
            }
            structObj = new SeaTunnelRow(fieldValues);
        }
        return structObj;
    }

    private Object readMapVal(ColumnVector colVec, TypeDescription colType, int rowNum) {
        Map<Object, Object> objMap = new HashMap<>();
        MapColumnVector mapVector = (MapColumnVector) colVec;
        if (checkMapColumnVectorTypes(mapVector)) {
            int mapSize = (int) mapVector.lengths[rowNum];
            int offset = (int) mapVector.offsets[rowNum];
            List<TypeDescription> mapTypes = colType.getChildren();
            TypeDescription keyType = mapTypes.get(0);
            TypeDescription valueType = mapTypes.get(1);
            ColumnVector keyChild = mapVector.keys;
            ColumnVector valueChild = mapVector.values;
            Object[] keyList = readMapVector(keyChild, keyType, offset, mapSize);
            Object[] valueList = readMapVector(valueChild, valueType, offset, mapSize);
            for (int i = 0; i < keyList.length; i++) {
                objMap.put(keyList[i], valueList[i]);
            }
        } else {
            throw new FileConnectorException(
                    CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                    "readMapVal: unsupported key or value types");
        }
        return objMap;
    }

    private boolean checkMapColumnVectorTypes(MapColumnVector mapVector) {
        ColumnVector.Type keyType = mapVector.keys.type;
        ColumnVector.Type valueType = mapVector.values.type;
        return keyType == ColumnVector.Type.BYTES
                || keyType == ColumnVector.Type.LONG
                || keyType == ColumnVector.Type.DOUBLE && valueType == ColumnVector.Type.LONG
                || valueType == ColumnVector.Type.DOUBLE
                || valueType == ColumnVector.Type.BYTES
                || valueType == ColumnVector.Type.DECIMAL
                || valueType == ColumnVector.Type.TIMESTAMP;
    }

    private Object[] readMapVector(
            ColumnVector mapVector, TypeDescription childType, int offset, int numValues) {
        Object[] mapList;
        switch (mapVector.type) {
            case BYTES:
                mapList =
                        readBytesListVector(
                                (BytesColumnVector) mapVector, childType, offset, numValues);
                break;
            case LONG:
                mapList =
                        readLongListVector(
                                (LongColumnVector) mapVector, childType, offset, numValues);
                break;
            case DOUBLE:
                mapList =
                        readDoubleListVector(
                                (DoubleColumnVector) mapVector, childType, offset, numValues);
                break;
            case DECIMAL:
                mapList = readDecimalListVector((DecimalColumnVector) mapVector, offset, numValues);
                break;
            case TIMESTAMP:
                mapList =
                        readTimestampListVector(
                                (TimestampColumnVector) mapVector, childType, offset, numValues);
                break;
            default:
                throw new FileConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        mapVector.type.name() + " is not supported for MapColumnVectors");
        }
        return mapList;
    }

    private Object readUnionVal(ColumnVector colVec, TypeDescription colType, int rowNum) {
        Pair<TypeDescription, Object> columnValuePair;
        UnionColumnVector unionVector = (UnionColumnVector) colVec;
        int tagVal = unionVector.tags[rowNum];
        List<TypeDescription> unionFieldTypes = colType.getChildren();
        if (tagVal < unionFieldTypes.size()) {
            TypeDescription fieldType = unionFieldTypes.get(tagVal);
            if (tagVal < unionVector.fields.length) {
                ColumnVector fieldVector = unionVector.fields[tagVal];
                Object unionValue = readColumn(fieldVector, fieldType, null, rowNum);
                columnValuePair = Pair.of(fieldType, unionValue);
            } else {
                throw new FileConnectorException(
                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                        "readUnionVal: union tag value out of range for union column vectors");
            }
        } else {
            throw new FileConnectorException(
                    CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                    "readUnionVal: union tag value out of range for union types");
        }
        return columnValuePair;
    }

    private Object readListVal(ColumnVector colVec, TypeDescription colType, int rowNum) {
        Object listValues = null;
        if (!colVec.isNull[rowNum]) {
            ListColumnVector listVector = (ListColumnVector) colVec;
            ColumnVector listChildVector = listVector.child;
            TypeDescription childType = colType.getChildren().get(0);
            switch (listChildVector.type) {
                case LONG:
                    listValues = readLongListValues(listVector, childType, rowNum);
                    break;
                case DOUBLE:
                    listValues = readDoubleListValues(listVector, colType, rowNum);
                    break;
                case BYTES:
                    listValues = readBytesListValues(listVector, childType, rowNum);
                    break;
                case DECIMAL:
                    listValues = readDecimalListValues(listVector, rowNum);
                    break;
                case TIMESTAMP:
                    listValues = readTimestampListValues(listVector, childType, rowNum);
                    break;
                default:
                    throw new FileConnectorException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                            listVector.type.name() + " is not supported for ListColumnVectors");
            }
        }
        return listValues;
    }

    private Object readLongListValues(
            ListColumnVector listVector, TypeDescription childType, int rowNum) {
        int offset = (int) listVector.offsets[rowNum];
        int numValues = (int) listVector.lengths[rowNum];
        LongColumnVector longVector = (LongColumnVector) listVector.child;
        return readLongListVector(longVector, childType, offset, numValues);
    }

    private Object[] readLongListVector(
            LongColumnVector longVector, TypeDescription childType, int offset, int numValues) {
        List<Object> longList = new ArrayList<>();
        for (int i = 0; i < numValues; i++) {
            if (!longVector.isNull[offset + i]) {
                long longVal = longVector.vector[offset + i];
                if (childType.getCategory() == TypeDescription.Category.BOOLEAN) {
                    Boolean boolVal = longVal == 0 ? Boolean.valueOf(false) : Boolean.valueOf(true);
                    longList.add(boolVal);
                } else if (childType.getCategory() == TypeDescription.Category.INT) {
                    Integer intObj = (int) longVal;
                    longList.add(intObj);
                } else if (childType.getCategory() == TypeDescription.Category.BYTE) {
                    Byte byteObj = (byte) longVal;
                    longList.add(byteObj);
                } else if (childType.getCategory() == TypeDescription.Category.SHORT) {
                    Short shortObj = (short) longVal;
                    longList.add(shortObj);
                } else {
                    longList.add(longVal);
                }
            } else {
                longList.add(null);
            }
        }
        if (childType.getCategory() == TypeDescription.Category.BOOLEAN) {
            return longList.toArray(TYPE_ARRAY_BOOLEAN);
        } else if (childType.getCategory() == TypeDescription.Category.INT) {
            return longList.toArray(TYPE_ARRAY_INTEGER);
        } else if (childType.getCategory() == TypeDescription.Category.BYTE) {
            return longList.toArray(TYPE_ARRAY_BYTE);
        } else if (childType.getCategory() == TypeDescription.Category.SHORT) {
            return longList.toArray(TYPE_ARRAY_SHORT);
        } else {
            return longList.toArray(TYPE_ARRAY_LONG);
        }
    }

    private Object readDoubleListValues(
            ListColumnVector listVector, TypeDescription colType, int rowNum) {
        int offset = (int) listVector.offsets[rowNum];
        int numValues = (int) listVector.lengths[rowNum];
        DoubleColumnVector doubleVec = (DoubleColumnVector) listVector.child;
        return readDoubleListVector(doubleVec, colType, offset, numValues);
    }

    private Object[] readDoubleListVector(
            DoubleColumnVector doubleVec, TypeDescription colType, int offset, int numValues) {
        List<Object> doubleList = new ArrayList<>();
        for (int i = 0; i < numValues; i++) {
            if (!doubleVec.isNull[offset + i]) {
                Double doubleVal = doubleVec.vector[offset + i];
                if (colType.getCategory() == TypeDescription.Category.FLOAT) {
                    doubleList.add(doubleVal.floatValue());
                } else {
                    doubleList.add(doubleVal);
                }
            } else {
                doubleList.add(null);
            }
        }
        if (colType.getCategory() == TypeDescription.Category.FLOAT) {
            return doubleList.toArray(TYPE_ARRAY_FLOAT);
        } else {
            return doubleList.toArray(TYPE_ARRAY_DOUBLE);
        }
    }

    private Object readBytesListValues(
            ListColumnVector listVector, TypeDescription childType, int rowNum) {
        int offset = (int) listVector.offsets[rowNum];
        int numValues = (int) listVector.lengths[rowNum];
        BytesColumnVector bytesVec = (BytesColumnVector) listVector.child;
        return readBytesListVector(bytesVec, childType, offset, numValues);
    }

    private Object[] readBytesListVector(
            BytesColumnVector bytesVec, TypeDescription childType, int offset, int numValues) {
        List<Object> bytesValList = new ArrayList<>();
        for (int i = 0; i < numValues; i++) {
            if (!bytesVec.isNull[offset + i]) {
                byte[] byteArray = bytesVec.vector[offset + i];
                int vecLen = bytesVec.length[offset + i];
                int vecStart = bytesVec.start[offset + i];
                byte[] vecCopy = Arrays.copyOfRange(byteArray, vecStart, vecStart + vecLen);
                if (childType.getCategory() == TypeDescription.Category.STRING) {
                    String str = new String(vecCopy);
                    bytesValList.add(str);
                } else {
                    bytesValList.add(vecCopy);
                }
            } else {
                bytesValList.add(null);
            }
        }
        if (childType.getCategory() == TypeDescription.Category.STRING) {
            return bytesValList.toArray(TYPE_ARRAY_STRING);
        } else {
            return bytesValList.toArray();
        }
    }

    private Object readDecimalListValues(ListColumnVector listVector, int rowNum) {
        int offset = (int) listVector.offsets[rowNum];
        int numValues = (int) listVector.lengths[rowNum];
        DecimalColumnVector decimalVec = (DecimalColumnVector) listVector.child;
        return readDecimalListVector(decimalVec, offset, numValues);
    }

    private Object[] readDecimalListVector(
            DecimalColumnVector decimalVector, int offset, int numValues) {
        List<Object> decimalList = new ArrayList<>();
        for (int i = 0; i < numValues; i++) {
            if (!decimalVector.isNull[offset + i]) {
                BigDecimal bigDecimal = decimalVector.vector[i].getHiveDecimal().bigDecimalValue();
                decimalList.add(bigDecimal);
            } else {
                decimalList.add(null);
            }
        }
        return decimalList.toArray(TYPE_ARRAY_BIG_DECIMAL);
    }

    private Object readTimestampListValues(
            ListColumnVector listVector, TypeDescription childType, int rowNum) {
        int offset = (int) listVector.offsets[rowNum];
        int numValues = (int) listVector.lengths[rowNum];
        TimestampColumnVector timestampVec = (TimestampColumnVector) listVector.child;
        return readTimestampListVector(timestampVec, childType, offset, numValues);
    }

    private Object[] readTimestampListVector(
            TimestampColumnVector timestampVector,
            TypeDescription childType,
            int offset,
            int numValues) {
        List<Object> timestampList = new ArrayList<>();
        for (int i = 0; i < numValues; i++) {
            if (!timestampVector.isNull[offset + i]) {
                int nanos = timestampVector.nanos[offset + i];
                long millis = timestampVector.time[offset + i];
                Timestamp timestamp = new Timestamp(millis);
                timestamp.setNanos(nanos);
                if (childType.getCategory() == TypeDescription.Category.DATE) {
                    LocalDate localDate = LocalDate.ofEpochDay(timestamp.getTime());
                    timestampList.add(localDate);
                } else {
                    timestampList.add(timestamp.toLocalDateTime());
                }
            } else {
                timestampList.add(null);
            }
        }
        if (childType.getCategory() == TypeDescription.Category.DATE) {
            return timestampList.toArray(TYPE_ARRAY_LOCAL_DATE);
        } else {
            return timestampList.toArray(TYPE_ARRAY_LOCAL_DATETIME);
        }
    }
}
