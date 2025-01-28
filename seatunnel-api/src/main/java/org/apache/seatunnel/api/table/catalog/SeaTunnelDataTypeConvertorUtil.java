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

package org.apache.seatunnel.api.table.catalog;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigObject;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.common.exception.CommonError;

public class SeaTunnelDataTypeConvertorUtil {

    /**
     * @param columnType column type, should be {@link SeaTunnelDataType##toString}.
     * @return {@link SeaTunnelDataType} instance.
     */
    public static SeaTunnelDataType<?> deserializeSeaTunnelDataType(
            String field, String columnType) {
        SqlType sqlType = null;
        try {
            String compatible = compatibleTypeDeclare(columnType);
            sqlType = SqlType.valueOf(compatible.toUpperCase().replace(" ", ""));
        } catch (IllegalArgumentException e) {
            // nothing
        }
        if (sqlType == null) {
            return parseComplexDataType(field, columnType);
        }
        switch (sqlType) {
            case STRING:
                return BasicType.STRING_TYPE;
            case BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case TINYINT:
                return BasicType.BYTE_TYPE;
            case BYTES:
                return PrimitiveByteArrayType.INSTANCE;
            case SMALLINT:
                return BasicType.SHORT_TYPE;
            case INT:
                return BasicType.INT_TYPE;
            case BIGINT:
                return BasicType.LONG_TYPE;
            case FLOAT:
                return BasicType.FLOAT_TYPE;
            case DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case NULL:
                return BasicType.VOID_TYPE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case TIMESTAMP_TZ:
                return LocalTimeType.OFFSET_DATE_TIME_TYPE;
            case MAP:
                return parseMapType(field, columnType);
            case BINARY_VECTOR:
                return VectorType.VECTOR_BINARY_TYPE;
            case FLOAT_VECTOR:
                return VectorType.VECTOR_FLOAT_TYPE;
            case FLOAT16_VECTOR:
                return VectorType.VECTOR_FLOAT16_TYPE;
            case BFLOAT16_VECTOR:
                return VectorType.VECTOR_BFLOAT16_TYPE;
            case SPARSE_FLOAT_VECTOR:
                return VectorType.VECTOR_SPARSE_FLOAT_TYPE;
            default:
                throw CommonError.unsupportedDataType("SeaTunnel", columnType, field);
        }
    }

    /**
     * User-facing data type declarations will adhere to the specifications outlined in
     * schema-feature.md. To maintain backward compatibility, this function will transform type
     * declarations into standard form, including: <code>long -> bigint</code>, <code>
     * short -> smallint</code>, and <code>byte -> tinyint</code>.
     *
     * <p>In a future version, user-facing data type declarations will strictly follow the
     * specifications, and this function will be removed.
     *
     * @param declare
     * @return compatible type
     */
    @Deprecated
    private static String compatibleTypeDeclare(String declare) {
        switch (declare.trim().toUpperCase()) {
            case "LONG":
                return "BIGINT";
            case "SHORT":
                return "SMALLINT";
            case "BYTE":
                return "TINYINT";
            default:
                return declare;
        }
    }

    private static SeaTunnelDataType<?> parseComplexDataType(String field, String columnStr) {
        String column = columnStr.toUpperCase().replace(" ", "");
        if (column.startsWith(SqlType.MAP.name())) {
            return parseMapType(field, columnStr);
        }
        if (column.startsWith(SqlType.ARRAY.name())) {
            return parseArrayType(field, columnStr);
        }
        if (column.startsWith(SqlType.DECIMAL.name())) {
            return parseDecimalType(columnStr);
        }
        if (column.trim().startsWith("{")) {
            return parseRowType(columnStr);
        }
        throw CommonError.unsupportedDataType("SeaTunnel", columnStr, field);
    }

    private static SeaTunnelDataType<?> parseRowType(String columnStr) {
        String confPayload = "{conf = " + columnStr + "}";
        Config conf;
        try {
            conf = ConfigFactory.parseString(confPayload);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                    String.format("HOCON Config parse from %s failed.", confPayload), e);
        }
        return parseRowType(conf.getObject("conf"));
    }

    private static SeaTunnelDataType<?> parseRowType(ConfigObject conf) {
        String[] fieldNames = new String[conf.size()];
        SeaTunnelDataType<?>[] fieldTypes = new SeaTunnelDataType[conf.size()];
        conf.keySet().toArray(fieldNames);

        for (int idx = 0; idx < fieldNames.length; idx++) {
            String fieldName = fieldNames[idx];
            ConfigValue typeVal = conf.get(fieldName);
            switch (typeVal.valueType()) {
                case STRING:
                    {
                        fieldTypes[idx] =
                                deserializeSeaTunnelDataType(
                                        fieldNames[idx], (String) typeVal.unwrapped());
                    }
                    break;
                case OBJECT:
                    {
                        fieldTypes[idx] = parseRowType((ConfigObject) typeVal);
                    }
                    break;
                case LIST:
                case NUMBER:
                case BOOLEAN:
                case NULL:
                default:
                    throw new IllegalArgumentException(
                            String.format(
                                    "Unsupported parse SeaTunnel Type from '%s'.",
                                    typeVal.unwrapped()));
            }
        }
        return new SeaTunnelRowType(fieldNames, fieldTypes);
    }

    private static SeaTunnelDataType<?> parseMapType(String field, String columnStr) {
        String genericType = getGenericType(columnStr).trim();
        int index =
                genericType.toUpperCase().startsWith(SqlType.DECIMAL.name())
                        ?
                        // if map key is decimal, we should find the index of second ','
                        genericType.indexOf(",", genericType.indexOf(",") + 1)
                        :
                        // if map key is not decimal, we should find the index of first ','
                        genericType.indexOf(",");
        String keyGenericType = genericType.substring(0, index).trim();
        String valueGenericType = genericType.substring(index + 1).trim();
        return new MapType<>(
                deserializeSeaTunnelDataType(field, keyGenericType),
                deserializeSeaTunnelDataType(field, valueGenericType));
    }

    private static String getGenericType(String columnStr) {
        // get the content between '<' and '>'
        return columnStr.substring(columnStr.indexOf("<") + 1, columnStr.lastIndexOf(">"));
    }

    private static SeaTunnelDataType<?> parseArrayType(String field, String columnStr) {
        String genericType = getGenericType(columnStr).trim();
        SeaTunnelDataType<?> dataType = deserializeSeaTunnelDataType(field, genericType);
        switch (dataType.getSqlType()) {
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
            case MAP:
                return new ArrayType<>(
                        MapType.class, new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE));
            default:
                throw CommonError.unsupportedDataType("SeaTunnel", genericType, field);
        }
    }

    private static SeaTunnelDataType<?> parseDecimalType(String columnStr) {
        String[] decimalInfos = columnStr.split(",");
        if (decimalInfos.length < 2) {
            throw new RuntimeException(
                    "Decimal type should assign precision and scale information");
        }
        int precision = Integer.parseInt(decimalInfos[0].replaceAll("\\D", ""));
        int scale = Integer.parseInt(decimalInfos[1].replaceAll("\\D", ""));
        return new DecimalType(precision, scale);
    }
}
