/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.format.protobuf;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.format.protobuf.exception.ProtobufFormatErrorCode;
import org.apache.seatunnel.format.protobuf.exception.SeaTunnelProtobufFormatException;

import com.github.os72.protocjar.Protoc;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProtobufToRowConverter implements Serializable {
    private static final long serialVersionUID = 8177020083886379563L;

    private Descriptors.Descriptor descriptor = null;
    private String protoContent;
    private String messageName;

    public ProtobufToRowConverter(String protoContent, String messageName) {
        this.protoContent = protoContent;
        this.messageName = messageName;
    }

    public Descriptors.Descriptor getDescriptor() {
        if (descriptor == null) {
            try {
                descriptor = createDescriptor();
            } catch (IOException
                    | Descriptors.DescriptorValidationException
                    | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return descriptor;
    }

    private Descriptors.Descriptor createDescriptor()
            throws IOException, InterruptedException, Descriptors.DescriptorValidationException {

        String targetDesc = compileDescriptorTempFile();

        FileInputStream fis = new FileInputStream(targetDesc);
        DescriptorProtos.FileDescriptorSet descriptorSet =
                DescriptorProtos.FileDescriptorSet.parseFrom(fis);

        List<DescriptorProtos.FileDescriptorProto> fileDescriptors = descriptorSet.getFileList();
        Descriptors.FileDescriptor[] descriptorsArray =
                new Descriptors.FileDescriptor[fileDescriptors.size()];
        for (int i = 0; i < fileDescriptors.size(); i++) {
            descriptorsArray[i] =
                    Descriptors.FileDescriptor.buildFrom(
                            fileDescriptors.get(i), new Descriptors.FileDescriptor[] {});
        }

        Descriptors.Descriptor personDescriptor =
                descriptorsArray[0].findMessageTypeByName(this.messageName);

        return personDescriptor;
    }

    private String compileDescriptorTempFile() throws IOException, InterruptedException {
        // Because Protobuf can only be dynamically parsed through the descriptor file, the file
        // needs to be compiled and generated. The following method is used here to solve the
        // problem: generate a temporary directory and compile .proto into a descriptor temporary
        // file. The temporary file and directory are deleted after the JVM runs.
        File tmpDir = File.createTempFile("tmp_protobuf_", "_proto");
        tmpDir.delete();
        tmpDir.mkdirs();
        tmpDir.deleteOnExit();
        File protoFile = new File(tmpDir, ".proto");
        protoFile.deleteOnExit();
        FileUtils.writeStringToFile(protoFile.getPath(), protoContent);
        String targetDesc = tmpDir + "/.desc";
        new File(targetDesc).deleteOnExit();

        int exitCode =
                Protoc.runProtoc(
                        new String[] {
                            "--proto_path=" + protoFile.getParent(),
                            "--descriptor_set_out=" + targetDesc,
                            protoFile.getPath()
                        });

        if (exitCode != 0) {
            throw new SeaTunnelProtobufFormatException(
                    ProtobufFormatErrorCode.DESCRIPTOR_CONVERT_FAILED,
                    "Protoc compile error, exit code: " + exitCode);
        }
        return targetDesc;
    }

    public SeaTunnelRow converter(
            Descriptors.Descriptor descriptor,
            DynamicMessage dynamicMessage,
            SeaTunnelRowType rowType) {
        String[] fieldNames = rowType.getFieldNames();
        Object[] values = new Object[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            Descriptors.FieldDescriptor fieldByName = descriptor.findFieldByName(fieldNames[i]);
            if (fieldByName == null) {
                Descriptors.Descriptor nestedTypeByName =
                        descriptor.findNestedTypeByName(fieldNames[i]);
                if (nestedTypeByName != null) {
                    values[i] =
                            convertField(
                                    descriptor,
                                    dynamicMessage,
                                    rowType.getFieldType(i),
                                    null,
                                    fieldNames[i]);
                } else {
                    values[i] = null;
                }
                continue;
            }
            values[i] =
                    convertField(
                            descriptor,
                            dynamicMessage,
                            rowType.getFieldType(i),
                            dynamicMessage.getField(fieldByName),
                            fieldNames[i]);
        }
        return new SeaTunnelRow(values);
    }

    private Object convertField(
            Descriptors.Descriptor descriptor,
            DynamicMessage dynamicMessage,
            SeaTunnelDataType<?> dataType,
            Object val,
            String fieldName) {
        switch (dataType.getSqlType()) {
            case STRING:
                return val.toString();
            case BOOLEAN:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case NULL:
            case DATE:
            case DECIMAL:
            case TIMESTAMP:
                return val;
            case BYTES:
                return ((ByteBuffer) val).array();
            case SMALLINT:
                return ((Integer) val).shortValue();
            case TINYINT:
                Class<?> typeClass = dataType.getTypeClass();
                if (typeClass == Byte.class) {
                    Integer integer = (Integer) val;
                    return integer.byteValue();
                }
                return val;
            case MAP:
                MapType<?, ?> mapType = (MapType<?, ?>) dataType;
                Map<Object, Object> res =
                        ((List<DynamicMessage>) val)
                                .stream()
                                        .collect(
                                                Collectors.toMap(
                                                        dm ->
                                                                convertField(
                                                                        descriptor,
                                                                        dm,
                                                                        mapType.getKeyType(),
                                                                        getFieldValue(dm, "key"),
                                                                        null),
                                                        dm ->
                                                                convertField(
                                                                        descriptor,
                                                                        dm,
                                                                        mapType.getValueType(),
                                                                        getFieldValue(dm, "value"),
                                                                        null)));

                return res;
            case ROW:
                Descriptors.Descriptor nestedTypeByName =
                        descriptor.findNestedTypeByName(fieldName);
                DynamicMessage s =
                        (DynamicMessage)
                                dynamicMessage.getField(
                                        descriptor.findFieldByName(fieldName.toLowerCase()));
                return converter(nestedTypeByName, s, (SeaTunnelRowType) dataType);
            case ARRAY:
                SeaTunnelDataType<?> basicType = ((ArrayType<?, ?>) dataType).getElementType();
                List<Object> list = (List<Object>) val;
                return convertArray(list, basicType);
            default:
                String errorMsg =
                        String.format(
                                "SeaTunnel avro format is not supported for this data type [%s]",
                                dataType.getSqlType());
                throw new RuntimeException(errorMsg);
        }
    }

    private Object getFieldValue(DynamicMessage dm, String fieldName) {
        return dm.getAllFields().entrySet().stream()
                .filter(entry -> entry.getKey().getName().equals(fieldName))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
    }

    protected Object convertArray(List<Object> val, SeaTunnelDataType<?> dataType) {
        if (val == null) {
            return null;
        }
        int length = val.size();
        Object instance = Array.newInstance(dataType.getTypeClass(), length);
        for (int i = 0; i < val.size(); i++) {
            Array.set(instance, i, convertField(null, null, dataType, val.get(i), null));
        }
        return instance;
    }
}
