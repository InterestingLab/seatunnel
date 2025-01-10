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

package org.apache.seatunnel.connectors.seatunnel.file.local;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.connectors.seatunnel.file.local.sink.LocalFileSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.sink.SinkFlowTestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LocalFileTest {

    CatalogTable catalogTable =
            CatalogTable.of(
                    TableIdentifier.of("catalog", "database", "table"),
                    TableSchema.builder()
                            .column(
                                    PhysicalColumn.of(
                                            "test", BasicType.STRING_TYPE, 1L, true, null, ""))
                            .build(),
                    Collections.emptyMap(),
                    Collections.emptyList(),
                    "comment");

    @Test
    void testSingleFileMode() throws IOException {
        Map<String, Object> options =
                new HashMap<String, Object>() {
                    {
                        put("path", "/tmp/seatunnel/LocalFileTest");
                        put("row_delimiter", "\n");
                        put("file_name_expression", "only_one_file");
                        put("file_format_type", "text");
                        put("is_enable_transaction", false);
                        put("batch_size", 1);
                    }
                };
        options.put("single_file_mode", true);
        FileUtils.deleteFile("/tmp/seatunnel/LocalFileTest");
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                catalogTable,
                ReadonlyConfig.fromMap(options),
                new LocalFileSinkFactory(),
                Arrays.asList(
                        new SeaTunnelRow(new Object[] {"test"}),
                        new SeaTunnelRow(new Object[] {"test"})));
        Assertions.assertTrue(
                FileUtils.getFileLineNumber("/tmp/seatunnel/LocalFileTest/only_one_file.txt") == 2);

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                SinkFlowTestUtils.runBatchWithCheckpointEnabled(
                                        catalogTable,
                                        ReadonlyConfig.fromMap(options),
                                        new LocalFileSinkFactory(),
                                        Arrays.asList(
                                                new SeaTunnelRow(new Object[] {"test"}),
                                                new SeaTunnelRow(new Object[] {"test"}))));
        Assertions.assertEquals(
                "Single file mode is not supported when checkpoint is enabled or in streaming mode.",
                exception.getMessage());

        IllegalArgumentException exception2 =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                SinkFlowTestUtils.runParallelSubtasksBatchWithCheckpointDisabled(
                                        catalogTable,
                                        ReadonlyConfig.fromMap(options),
                                        new LocalFileSinkFactory(),
                                        Arrays.asList(
                                                new SeaTunnelRow(new Object[] {"test"}),
                                                new SeaTunnelRow(new Object[] {"test"})),
                                        2));
        Assertions.assertEquals(
                "Single file mode is not supported when file_name_expression not contains ${transactionId} but has parallel subtasks.",
                exception2.getMessage());

        FileUtils.deleteFile("/tmp/seatunnel/LocalFileTest");
        options.put("file_name_expression", "${transactionId}_2");
        SinkFlowTestUtils.runParallelSubtasksBatchWithCheckpointDisabled(
                catalogTable,
                ReadonlyConfig.fromMap(options),
                new LocalFileSinkFactory(),
                Arrays.asList(
                        new SeaTunnelRow(new Object[] {"test"}),
                        new SeaTunnelRow(new Object[] {"test"})),
                2);
        Assertions.assertFalse(
                FileUtils.isFileExist("/tmp/seatunnel/LocalFileTest/only_one_file.txt"));
        Assertions.assertTrue(FileUtils.listFile("/tmp/seatunnel/LocalFileTest").size() == 2);

        options.put("single_file_mode", false);
        options.put("file_name_expression", "only_one_file");
        FileUtils.deleteFile("/tmp/seatunnel/LocalFileTest");
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                catalogTable,
                ReadonlyConfig.fromMap(options),
                new LocalFileSinkFactory(),
                Arrays.asList(
                        new SeaTunnelRow(new Object[] {"test"}),
                        new SeaTunnelRow(new Object[] {"test"})));
        Assertions.assertFalse(
                FileUtils.isFileExist("/tmp/seatunnel/LocalFileTest/only_one_file.txt"));
        Assertions.assertTrue(
                FileUtils.getFileLineNumber("/tmp/seatunnel/LocalFileTest/only_one_file_0.txt")
                        == 1);
        Assertions.assertTrue(
                FileUtils.getFileLineNumber("/tmp/seatunnel/LocalFileTest/only_one_file_1.txt")
                        == 1);
    }
}
