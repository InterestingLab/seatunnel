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
package org.apache.seatunnel.e2e.transform;

import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

public class TestJsonPathTransformIT extends TestSuiteBase {

    @TestTemplate
    public void testBasicType(TestContainer container) throws Exception {
        Container.ExecResult execResult =
                container.executeJob("/json_path_transform/json_path_basic_type_test.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testBasicTypeMultiTable(TestContainer container) throws Exception {
        Container.ExecResult execResult =
                container.executeJob(
                        "/json_path_transform/json_path_basic_type_test_multi_table.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testArray(TestContainer container) throws Exception {
        Container.ExecResult execResult =
                container.executeJob("/json_path_transform/array_test.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testNestedRow(TestContainer container) throws Exception {
        Container.ExecResult execResult =
                container.executeJob("/json_path_transform/nested_row_test.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testErrorHandleWay(TestContainer container) throws Exception {
        Container.ExecResult execResult =
                container.executeJob("/json_path_transform/json_path_with_error_handle_way.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testArrayType(TestContainer container) throws Exception {
        Container.ExecResult execResult =
                container.executeJob("/json_path_transform/json_path_array_map.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }
}
