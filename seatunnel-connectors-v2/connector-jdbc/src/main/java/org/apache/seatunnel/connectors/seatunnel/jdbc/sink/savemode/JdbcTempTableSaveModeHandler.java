/// *
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
// package org.apache.seatunnel.connectors.seatunnel.jdbc.sink.savemode;
//
// import org.apache.seatunnel.api.sink.DataSaveMode;
// import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
// import org.apache.seatunnel.api.sink.SaveModeHandler;
// import org.apache.seatunnel.api.sink.SchemaSaveMode;
// import org.apache.seatunnel.api.table.catalog.Catalog;
// import org.apache.seatunnel.api.table.catalog.CatalogTable;
// import org.apache.seatunnel.api.table.catalog.TablePath;
//
// import lombok.Getter;
// import lombok.extern.slf4j.Slf4j;
//
// @Slf4j
// public class JdbcTempTableSaveModeHandler extends JdbcSaveModeHandler implements SaveModeHandler
// {
//    @Getter private final TablePath tempTablePath;
//
//    @Getter private final CatalogTable tempCatalogTable;
//
//    public JdbcTempTableSaveModeHandler(
//            SchemaSaveMode schemaSaveMode,
//            DataSaveMode dataSaveMode,
//            Catalog catalog,
//            TablePath tablePath,
//            CatalogTable catalogTable,
//            TablePath tempTablePath,
//            CatalogTable tempCatalogTable,
//            String customSql,
//            boolean createIndex) {
//        super(
//                schemaSaveMode,
//                dataSaveMode,
//                catalog,
//                tablePath,
//                catalogTable,
//                customSql,
//                createIndex);
//        this.tempTablePath = tempTablePath;
//        this.tempCatalogTable = tempCatalogTable;
//    }
//
//    @Override
//    protected void createSchemaWhenNotExist() {
//        if (!tableExists()) {
//            createTable();
//        }
//        if (tempTablePath != null && tempCatalogTable != null) {
//            if (!catalog.tableExists(tempTablePath)) {
//                DefaultSaveModeHandler.createTablePreCheck(
//                        tempTablePath, catalog, tempCatalogTable);
//                catalog.createTable(tempTablePath, tempCatalogTable, true, true);
//            }
//        }
//    }
// }
