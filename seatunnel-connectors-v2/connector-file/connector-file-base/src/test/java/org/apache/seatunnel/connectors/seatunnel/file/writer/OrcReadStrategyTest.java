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

package org.apache.seatunnel.connectors.seatunnel.file.writer;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.OrcWriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.OrcReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;

import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

@Slf4j
public class OrcReadStrategyTest {

    private static final String TMP_PATH = "/tmp/seatunnel/orc/";

    public void testOrcWriteStrategy() throws IOException {
        Map<String, Object> writeConfig = new HashMap<>();
        writeConfig.put("tmp_path", TMP_PATH);
        writeConfig.put("path", "/tmp/seatunnel/aa.orc");
        writeConfig.put("file_format_type", FileFormat.ORC.name());

        SeaTunnelRowType writeRowType =
                new SeaTunnelRowType(
                        new String[] {"f1_text", "f2_timestamp"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});
        System.setProperty("HODOOP_HOME", "/");
        FileSinkConfig writeSinkConfig =
                new FileSinkConfig(ConfigFactory.parseMap(writeConfig), writeRowType);
        try (OrcWriteStrategy orcWriteStrategy = new OrcWriteStrategy(writeSinkConfig); ) {
            ParquetReadStrategyTest.LocalConf hadoopConf =
                    new ParquetReadStrategyTest.LocalConf(FS_DEFAULT_NAME_DEFAULT);
            orcWriteStrategy.setCatalogTable(
                    CatalogTableUtil.getCatalogTable("test", null, null, "test", writeRowType));
            orcWriteStrategy.init(hadoopConf, "test1", "test1", 0);
            Random random = new Random();
            for (int i = 0; i < 10; i++) {
                Object[] objects = new Object[3];
                objects[0] = String.valueOf(random.nextInt());
                objects[1] = String.valueOf(random.nextInt());
                SeaTunnelRow seaTunnelRow = new SeaTunnelRow(objects);
                orcWriteStrategy.write(seaTunnelRow);
            }
        }
    }

    public static List<StripeInformation> autoGenData(String filePath, int count)
            throws IOException {
        // Define the schema (for example, a single column with a string type)
        TypeDescription schema =
                TypeDescription.createStruct()
                        .addField("name", TypeDescription.createString())
                        .addField("sex", TypeDescription.createString());

        // Create the ORC file writer
        Path path = new Path(filePath);
        OrcFile.WriterOptions options =
                OrcFile.writerOptions(new org.apache.hadoop.conf.Configuration()).setSchema(schema);
        long startTime = System.currentTimeMillis();
        try (Writer writer = OrcFile.createWriter(path, options); ) {
            // Create a vectorized row batch for writing data
            VectorizedRowBatch batch = schema.createRowBatch(100000);
            Random random = new Random();
            // 300M
            for (int i = 0; i < count; i++) {
                // Add data to the vector
                ((BytesColumnVector) batch.cols[0])
                        .setVal(batch.size, String.valueOf(i).getBytes());
                ((BytesColumnVector) batch.cols[1])
                        .setVal(batch.size, String.valueOf(random.nextInt()).getBytes());
                batch.size++;
                // If the batch is full, write it to the file
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
            // Write any remaining rows to the file
            if (batch.size > 0) {
                writer.addRowBatch(batch);
            }
        }
        OrcFile.ReaderOptions readerOptions =
                OrcFile.readerOptions(new org.apache.hadoop.conf.Configuration());
        int stripes = 0;
        List<StripeInformation> stripeInformation = new ArrayList<>();
        try (Reader reader = OrcFile.createReader(path, readerOptions); ) {
            stripes = reader.getStripes().size();
            stripeInformation = reader.getStripes();
        }
        System.out.println(
                "ORC file written successfully to local_output.orc, rows:"
                        + count
                        + ", stripes:"
                        + stripes
                        + ", useTime:"
                        + (System.currentTimeMillis() - startTime));
        return stripeInformation;
    }

    public static void deleteFile(String filePath) {
        File file = new File(filePath);
        if (file.exists()) {
            file.delete();
        }
    }

    public OrcReadStrategy getSplitOrcReadStrategyBySize(String file) throws IOException {
        OrcReadStrategy orcReadStrategy = new OrcReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        orcReadStrategy.init(localConf);
        SeaTunnelRowType seaTunnelRowTypeInfo = orcReadStrategy.getSeaTunnelRowTypeInfo(file);
        Config config = ConfigFactory.empty();
        config =
                config.withValue(
                        BaseSourceConfigOptions.ROW_COUNT_PER_SPLIT.key(),
                        ConfigValueFactory.fromAnyRef(1500000));
        config =
                config.withValue(
                        BaseSourceConfigOptions.WHETHER_SPLIT_FILE.key(),
                        ConfigValueFactory.fromAnyRef(true));
        orcReadStrategy.setPluginConfig(config);
        return orcReadStrategy;
    }

    public OrcReadStrategy getSplitOrcReadStrategyByRowCount(String file) throws IOException {
        OrcReadStrategy orcReadStrategy = new OrcReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        orcReadStrategy.init(localConf);
        SeaTunnelRowType seaTunnelRowTypeInfo = orcReadStrategy.getSeaTunnelRowTypeInfo(file);
        Config config = ConfigFactory.empty();
        config =
                config.withValue(
                        BaseSourceConfigOptions.ROW_COUNT_PER_SPLIT.key(),
                        ConfigValueFactory.fromAnyRef(1500000));
        config =
                config.withValue(
                        BaseSourceConfigOptions.WHETHER_SPLIT_FILE.key(),
                        ConfigValueFactory.fromAnyRef(true));
        orcReadStrategy.setPluginConfig(config);
        return orcReadStrategy;
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    public void testOrcGetSplits() throws IOException {
        String file = "/tmp/orc_split/local_output.orc";
        deleteFile(file);
        int rowCount = 30000000;
        List<StripeInformation> list = autoGenData(file, rowCount);
        try {
            OrcReadStrategy orcReadStrategy = getSplitOrcReadStrategyBySize(file);
            Set<FileSourceSplit> set = orcReadStrategy.getFileSourceSplits(file);
            for (FileSourceSplit fileSourceSplit :
                    set.stream()
                            .sorted((o1, o2) -> (int) (o1.getMinRowIndex() - o2.getMinRowIndex()))
                            .collect(Collectors.toList())) {
                System.out.println(fileSourceSplit);
            }
            // file size: 300m / 30m
            Assertions.assertTrue(set.size() >= 10);

            orcReadStrategy = getSplitOrcReadStrategyByRowCount(file);
            Set<FileSourceSplit> set1 = orcReadStrategy.getFileSourceSplits(file);
            for (FileSourceSplit fileSourceSplit :
                    set1.stream()
                            .sorted((o1, o2) -> (int) (o1.getMinRowIndex() - o2.getMinRowIndex()))
                            .collect(Collectors.toList())) {
                System.out.println(fileSourceSplit);
            }

            // split size should be fixed as calculate by rowCountPerSplit
            int splitSize = 0;
            for (StripeInformation stripeInformation : list) {
                splitSize +=
                        (int) Math.ceil((double) stripeInformation.getNumberOfRows() / 1500000);
            }
            Assertions.assertEquals(splitSize, set1.size());

        } finally {
            deleteFile(file);
        }
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    public void testOrcReadSplitBigFile() throws IOException {
        String file = "/tmp/orc_split/local_output.orc";
        deleteFile(file);
        int rowCount = 30000000;
        autoGenData(file, rowCount);
        try {
            OrcReadStrategy orcReadStrategy = getSplitOrcReadStrategyBySize(file);
            Set<FileSourceSplit> set = orcReadStrategy.getFileSourceSplits(file);
            long allCount1 = 0;
            // single thread
            for (FileSourceSplit fileSourceSplit : set) {
                long l = readSingleSplitSize(orcReadStrategy, fileSourceSplit);
                allCount1 += l;
            }
            Assertions.assertEquals(rowCount, allCount1);
            // parallel
            AtomicLong allCount = new AtomicLong(0);
            set.parallelStream()
                    .forEach(
                            (a) -> {
                                try {
                                    long l = readSingleSplitSize(orcReadStrategy, a);
                                    allCount.getAndAdd(l);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            });
            // row count should be same
            Assertions.assertEquals(rowCount, allCount.get());
        } finally {
            deleteFile(file);
        }
    }

    public long readSingleSplitSize(
            OrcReadStrategy orcReadStrategy, FileSourceSplit fileSourceSplit) throws IOException {
        TestCollector testCollector = new TestCollector();
        orcReadStrategy.read(fileSourceSplit, testCollector);
        return testCollector.rows.size();
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    public void testOrcReadSplit() throws URISyntaxException, IOException {
        URL orcFile = OrcReadStrategyTest.class.getResource("/test.orc");
        Assertions.assertNotNull(orcFile);
        String orcFilePath = Paths.get(orcFile.toURI()).toString();
        OrcReadStrategy orcReadStrategy = new OrcReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        orcReadStrategy.init(localConf);
        TestCollector testCollector = new TestCollector();
        Set<FileSourceSplit> set = orcReadStrategy.getFileSourceSplits(orcFilePath);
        for (FileSourceSplit fileSourceSplit :
                set.stream()
                        .sorted((o1, o2) -> (int) (o1.getMinRowIndex() - o2.getMinRowIndex()))
                        .collect(Collectors.toList())) {
            System.out.println(fileSourceSplit);
        }
        SeaTunnelRowType seaTunnelRowTypeInfo =
                orcReadStrategy.getSeaTunnelRowTypeInfo(orcFilePath);
        for (FileSourceSplit fileSourceSplit : set) {
            orcReadStrategy.read(fileSourceSplit, testCollector);
            System.out.println(testCollector.rows);
        }
        Assertions.assertEquals(1, testCollector.rows.size());
        for (SeaTunnelRow row : testCollector.getRows()) {
            Assertions.assertEquals(row.getField(0).getClass(), Boolean.class);
            Assertions.assertEquals(row.getField(1).getClass(), Byte.class);
            Assertions.assertEquals(row.getField(16).getClass(), SeaTunnelRow.class);
        }
    }

    @Test
    public void testOrcRead() throws Exception {
        URL orcFile = OrcReadStrategyTest.class.getResource("/test.orc");
        Assertions.assertNotNull(orcFile);
        String orcFilePath = Paths.get(orcFile.toURI()).toString();
        OrcReadStrategy orcReadStrategy = new OrcReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        orcReadStrategy.init(localConf);
        TestCollector testCollector = new TestCollector();
        SeaTunnelRowType seaTunnelRowTypeInfo =
                orcReadStrategy.getSeaTunnelRowTypeInfo(orcFilePath);
        Assertions.assertNotNull(seaTunnelRowTypeInfo);
        log.info(seaTunnelRowTypeInfo.toString());
        orcReadStrategy.read(orcFilePath, "", testCollector);
        for (SeaTunnelRow row : testCollector.getRows()) {
            Assertions.assertEquals(row.getField(0).getClass(), Boolean.class);
            Assertions.assertEquals(row.getField(1).getClass(), Byte.class);
            Assertions.assertEquals(row.getField(16).getClass(), SeaTunnelRow.class);
        }
    }

    @Test
    public void testOrcReadProjection() throws Exception {
        URL orcFile = OrcReadStrategyTest.class.getResource("/test.orc");
        URL conf = OrcReadStrategyTest.class.getResource("/test_read_orc.conf");
        Assertions.assertNotNull(orcFile);
        Assertions.assertNotNull(conf);
        String orcFilePath = Paths.get(orcFile.toURI()).toString();
        String confPath = Paths.get(conf.toURI()).toString();
        OrcReadStrategy orcReadStrategy = new OrcReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        Config pluginConfig = ConfigFactory.parseFile(new File(confPath));
        orcReadStrategy.init(localConf);
        orcReadStrategy.setPluginConfig(pluginConfig);
        TestCollector testCollector = new TestCollector();
        SeaTunnelRowType seaTunnelRowTypeInfo =
                orcReadStrategy.getSeaTunnelRowTypeInfo(orcFilePath);
        Assertions.assertNotNull(seaTunnelRowTypeInfo);
        log.info(seaTunnelRowTypeInfo.toString());
        orcReadStrategy.read(orcFilePath, "", testCollector);
        for (SeaTunnelRow row : testCollector.getRows()) {
            Assertions.assertEquals(row.getField(0).getClass(), Byte.class);
            Assertions.assertEquals(row.getField(1).getClass(), Boolean.class);
        }
    }

    public static class TestCollector implements Collector<SeaTunnelRow> {

        private final List<SeaTunnelRow> rows = new ArrayList<>();

        public List<SeaTunnelRow> getRows() {
            return rows;
        }

        @Override
        public void collect(SeaTunnelRow record) {
            log.info(record.toString());
            rows.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return null;
        }
    }

    public static class LocalConf extends HadoopConf {
        private static final String HDFS_IMPL = "org.apache.hadoop.fs.LocalFileSystem";
        private static final String SCHEMA = "file";

        public LocalConf(String hdfsNameKey) {
            super(hdfsNameKey);
        }

        @Override
        public String getFsHdfsImpl() {
            return HDFS_IMPL;
        }

        @Override
        public String getSchema() {
            return SCHEMA;
        }
    }
}
