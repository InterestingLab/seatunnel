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

package org.apache.seatunnel.connectors.seatunnel.fake.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.fake.config.FakeConfig;
import org.apache.seatunnel.connectors.seatunnel.fake.state.FakeSourceState;

import java.util.Collections;
import java.util.List;

public class FakeSource
        implements SeaTunnelSource<SeaTunnelRow, FakeSourceSplit, FakeSourceState>,
                SupportParallelism,
                SupportColumnProjection {

    private JobContext jobContext;
    private List<CatalogTable> catalogTables;
    private FakeConfig fakeConfig;

    public FakeSource() {}

    public FakeSource(ReadonlyConfig readonlyConfig) {
        this.catalogTables = CatalogTableUtil.buildWithConfig(getPluginName(), readonlyConfig);
        this.fakeConfig = FakeConfig.buildWithConfig(readonlyConfig.toConfig());
    }

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return catalogTables;
    }

    @Override
    public SourceSplitEnumerator<FakeSourceSplit, FakeSourceState> createEnumerator(
            SourceSplitEnumerator.Context<FakeSourceSplit> enumeratorContext) throws Exception {
        return new FakeSourceSplitEnumerator(enumeratorContext, fakeConfig, Collections.emptySet());
    }

    @Override
    public SourceSplitEnumerator<FakeSourceSplit, FakeSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<FakeSourceSplit> enumeratorContext,
            FakeSourceState checkpointState) {
        return new FakeSourceSplitEnumerator(
                enumeratorContext, fakeConfig, checkpointState.getAssignedSplits());
    }

    @Override
    public SourceReader<SeaTunnelRow, FakeSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new FakeSourceReader(readerContext, catalogTables, fakeConfig);
    }

    @Override
    public String getPluginName() {
        return "FakeSource";
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }
}
