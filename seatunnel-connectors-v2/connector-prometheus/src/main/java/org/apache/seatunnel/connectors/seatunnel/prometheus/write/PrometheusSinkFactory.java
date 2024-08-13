package org.apache.seatunnel.connectors.seatunnel.prometheus.write;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.http.sink.HttpSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSinkConfig;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class PrometheusSinkFactory extends HttpSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Prometheus";
    }

    public TableSink createSink(TableSinkFactoryContext context) {
        CatalogTable catalogTable = context.getCatalogTable();
        return () ->
                new PrometheusSink(
                        context.getOptions().toConfig(), catalogTable.getSeaTunnelRowType());
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(PrometheusSinkConfig.URL)
                .required(PrometheusSinkConfig.KEY_LABEL)
                .required(PrometheusSinkConfig.KEY_VALUE)
                .optional(PrometheusSinkConfig.KEY_TIMESTAMP)
                .optional(PrometheusSinkConfig.HEADERS)
                .optional(PrometheusSinkConfig.RETRY)
                .optional(PrometheusSinkConfig.RETRY_BACKOFF_MULTIPLIER_MS)
                .optional(PrometheusSinkConfig.RETRY_BACKOFF_MAX_MS)
                .optional(PrometheusSinkConfig.BATCH_SIZE)
                .build();
    }
}
