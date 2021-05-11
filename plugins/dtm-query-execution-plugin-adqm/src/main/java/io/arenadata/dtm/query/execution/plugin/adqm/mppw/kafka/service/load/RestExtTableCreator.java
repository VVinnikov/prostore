package io.arenadata.dtm.query.execution.plugin.adqm.mppw.kafka.service.load;

import io.arenadata.dtm.query.execution.plugin.adqm.base.utils.AdqmDdlUtil;
import io.arenadata.dtm.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import lombok.NonNull;
import org.apache.avro.Schema;

import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adqm.base.utils.Constants.EXT_SHARD_POSTFIX;
import static java.lang.String.format;

public class RestExtTableCreator implements ExtTableCreator {
    private static final String EXT_SHARD_TEMPLATE =
            "CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s (\n" +
                    "  %s\n" +
                    ")\n" +
                    "ENGINE = MergeTree()\n" +
                    "ORDER BY (%s)\n";

    private final DdlProperties ddlProperties;

    public RestExtTableCreator(DdlProperties ddlProperties) {
        this.ddlProperties = ddlProperties;
    }

    @Override
    public String generate(@NonNull String topic, @NonNull String table, @NonNull Schema schema, @NonNull String sortingKey) {
        String removeSysFrom = sortingKey.replaceAll(",\\s*sys_from", "");
        String columns = schema.getFields().stream()
                .map(f -> {
                    boolean isNullable = !sortingKey.contains(f.name());
                    return AdqmDdlUtil.avroFieldToString(f, isNullable);
                })
                .collect(Collectors.joining(", "));

        return format(EXT_SHARD_TEMPLATE, table + EXT_SHARD_POSTFIX, ddlProperties.getCluster(), columns, removeSysFrom);
    }
}
