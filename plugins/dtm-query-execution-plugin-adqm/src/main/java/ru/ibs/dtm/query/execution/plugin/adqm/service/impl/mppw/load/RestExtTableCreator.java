package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.mppw.load;

import lombok.NonNull;
import org.apache.avro.Schema;
import ru.ibs.dtm.query.execution.plugin.adqm.common.DdlUtils;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static ru.ibs.dtm.query.execution.plugin.adqm.common.Constants.EXT_SHARD_POSTFIX;

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
                    return DdlUtils.avroFieldToString(f, isNullable);
                })
                .collect(Collectors.joining(", "));

        return format(EXT_SHARD_TEMPLATE, table + EXT_SHARD_POSTFIX, ddlProperties.getCluster(), columns, removeSysFrom);
    }
}
