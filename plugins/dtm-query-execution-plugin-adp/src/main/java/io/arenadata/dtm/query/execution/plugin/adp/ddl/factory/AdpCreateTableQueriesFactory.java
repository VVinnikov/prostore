package io.arenadata.dtm.query.execution.plugin.adp.ddl.factory;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.adp.base.dto.metadata.AdpTableColumn;
import io.arenadata.dtm.query.execution.plugin.adp.base.dto.metadata.AdpTableEntity;
import io.arenadata.dtm.query.execution.plugin.adp.base.dto.metadata.AdpTables;
import io.arenadata.dtm.query.execution.plugin.api.factory.CreateTableQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.api.factory.TableEntitiesFactory;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
@AllArgsConstructor
public class AdpCreateTableQueriesFactory implements CreateTableQueriesFactory<AdpTables> {

    public static final String CREATE_PATTERN = "CREATE TABLE %s.%s (%s%s)%s";
    public static final String PRIMARY_KEY_PATTERN = ", constraint pk_%s primary key (%s)";
    public static final String SHARDING_KEY_PATTERN = " DISTRIBUTED BY (%s)";

    private final TableEntitiesFactory<AdpTables<AdpTableEntity>> tableEntitiesFactory;

    @Override
    public AdpTables create(Entity entity, String envName) {
        AdpTables<AdpTableEntity> tableEntities = tableEntitiesFactory.create(entity, envName);
        return new AdpTables<>(createTableQuery(tableEntities.getActual()),
                createTableQuery(tableEntities.getStaging()));
    }

    private String createTableQuery(AdpTableEntity table) {
        return String.format(CREATE_PATTERN, table.getSchema(), table.getName(),
                getColumnsQuery(table), getPrimaryKeyQuery(table),
                getShardingKeyQuery(table));
    }

    private String getColumnsQuery(AdpTableEntity AdpTableEntity) {
        return AdpTableEntity.getColumns().stream()
                .map(this::getColumnQuery)
                .collect(Collectors.joining(", "));
    }

    private String getColumnQuery(AdpTableColumn column) {
        return String.format("%s %s%s", column.getName(), column.getType(), column.getNullable() ? "" : " NOT NULL");
    }

    private String getPrimaryKeyQuery(AdpTableEntity AdpTableEntity) {
        List<String> primaryKeys = AdpTableEntity.getPrimaryKeys();
        String pkTableName = String.format("%s_%s", AdpTableEntity.getSchema(), AdpTableEntity.getName());
        String pkKeys = String.join(", ", primaryKeys);
        return primaryKeys.isEmpty() ? "" : String.format(PRIMARY_KEY_PATTERN, pkTableName, pkKeys);
    }

    private String getShardingKeyQuery(AdpTableEntity AdpTableEntity) {
        return String.format(SHARDING_KEY_PATTERN, String.join(", ", AdpTableEntity.getShardingKeys()));
    }

}
