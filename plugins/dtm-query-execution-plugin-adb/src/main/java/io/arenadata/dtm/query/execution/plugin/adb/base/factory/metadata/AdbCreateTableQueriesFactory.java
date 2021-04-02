package io.arenadata.dtm.query.execution.plugin.adb.base.factory.metadata;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.adb.base.dto.AdbTableColumn;
import io.arenadata.dtm.query.execution.plugin.adb.base.dto.AdbTableEntity;
import io.arenadata.dtm.query.execution.plugin.adb.base.dto.AdbTables;
import io.arenadata.dtm.query.execution.plugin.api.factory.CreateTableQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.api.factory.TableEntitiesFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service("adbCreateTableQueriesFactory")
public class AdbCreateTableQueriesFactory implements CreateTableQueriesFactory<AdbTables<String>> {
    public static final String CREATE_PATTERN = "CREATE TABLE %s.%s (%s%s)%s";
    public static final String PRIMARY_KEY_PATTERN = ", constraint pk_%s primary key (%s)";
    public static final String SHARDING_KEY_PATTERN = " DISTRIBUTED BY (%s)";

    private final TableEntitiesFactory<AdbTables<AdbTableEntity>> tableEntitiesFactory;

    @Autowired
    public AdbCreateTableQueriesFactory(TableEntitiesFactory<AdbTables<AdbTableEntity>> tableEntitiesFactory) {
        this.tableEntitiesFactory = tableEntitiesFactory;
    }

    @Override
    public AdbTables<String> create(Entity entity, String envName) {
        AdbTables<AdbTableEntity> tableEntities = tableEntitiesFactory.create(entity, envName);
        return new AdbTables<>(createTableQuery(tableEntities.getActual()),
                createTableQuery(tableEntities.getHistory()),
                createTableQuery(tableEntities.getStaging()));
    }

    private String createTableQuery(AdbTableEntity adbTableEntity) {
        return String.format(CREATE_PATTERN, adbTableEntity.getSchema(), adbTableEntity.getName(),
                getColumnsQuery(adbTableEntity), getPrimaryKeyQuery(adbTableEntity),
                getShardingKeyQuery(adbTableEntity));
    }

    private String getColumnsQuery(AdbTableEntity adbTableEntity) {
        return adbTableEntity.getColumns().stream()
                .map(this::getColumnQuery)
                .collect(Collectors.joining(", "));
    }

    private String getColumnQuery(AdbTableColumn column) {
        return String.format("%s %s%s", column.getName(), column.getType(), column.getNullable() ? "" : " NOT NULL");
    }

    private String getPrimaryKeyQuery(AdbTableEntity adbTableEntity) {
        List<String> primaryKeys = adbTableEntity.getPrimaryKeys();
        String pkTableName = String.format("%s_%s", adbTableEntity.getSchema(), adbTableEntity.getName());
        String pkKeys = String.join(", ", primaryKeys);
        return primaryKeys.isEmpty() ? "" : String.format(PRIMARY_KEY_PATTERN, pkTableName, pkKeys);
    }

    private String getShardingKeyQuery(AdbTableEntity adbTableEntity) {
        return String.format(SHARDING_KEY_PATTERN, String.join(", ", adbTableEntity.getShardingKeys()));
    }
}
