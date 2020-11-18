package io.arenadata.dtm.query.execution.plugin.adb.factory.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityFieldUtils;
import io.arenadata.dtm.common.model.ddl.EntityTypeUtil;
import io.arenadata.dtm.query.execution.plugin.adb.dto.AdbTableColumn;
import io.arenadata.dtm.query.execution.plugin.adb.dto.AdbTableEntity;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.ddl.AdbCreateTableQueries;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.CreateTableQueriesFactory;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class AdbCreateTableQueriesFactory implements CreateTableQueriesFactory<AdbCreateTableQueries> {

    public static final String ACTUAL_TABLE_POSTFIX = "actual";
    /**
     * History table name
     */
    public static final String HISTORY_TABLE_POSTFIX = "history";
    /**
     * Name staging table
     */
    public static final String STAGING_TABLE_POSTFIX = "staging";
    /**
     * Delta Number System Field
     */
    public static final String SYS_FROM_ATTR = "sys_from";
    /**
     * System field of maximum delta number
     */
    public static final String SYS_TO_ATTR = "sys_to";
    /**
     * System field of operation on an object
     */
    public static final String SYS_OP_ATTR = "sys_op";
    /**
     * Request ID system field
     */
    public static final String REQ_ID_ATTR = "req_id";
    public static final String TABLE_POSTFIX_DELIMITER = "_";

    public static final String CREATE_PATTERN = "CREATE TABLE %s.%s (%s%s)%s";
    public static final String PRIMARY_KEY_PATTERN = ", constraint pk_%s primary key (%s)";
    public static final String SHARDING_KEY_PATTERN = " DISTRIBUTED BY (%s)";

    private static final List<AdbTableColumn> SYSTEM_COLUMNS = Arrays.asList(
            new AdbTableColumn(SYS_FROM_ATTR, "bigint", true),
            new AdbTableColumn(SYS_TO_ATTR, "bigint", true),
            new AdbTableColumn(SYS_OP_ATTR, "int", true)
    );

    @Override
    public AdbCreateTableQueries create(DdlRequestContext context) {

        Entity entity = context.getRequest().getEntity();
        AdbTableEntity actualTableEntity = createTableEntity(entity, getTableName(entity, ACTUAL_TABLE_POSTFIX), false, false);
        AdbTableEntity historyTableEntity = createTableEntity(entity, getTableName(entity, HISTORY_TABLE_POSTFIX), false, true);
        AdbTableEntity stagingTableEntity = createTableEntity(entity, getTableName(entity, STAGING_TABLE_POSTFIX), true, false);
        return AdbCreateTableQueries.builder()
                .actual(createTableQuery(actualTableEntity))
                .actualEntity(actualTableEntity)
                .history(createTableQuery(historyTableEntity))
                .historyEntity(historyTableEntity)
                .staging(createTableQuery(stagingTableEntity))
                .stagingEntity(stagingTableEntity)
                .build();
    }

    private AdbTableEntity createTableEntity(Entity entity,
                                             String tableName,
                                             boolean addReqId,
                                             boolean pkWithSystemFields) {
        List<EntityField> entityFields = entity.getFields();
        AdbTableEntity adbTableEntity = new AdbTableEntity();
        adbTableEntity.setSchema(entity.getSchema());
        adbTableEntity.setName(tableName);
        List<AdbTableColumn> columns = entityFields.stream()
                .map(this::transformColumn)
                .collect(Collectors.toList());
        columns.addAll(SYSTEM_COLUMNS);
        if (addReqId) {
            columns.add(new AdbTableColumn(REQ_ID_ATTR, "varchar(36)", true));
        }
        adbTableEntity.setColumns(columns);
        List<String> pkList = EntityFieldUtils.getPrimaryKeyList(entityFields).stream()
                .map(EntityField::getName)
                .collect(Collectors.toList());
        if (pkWithSystemFields) {
            pkList.add(SYS_FROM_ATTR);
        }
        adbTableEntity.setPrimaryKeys(pkList);
        adbTableEntity.setShardingKeys(EntityFieldUtils.getShardingKeyList(entityFields).stream()
                .map(EntityField::getName)
                .collect(Collectors.toList()));
        return adbTableEntity;
    }

    private String getTableName(Entity entity,
                                String tablePostfix) {
        return entity.getName() + TABLE_POSTFIX_DELIMITER + tablePostfix;
    }

    private AdbTableColumn transformColumn(EntityField field) {
        return new AdbTableColumn(field.getName(), EntityTypeUtil.pgFromDtmType(field), field.getNullable());
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
        return String.format("%s %s%s", column.getName(), column.getType(), column.isNullable() ? "" : " NOT NULL");
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
