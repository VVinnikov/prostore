package io.arenadata.dtm.query.execution.plugin.adb.factory.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityFieldUtils;
import io.arenadata.dtm.common.model.ddl.EntityTypeUtil;
import io.arenadata.dtm.query.execution.plugin.adb.dto.AdbTableColumn;
import io.arenadata.dtm.query.execution.plugin.adb.dto.AdbTableEntity;
import io.arenadata.dtm.query.execution.plugin.adb.dto.AdbTables;
import io.arenadata.dtm.query.execution.plugin.api.factory.TableEntitiesFactory;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Service("adbTableEntitiesFactory")
public class AdbTableEntitiesFactory implements TableEntitiesFactory<AdbTables<AdbTableEntity>> {

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

    private static final List<AdbTableColumn> SYSTEM_COLUMNS = Arrays.asList(
            new AdbTableColumn(SYS_FROM_ATTR, "int8", true),
            new AdbTableColumn(SYS_TO_ATTR, "int8", true),
            new AdbTableColumn(SYS_OP_ATTR, "int4", true)
    );

    @Override
    public AdbTables<AdbTableEntity> create(Entity entity, String envName) {
        return new AdbTables<>(
                createTableEntity(entity, getTableName(entity, AdbTables.ACTUAL_TABLE_POSTFIX), false, true),
                createTableEntity(entity, getTableName(entity, AdbTables.HISTORY_TABLE_POSTFIX), false, true),
                createTableEntity(entity, getTableName(entity, AdbTables.STAGING_TABLE_POSTFIX), true, false)
        );
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
                .sorted(Comparator.comparingInt(EntityField::getOrdinalPosition))
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
}
