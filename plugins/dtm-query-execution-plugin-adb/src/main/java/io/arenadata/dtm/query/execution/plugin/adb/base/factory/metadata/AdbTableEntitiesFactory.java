package io.arenadata.dtm.query.execution.plugin.adb.base.factory.metadata;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityFieldUtils;
import io.arenadata.dtm.common.model.ddl.EntityTypeUtil;
import io.arenadata.dtm.query.execution.plugin.adb.base.dto.metadata.AdbTableColumn;
import io.arenadata.dtm.query.execution.plugin.adb.base.dto.metadata.AdbTableEntity;
import io.arenadata.dtm.query.execution.plugin.adb.base.dto.metadata.AdbTables;
import io.arenadata.dtm.query.execution.plugin.api.factory.TableEntitiesFactory;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adb.base.factory.Constants.*;

@Service("adbTableEntitiesFactory")
public class AdbTableEntitiesFactory implements TableEntitiesFactory<AdbTables<AdbTableEntity>> {
    public static final String TABLE_POSTFIX_DELIMITER = "_";

    private static final List<AdbTableColumn> SYSTEM_COLUMNS = Arrays.asList(
            new AdbTableColumn(SYS_FROM_ATTR, "int8", true),
            new AdbTableColumn(SYS_TO_ATTR, "int8", true),
            new AdbTableColumn(SYS_OP_ATTR, "int4", true)
    );

    @Override
    public AdbTables<AdbTableEntity> create(Entity entity, String envName) {
        return new AdbTables<>(
                createTableEntity(entity, AdbTables.ACTUAL_TABLE_POSTFIX),
                createTableEntity(entity, AdbTables.HISTORY_TABLE_POSTFIX),
                createTableEntity(entity, AdbTables.STAGING_TABLE_POSTFIX)
        );
    }

    private AdbTableEntity createTableEntity(Entity entity, String tablePostfix) {
        AdbTableEntity tableEntity;
        List<String> pkTableColumnKeys;
        switch (tablePostfix) {
            case AdbTables.ACTUAL_TABLE_POSTFIX:
            case AdbTables.HISTORY_TABLE_POSTFIX:
                tableEntity = createEntity(entity, getTableName(entity, tablePostfix));
                pkTableColumnKeys = createPkKeys(entity.getFields());
                pkTableColumnKeys.add(SYS_FROM_ATTR);
                tableEntity.setPrimaryKeys(pkTableColumnKeys);
                return tableEntity;
            case AdbTables.STAGING_TABLE_POSTFIX:
                tableEntity = createEntity(entity, getTableName(entity, tablePostfix));
                tableEntity.setPrimaryKeys(Collections.emptyList());
                return tableEntity;
            default:
                throw new DtmException(String.format("Incorrect table postfix %s", tablePostfix));
        }
    }

    private AdbTableEntity createEntity(Entity entity, String tableName) {
        List<EntityField> entityFields = entity.getFields();
        AdbTableEntity adbTableEntity = new AdbTableEntity();
        adbTableEntity.setSchema(entity.getSchema());
        adbTableEntity.setName(tableName);
        List<AdbTableColumn> columns = entityFields.stream()
                .sorted(Comparator.comparingInt(EntityField::getOrdinalPosition))
                .map(this::transformColumn)
                .collect(Collectors.toList());
        columns.addAll(SYSTEM_COLUMNS);
        adbTableEntity.setColumns(columns);
        adbTableEntity.setShardingKeys(EntityFieldUtils.getShardingKeyList(entityFields).stream()
                .map(EntityField::getName)
                .collect(Collectors.toList()));
        return adbTableEntity;
    }

    private String getTableName(Entity entity,
                                String tablePostfix) {
        return entity.getName() + TABLE_POSTFIX_DELIMITER + tablePostfix;
    }

    private List<String> createPkKeys(List<EntityField> entityFields) {
        return EntityFieldUtils.getPrimaryKeyList(entityFields).stream()
                .map(EntityField::getName)
                .collect(Collectors.toList());
    }

    private AdbTableColumn transformColumn(EntityField field) {
        return new AdbTableColumn(field.getName(), EntityTypeUtil.pgFromDtmType(field), field.getNullable());
    }
}
