package io.arenadata.dtm.query.execution.core.ddl.utils;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.calcite.core.visitors.SqlInvalidTimestampFinder;
import io.arenadata.dtm.query.execution.core.base.exception.table.ValidationDtmException;
import lombok.val;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public final class ValidationUtils {
    private ValidationUtils() {
    }

    public static void checkVarcharSize(List<EntityField> fields) {
        List<String> notSetSizeFields = fields.stream()
                .filter(field -> field.getType() == ColumnType.CHAR)
                .filter(field -> field.getSize() == null)
                .map(EntityField::getName)
                .collect(Collectors.toList());
        if (!notSetSizeFields.isEmpty()) {
            throw new ValidationDtmException(
                    String.format("Specifying the size for columns%s with types[CHAR] is required", notSetSizeFields)
            );
        }
    }

    public static void checkRequiredKeys(List<EntityField> fields) {
        val notExistsKeys = new ArrayList<String>();
        val notExistsPrimaryKeys = fields.stream()
                .noneMatch(f -> f.getPrimaryOrder() != null);
        if (notExistsPrimaryKeys) {
            notExistsKeys.add("primary key(s)");
        }

        val notExistsShardingKey = fields.stream()
                .noneMatch(f -> f.getShardingOrder() != null);
        if (notExistsShardingKey) {
            notExistsKeys.add("sharding key(s)");
        }

        if (!notExistsKeys.isEmpty()) {
            throw new ValidationDtmException(
                    String.format("Primary keys and Sharding keys are required. The following keys do not exist: %s",
                            String.join(",", notExistsKeys)));
        }
    }


    public static void checkFieldsDuplication(List<EntityField> fields) {
        Set<String> uniqueFieldNames = fields.stream()
                .map(EntityField::getName)
                .collect(Collectors.toSet());

        if (uniqueFieldNames.size() != fields.size()) {
            throw new ValidationDtmException("Entity has duplication fields names");
        }
    }

    public static void checkTimestampFormat(SqlNode node) {
        val finder = new SqlInvalidTimestampFinder();
        node.accept(finder);
        if (!finder.getInvalidTimestamps().isEmpty()) {
            throw new ValidationDtmException(String.format("Query contains invalid TIMESTAMP format [yyyy-MM-dd HH:mm:ss(.mmmmmm)]: %s", finder.getInvalidTimestamps()));
        }
    }
}
