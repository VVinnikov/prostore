package ru.ibs.dtm.query.calcite.core.extension.eddl;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.springframework.util.CollectionUtils;
import ru.ibs.dtm.common.dto.TableInfo;

import java.util.List;
import java.util.Optional;

/**
 * Методы для работы с sqlNode
 */
public class SqlNodeUtils {

    public static List<String> getTableNames(SqlCall sqlNode) {
        List<String> names = getOne(sqlNode, SqlIdentifier.class).names;
        if (CollectionUtils.isEmpty(names) || names.size() > 2) {
            throw new RuntimeException("Наименование таблицы должно быть представлено в виде " +
                    "[schema_name.table_name | table_name]");
        }
        return names;
    }

    public static TableInfo getTableInfo(SqlCall sqlNode, String defaultSchema) {
        List<String> tableNames = getTableNames(sqlNode);
        return new TableInfo(getSchema(tableNames, defaultSchema), getTableName(tableNames));
    }

    public static TableInfo getTableInfo(SqlIdentifier sqlNode, String defaultSchema) {
        List<String> tableNames = sqlNode.names;
        return new TableInfo(getSchema(tableNames, defaultSchema), getTableName(tableNames));
    }

    public static <T> T getOne(SqlCall sqlNode, Class<T> sqlNodeClass) {
        Optional<T> node = sqlNode.getOperandList().stream()
                .filter(sqlNodeClass::isInstance)
                .map(operand -> (T) operand)
                .findAny();
        if (node.isPresent()) {
            return node.get();
        }
        throw new RuntimeException("Не удалось найти параметр в [" + sqlNode + "]");
    }

    private static String getTableName(List<String> names) {
        return names.get(names.size() - 1);
    }

    private static String getSchema(List<String> names, String defaultSchema) {
        return names.size() > 1 ? names.get(names.size() - 2) : defaultSchema;
    }
}
