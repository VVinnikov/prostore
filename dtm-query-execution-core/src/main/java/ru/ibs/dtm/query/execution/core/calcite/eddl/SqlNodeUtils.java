package ru.ibs.dtm.query.execution.core.calcite.eddl;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Optional;

/**
 * Методы для работы с sqlNode
 */
public class SqlNodeUtils {

  public static List<String> getTableNames(SqlDdl ddlNode) {
    List<String> names = getOne(ddlNode, SqlIdentifier.class).names;
    if (CollectionUtils.isEmpty(names) || names.size() > 2) {
      throw new RuntimeException("Наименование таблицы должно быть представлено в виде " +
        "[schema_name.table_name | table_name]");
    }
    return names;
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
}
