package io.arenadata.dtm.query.execution.core.service.dml;

import io.vertx.core.Future;
import lombok.SneakyThrows;
import org.apache.calcite.sql.SqlNode;

public interface LogicViewReplacer {
    Future<String> replace(String sql, String datamart);

    @SneakyThrows
    Future<SqlNode> replace(SqlNode sql, String datamart);
}
