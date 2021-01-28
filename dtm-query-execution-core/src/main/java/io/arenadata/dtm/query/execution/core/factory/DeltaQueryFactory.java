package io.arenadata.dtm.query.execution.core.factory;

import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaQuery;
import org.apache.calcite.sql.SqlNode;

public interface DeltaQueryFactory {
    DeltaQuery create(SqlNode sqlNode);
}
