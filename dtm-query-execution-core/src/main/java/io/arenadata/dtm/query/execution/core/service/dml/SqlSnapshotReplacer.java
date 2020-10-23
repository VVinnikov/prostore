package io.arenadata.dtm.query.execution.core.service.dml;

import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSnapshot;

public interface SqlSnapshotReplacer {
    void replace(SqlSnapshot parentSnapshot, SqlSelect replacingNode);
}
