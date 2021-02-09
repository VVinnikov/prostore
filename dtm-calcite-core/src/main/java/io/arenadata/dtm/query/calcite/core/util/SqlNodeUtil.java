package io.arenadata.dtm.query.calcite.core.util;

import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import org.apache.calcite.sql.SqlNode;

public final class SqlNodeUtil {
    private SqlNodeUtil() {
    }

    public static SqlNode copy(SqlNode sqlNode) {
        return new SqlSelectTree(sqlNode).copy().getRoot().getNode();
    }
}
