package io.arenadata.dtm.query.calcite.core.util;

import io.arenadata.dtm.query.calcite.core.extension.parser.ParseException;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;

public final class SqlNodeUtil {
    private SqlNodeUtil() {
    }

    public static SqlNode copy(SqlNode sqlNode) {
        return new SqlSelectTree(sqlNode).copy().getRoot().getNode();
    }

    public static SqlNode getViewQueryAndCheck(SqlNode query) throws ParseException {
        if (query instanceof SqlSelect) {
            if (((SqlSelect) query).getFrom() == null) {
                throw new ParseException("View query must have from clause!");
            } else {
                return query;
            }
        } else {
            throw new ParseException(String.format("Type %s of query does not support!", query.getClass().getName()));
        }
    }
}
