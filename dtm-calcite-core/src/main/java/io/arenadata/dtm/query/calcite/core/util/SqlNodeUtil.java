package io.arenadata.dtm.query.calcite.core.util;

import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.dml.LimitableSqlOrderBy;
import io.arenadata.dtm.query.calcite.core.extension.parser.ParseException;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public final class SqlNodeUtil {
    private SqlNodeUtil() {
    }

    public static SqlNode copy(SqlNode sqlNode) {
        return new SqlSelectTree(sqlNode).copy().getRoot().getNode();
    }

    public static SqlNode checkViewQueryAndGet(SqlNode query) throws ParseException {
        if (query instanceof SqlSelect) {
            if (((SqlSelect) query).getFrom() == null) {
                throw new ParseException("View query must have from clause!");
            } else {
                return query;
            }
        } else if (query instanceof LimitableSqlOrderBy) {
            checkViewQueryAndGet(((LimitableSqlOrderBy) query).query);
            return query;
        } else {
            throw new ParseException(String.format("Type %s of query does not support!", query.getClass().getName()));
        }
    }

    public static Set<SourceType> extractSourceTypes(SqlNodeList sourceTypesSqlList) {
        return Optional.ofNullable(sourceTypesSqlList)
                .map(nodeList -> nodeList.getList().stream()
                        .map(node -> SourceType.valueOfAvailable(node.toString()))
                        .collect(Collectors.toSet()))
                .orElse(null);
    }

    public static SourceType extractSourceType(SqlNode destination) {
        return Optional.ofNullable(destination)
                .map(node -> SourceType.valueOfAvailable(node.toString().replace("'", "")))
                .orElse(null);
    }
}
