package ru.ibs.dtm.query.execution.core.utils;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.*;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Slf4j
@Component
public class DatamartMnemonicExtractor {

    public Optional<String> extract(SqlNode sqlNode) {
        val datamartOpt = extractInner(sqlNode);
        log.debug("Extracted datamart [{}] from sql [{}]", datamartOpt.orElse(""), sqlNode);
        return datamartOpt;
    }

    private Optional<String> extractInner(SqlNode sqlNode) {
        if (sqlNode instanceof SqlSelect) {
            return findDatamart(sqlNode);
        } else if (sqlNode instanceof SqlDdl || sqlNode instanceof SqlAlter) {
            return findDatamart(readTableNode(sqlNode, "name"));
        } else if (sqlNode instanceof SqlInsert || sqlNode instanceof SqlUpdate) {
            return findDatamart(readTableNode(sqlNode, "targetTable"));
        } else {
            log.info("Sql type not defined by: {}", sqlNode);
            return Optional.empty();
        }
    }

    @SneakyThrows
    private SqlNode readTableNode(SqlNode o, String fieldName) {
        return (SqlNode) FieldUtils.readField(o, fieldName, true);
    }

    private Optional<String> findDatamart(SqlNode node) {
        if (node instanceof SqlSelect) {
            return findDatamart(((SqlSelect) node).getFrom());
        } else if (node instanceof SqlIdentifier || node instanceof SqlSnapshot) {
            return Optional.ofNullable(getDatamart(node));
        } else if (node instanceof SqlJoin) {
            return processSqlJoin((SqlJoin) node);
        } else if (node instanceof SqlBasicCall) {
            return processSqlBasicCall((SqlBasicCall) node);
        } else {
            return Optional.empty();
        }
    }

    private Optional<String> processSqlBasicCall(SqlBasicCall basicCall) {
        for (int i = 0; i < basicCall.getOperands().length; i++) {
            val optional = findDatamart(basicCall.operand(i));
            if (optional.isPresent()) return optional;
        }
        return Optional.empty();
    }

    private Optional<String> processSqlJoin(SqlJoin join) {
        Optional<String> datamartOpt = findDatamart(join.getLeft());
        return datamartOpt.isPresent() ? datamartOpt : findDatamart(join.getRight());
    }

    private String getDatamart(SqlNode sqlNode) {
        if (sqlNode instanceof SqlIdentifier) {
            val identifier = (SqlIdentifier) sqlNode;
            return identifier.names.size() > 1 ? identifier.names.get(0).toLowerCase() : null;
        } else if (sqlNode instanceof SqlSnapshot) {
            return getDatamart(((SqlSnapshot) sqlNode).getTableRef());
        } else throw new IllegalArgumentException("Node required instance of SqlIdentifier or SqlSnapshot");
    }

}
