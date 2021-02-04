package io.arenadata.dtm.query.calcite.core.service.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.QueryTemplateResult;
import io.arenadata.dtm.query.calcite.core.dto.EnrichmentTemplateRequest;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.node.SqlTreeNode;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.calcite.core.service.QueryTemplateExtractor;
import io.arenadata.dtm.query.calcite.core.util.SqlNodeUtil;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class QueryTemplateExtractorImpl implements QueryTemplateExtractor {
    private static final SqlDynamicParam DYNAMIC_PARAM = new SqlDynamicParam(0, SqlParserPos.QUOTED_ZERO);
    private static final String REGEX = "(?i).*(LIKE|EQUAL\\w*|LESS\\w*|GREATER\\w*).*";
    private static final String DYNAMIC_PARAM_PATH = "[1].DYNAMIC_PARAM";
    private final DefinitionService<SqlNode> definitionService;
    private final SqlDialect sqlDialect;

    public QueryTemplateExtractorImpl(DefinitionService<SqlNode> definitionService, SqlDialect sqlDialect) {
        this.definitionService = definitionService;
        this.sqlDialect = sqlDialect;
    }

    @Override
    public QueryTemplateResult extract(String sql) {
        return extract(definitionService.processingQuery(sql));
    }

    @Override
    public QueryTemplateResult extract(String sql, List<String> excludeColumns) {
        return extract(definitionService.processingQuery(sql), excludeColumns);
    }

    @Override
    public SqlNode enrichTemplate(EnrichmentTemplateRequest request) {

        SqlSelectTree selectTree = new SqlSelectTree(SqlNodeUtil.copy(request.getTemplateNode()));
        List<SqlTreeNode> dynamicNodes = selectTree.findNodesByPath(DYNAMIC_PARAM_PATH);
        Iterator<SqlNode> paramIterator = request.getParams().iterator();
        for (SqlTreeNode dynamicNode : dynamicNodes) {
            SqlNode param;
            if (!paramIterator.hasNext()) {
                paramIterator = request.getParams().iterator();
            }
            param = paramIterator.next();
            dynamicNode.getSqlNodeSetter().accept(param);
        }
        if (paramIterator.hasNext()) {
            throw new DtmException("The number of passed parameters and parameters in the template does not match");
        } else {
            return selectTree.getRoot().getNode();
        }
    }

    @Override
    public QueryTemplateResult extract(SqlNode sqlNode) {
        return extractTemplate(sqlNode, Collections.emptyList());
    }

    @Override
    public QueryTemplateResult extract(SqlNode sqlNode, List<String> excludeColumns) {
        return extractTemplate(sqlNode, excludeColumns);
    }

    private QueryTemplateResult extractTemplate(SqlNode sqlNode, List<String> excludeList) {
        SqlSelectTree selectTree = new SqlSelectTree(sqlNode);
        List<SqlNode> params = setDynamicParams(excludeList, selectTree);
        SqlNode resultTemplateNode = selectTree.getRoot().getNode();
        return new QueryTemplateResult(
                resultTemplateNode
                        .toSqlString(sqlDialect).toString(),
                resultTemplateNode,
                params
        );
    }

    private List<SqlNode> setDynamicParams(List<String> excludeList, SqlSelectTree selectTree) {
        if (excludeList.isEmpty()) {
            return selectTree.findNodesByPathRegex(REGEX).stream()
                    .map(this::replace)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());
        } else {
            return selectTree.findNodesByPathRegex(REGEX).stream()
                    .map(sqlTreeNode -> replaceWithExclude(sqlTreeNode, excludeList))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());
        }
    }

    private Optional<SqlNode> replace(SqlTreeNode sqlTreeNode) {
        SqlBasicCall sqlBasicCall = sqlTreeNode.getNode();
        if (sqlBasicCall.getOperands().length == 2) {
            SqlNode leftOperand = sqlBasicCall.getOperands()[0];
            SqlNode rightOperand = sqlBasicCall.getOperands()[1];
            boolean leftIsIdentifier = leftOperand instanceof SqlIdentifier;
            boolean rightIsIdentifier = rightOperand instanceof SqlIdentifier;
            if (leftIsIdentifier && !rightIsIdentifier) {
                sqlTreeNode.getSqlNodeSetter().accept(new SqlBasicCall(
                        sqlBasicCall.getOperator(),
                        new SqlNode[]{leftOperand, DYNAMIC_PARAM},
                        sqlBasicCall.getParserPosition()
                ));
                return Optional.of(rightOperand);
            } else if (!leftIsIdentifier && rightIsIdentifier) {
                sqlTreeNode.getSqlNodeSetter().accept(new SqlBasicCall(
                        sqlBasicCall.getOperator(),
                        new SqlNode[]{DYNAMIC_PARAM, rightOperand},
                        sqlBasicCall.getParserPosition()
                ));
                return Optional.of(leftOperand);
            }
        }
        return Optional.empty();
    }

    private Optional<SqlNode> replaceWithExclude(SqlTreeNode sqlTreeNode, List<String> excludeList) {
        SqlBasicCall sqlBasicCall = sqlTreeNode.getNode();
        if (sqlBasicCall.getOperands().length == 2) {
            SqlNode leftOperand = sqlBasicCall.getOperands()[0];
            SqlNode rightOperand = sqlBasicCall.getOperands()[1];
            boolean leftIsLiteral = leftOperand instanceof SqlLiteral;
            boolean rightIsLiteral = rightOperand instanceof SqlLiteral;
            if (leftIsLiteral && !rightIsLiteral && isNotExclude(rightOperand, excludeList)) {
                sqlTreeNode.getSqlNodeSetter().accept(new SqlBasicCall(
                        sqlBasicCall.getOperator(),
                        new SqlNode[]{DYNAMIC_PARAM, rightOperand},
                        sqlBasicCall.getParserPosition()
                ));
                return Optional.of(rightOperand);
            } else if (!leftIsLiteral && rightIsLiteral && isNotExclude(leftOperand, excludeList)) {
                sqlTreeNode.getSqlNodeSetter().accept(new SqlBasicCall(
                        sqlBasicCall.getOperator(),
                        new SqlNode[]{leftOperand, DYNAMIC_PARAM},
                        sqlBasicCall.getParserPosition()
                ));
                return Optional.of(leftOperand);
            }
        }
        return Optional.empty();
    }

    private boolean isNotExclude(SqlNode operand, List<String> excludeList) {
        if (excludeList.isEmpty()) {
            return true;
        } else if (operand instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier) operand;
            String columnName = identifier.getSimple();
            return excludeList.stream()
                    .noneMatch(e -> e.equalsIgnoreCase(columnName));
        } else {
            return true;
        }
    }

}
