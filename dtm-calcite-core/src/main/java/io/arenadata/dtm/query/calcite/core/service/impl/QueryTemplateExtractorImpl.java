package io.arenadata.dtm.query.calcite.core.service.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.calcite.core.dto.EnrichmentTemplateRequest;
import io.arenadata.dtm.common.reader.QueryTemplateResult;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.node.SqlTreeNode;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.calcite.core.service.QueryTemplateExtractor;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class QueryTemplateExtractorImpl implements QueryTemplateExtractor {
    private static final SqlDynamicParam DYNAMIC_PARAM = new SqlDynamicParam(0, SqlParserPos.QUOTED_ZERO);
    private static final String REGEX = "(?i).*(LIKE\\[1\\]|EQUAL\\w*\\[1\\]|LESS\\w*\\[1\\]|GREATER\\w*\\[1\\]).*";
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
    public SqlNode enrichTemplate(EnrichmentTemplateRequest request) {
        SqlSelectTree selectTree = new SqlSelectTree(definitionService.processingQuery(request.getTemplate()));
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
        List<SqlTreeNode> paramNodes = getTreeNodes(excludeList, selectTree);
        paramNodes.forEach(node -> node.getSqlNodeSetter().accept(DYNAMIC_PARAM));
        return new QueryTemplateResult(
                selectTree.getRoot()
                        .getNode()
                        .toSqlString(sqlDialect).toString(),
                paramNodes.stream()
                        .map(node -> (SqlNode) node.getNode())
                        .collect(Collectors.toList())
        );
    }

    private List<SqlTreeNode> getTreeNodes(List<String> excludeList, SqlSelectTree selectTree) {
        if (excludeList.isEmpty()) {
            return selectTree.findNodesByPathRegex(REGEX);
        } else {
            return selectTree.findNodesByPathRegex(REGEX)
                    .stream()
                    .filter(n -> {
                        final String column = getConditionColumnName(selectTree, n);
                        return excludeList.stream().noneMatch(column::equalsIgnoreCase);
                    })
                    .collect(Collectors.toList());
        }
    }

    private String getConditionColumnName(SqlSelectTree selectTree, SqlTreeNode n) {
        final Optional<SqlTreeNode> parentNode = selectTree.getParentByChild(n);
        if (parentNode.isPresent()) {
            return ((SqlIdentifier) ((SqlBasicCall) parentNode.get().getNode()).getOperandList().get(0)).names.get(0);
        } else {
            throw new DtmException("Can't extract condition column name from sql node");
        }
    }

}
