package io.arenadata.dtm.query.calcite.core.service.impl;

import io.arenadata.dtm.query.calcite.core.dto.EnrichmentTemplateRequest;
import io.arenadata.dtm.query.calcite.core.dto.QueryTemplateResult;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.node.SqlTreeNode;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.calcite.core.service.QueryTemplateExtractor;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Iterator;
import java.util.List;
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
            throw new IllegalArgumentException("The number of passed parameters and parameters in the template does not match");
        } else {
            return selectTree.getRoot().getNode();
        }
    }

    @Override
    public QueryTemplateResult extract(SqlNode sqlNode) {
        SqlSelectTree selectTree = new SqlSelectTree(sqlNode);
        List<SqlTreeNode> paramNodes = selectTree.findNodesByPathRegex(REGEX);
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

}
