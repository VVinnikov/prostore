package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.node.SqlTreeNode;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.service.dml.LogicViewReplacer;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class LogicViewReplacerImpl implements LogicViewReplacer {
    private static final SqlDialect SQL_DIALECT = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
    private final DefinitionService<SqlNode> definitionService;
    private final EntityDao entityDao;

    public LogicViewReplacerImpl(
        @Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
        EntityDao entityDao) {
        this.definitionService = definitionService;
        this.entityDao = entityDao;
    }

    @SneakyThrows
    @Override
    public void replace(String sql,
                        String datamart,
                        Handler<AsyncResult<String>> resultHandler) {
        log.debug("before replacing:\n{}", sql);
        SqlNode rootSqlNode = definitionService.processingQuery(sql);
        replace(rootSqlNode,
            datamart,
            null,
            null,
            null,
            null)
            .onSuccess(v -> {
                String replacedSql = rootSqlNode.toSqlString(SQL_DIALECT).getSql();
                log.debug("after replacing: [{}]", replacedSql);
                resultHandler.handle(Future.succeededFuture(replacedSql));
            })
            .onFailure(error -> resultHandler.handle(Future.failedFuture(error)));
    }

    private Future<Void> replace(SqlNode viewQueryNode,
                                 String datamart,
                                 SqlSelectTree parentTree,
                                 SqlTreeNode parentNode,
                                 SqlSnapshot sqlSnapshot,
                                 Entity view) {
        SqlSelectTree tree = new SqlSelectTree(viewQueryNode);
        List<SqlTreeNode> allTableAndSnapshots = tree.findAllTableAndSnapshots();
        return CompositeFuture.join(allTableAndSnapshots.stream()
            .map(childNode -> entityDao.getEntity(
                childNode.tryGetSchemaName().orElse(datamart),
                childNode.tryGetTableName().orElseThrow(() -> new RuntimeException("Can't get tableName"))
                ).compose(entity -> {
                    SqlSnapshot currSnapshot = getSqlSnapshot(parentNode, sqlSnapshot);
                    if (entity.getEntityType() == EntityType.VIEW) {
                        return replace(definitionService.processingQuery(entity.getViewQuery()),
                            datamart,
                            tree,
                            childNode,
                            currSnapshot,
                            entity);
                    } else {
                        if (currSnapshot != null) {
                            SqlSnapshot parentSnapshot = parentNode.getNode();
                            SqlSnapshot childSnapshot = new SqlSnapshot(SqlParserPos.QUOTED_ZERO,
                                childNode.getNode(),
                                parentSnapshot.getPeriod());
                            childNode.getSqlNodeSetter().accept(childSnapshot);
                        }
                        return Future.succeededFuture();
                    }
                })
            )
            .collect(Collectors.toList()))
            .onSuccess(cf -> {
                if (parentNode != null && view != null) {
                    if (isAliasExists(parentTree, parentNode)) {
                        parentNode.getSqlNodeSetter().accept(tree.getRoot().getNode());
                    } else {
                        SqlBasicCall alias = getAlias(parentNode, tree);
                        parentNode.getSqlNodeSetter().accept(alias);
                    }
                }
            })
            .mapEmpty();
    }

    private SqlSnapshot getSqlSnapshot(SqlTreeNode parentNode, SqlSnapshot sqlSnapshot) {
        if (parentNode == null) {
            return null;
        } else {
            return sqlSnapshot == null ?
                (parentNode.getNode() instanceof SqlSnapshot ? parentNode.getNode() : null)
                : sqlSnapshot;
        }
    }

    private SqlBasicCall getAlias(SqlTreeNode parentNode, SqlSelectTree tree) {
        String tableName = parentNode.tryGetTableName()
            .orElseThrow(() -> new RuntimeException("Can't get tableName"));
        return new SqlBasicCall(new SqlAsOperator(),
            new SqlNode[]{tree.getRoot().getNode(),
                new SqlIdentifier(tableName, SqlParserPos.QUOTED_ZERO)},
            SqlParserPos.QUOTED_ZERO);
    }

    private boolean isAliasExists(SqlSelectTree tree, SqlTreeNode node) {
        return tree.getParentByChild(node)
            .filter(parentNode -> parentNode.getKindPath().endsWith(".AS"))
            .isPresent();
    }

}


