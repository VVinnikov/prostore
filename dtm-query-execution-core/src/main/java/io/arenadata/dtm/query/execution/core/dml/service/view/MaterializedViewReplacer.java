package io.arenadata.dtm.query.execution.core.dml.service.view;

import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.node.SqlTreeNode;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;

@Component("materializedViewReplacer")
public class MaterializedViewReplacer implements ViewReplacer {

    private final DefinitionService<SqlNode> definitionService;

    public MaterializedViewReplacer(@Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService) {
        this.definitionService = definitionService;
    }

    public Future<Void> replace(ViewReplaceContext context) {
        List<SqlTreeNode> nodes = context.getAllNodes().findNodesByPath(SqlSelectTree.SELECT_AS_SNAPSHOT);
        if (nodes.isEmpty()) {
            return Future.succeededFuture();
        }

        ViewReplacerService replacerService = context.getViewReplacerService();
        context.setViewQueryNode(definitionService.processingQuery(context.getEntity().getViewQuery()));
        return replacerService.replace(context);
    }

}

