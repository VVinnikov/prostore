package io.arenadata.dtm.query.execution.core.dml.service.view;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.node.SqlTreeNode;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.calcite.core.util.CalciteUtil;
import io.arenadata.dtm.query.execution.core.base.service.delta.DeltaInformationExtractor;
import io.arenadata.dtm.query.execution.core.base.service.delta.DeltaInformationService;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;

@Component("materializedViewReplacer")
public class MaterializedViewReplacer implements ViewReplacer {

    private final DefinitionService<SqlNode> definitionService;
    private final DeltaInformationExtractor deltaInformationExtractor;
    private final DeltaInformationService deltaInformationService;

    public MaterializedViewReplacer(@Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
                                    DeltaInformationExtractor deltaInformationExtractor,
                                    DeltaInformationService deltaInformationService) {
        this.definitionService = definitionService;
        this.deltaInformationExtractor = deltaInformationExtractor;
        this.deltaInformationService = deltaInformationService;
    }

    public Future<Void> replace(ViewReplaceContext context) {
        SqlSelectTree tree = new SqlSelectTree(context.getCurrentNode().getNode());
        List<SqlTreeNode> snapshots = tree.findNodesByPath(SqlSelectTree.SELECT_AS_SNAPSHOT);
        if (snapshots.isEmpty()) {
            return Future.succeededFuture();
        }

        DeltaInformation deltaInformation = deltaInformationExtractor.getDeltaInformation(context.getAllNodes(), context.getCurrentNode());
        switch (deltaInformation.getType()) {
            case DATETIME: {
                return getDeltaByDateTime(context.getDatamart(), deltaInformation.getDeltaTimestamp()).onSuccess(deltaNum -> {
                    Long matViewDeltaNum = context.getEntity().getMaterializedDeltaNum();
                    if (matViewDeltaNum != null && deltaNum <= matViewDeltaNum) {
                        return;
                    }

                    ViewReplacerService replacerService = context.getViewReplacerService();
                    context.setViewQueryNode(definitionService.processingQuery(context.getEntity().getViewQuery()));
                    replacerService.replace(context);
                }).mapEmpty();
            }
        }

        return Future.succeededFuture();
    }

    private Future<Long> getDeltaByDateTime(String datamart, String deltaTimestamp) {
        String deltaTime = deltaTimestamp.replace("\'", "");
        return deltaInformationService.getCnToByDeltaDatetime(datamart, CalciteUtil.parseLocalDateTime(deltaTime));
    }

}

