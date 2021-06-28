package io.arenadata.dtm.query.execution.core.dml.service.view;

import io.arenadata.dtm.common.exception.DeltaRangeInvalidException;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.calcite.core.util.CalciteUtil;
import io.arenadata.dtm.query.execution.core.base.service.delta.DeltaInformationExtractor;
import io.arenadata.dtm.query.execution.core.base.service.delta.DeltaInformationService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Slf4j
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
        val deltaInformation = deltaInformationExtractor.getDeltaInformation(context.getAllNodes(), context.getCurrentNode());
        switch (deltaInformation.getType()) {
            case DATETIME: {
                return handleDateTime(context, deltaInformation.getDeltaTimestamp());
            }
            case NUM: {
                if (deltaInformation.isLatestUncommittedDelta()) {
                    throw new DeltaRangeInvalidException("LATEST_UNCOMMITTED_DELTA is not supported for materialized views");
                }
                return handleDeltaNum(context, deltaInformation.getSelectOnNum());
            }
            case STARTED_IN:
            case FINISHED_IN: {
                return handleDeltaInterval(context.getEntity(), deltaInformation.getSelectOnInterval().getSelectOnTo());
            }
            case WITHOUT_SNAPSHOT:
            default: {
                return Future.succeededFuture();
            }
        }
    }

    private Future<Void> handleDateTime(ViewReplaceContext context, String deltaTimestamp) {
        return getDeltaByDateTime(context.getDatamart(), deltaTimestamp)
                .compose(deltaNum -> {
                    Long matViewDeltaNum = context.getEntity().getMaterializedDeltaNum();
                    if (materializedViewIsSync(matViewDeltaNum, deltaNum)) {
                        log.debug("Not replacing view, because delta from the request ({}) is not greater than delta of the view ({}).", deltaNum, matViewDeltaNum);
                        return Future.succeededFuture();
                    }

                    return replaceView(context);
                });
    }

    private Future<Long> getDeltaByDateTime(String datamart, String deltaTimestamp) {
        String deltaTime = deltaTimestamp.replace("\'", "");
        return deltaInformationService.getDeltaNumByDatetime(datamart, CalciteUtil.parseLocalDateTime(deltaTime));
    }

    private Future<Void> handleDeltaNum(ViewReplaceContext context, Long deltaNum) {
        Long matViewDeltaNum = context.getEntity().getMaterializedDeltaNum();
        if (materializedViewIsSync(matViewDeltaNum, deltaNum)) {
            log.debug("Not replacing view, because delta from the request ({}) is not greater than delta of the view ({}).", deltaNum, matViewDeltaNum);
            return Future.succeededFuture();
        }

        return replaceView(context);
    }

    private Future<Void> handleDeltaInterval(Entity matView, Long deltaTo) {
        Long materializedViewDeltaNum = matView.getMaterializedDeltaNum();
        if (!materializedViewIsSync(materializedViewDeltaNum, deltaTo)) {
            log.error("Range invalid. Delta_to ({}) is greater than materialized view's delta ({})",
                    deltaTo, materializedViewDeltaNum);
            throw new DeltaRangeInvalidException(String.format("Invalid delta range for materialized view: [mat_view delta: %d, delta_to: %d]",
                    materializedViewDeltaNum, deltaTo));
        }

        return Future.succeededFuture();
    }

    private boolean materializedViewIsSync(Long matViewDeltaNum, Long requestDeltaNum) {
        return matViewDeltaNum != null && matViewDeltaNum >= requestDeltaNum;
    }

    private Future<Void> replaceView(ViewReplaceContext context) {
        ViewReplacerService replacerService = context.getViewReplacerService();
        context.setViewQueryNode(definitionService.processingQuery(context.getEntity().getViewQuery()));
        return replacerService.replace(context);
    }

}

