package io.arenadata.dtm.query.execution.plugin.adb.enrichment.service;

import io.arenadata.dtm.common.calcite.CalciteContext;
import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.calcite.core.rel2sql.NullNotCastableRelToSqlConverter;
import io.arenadata.dtm.query.calcite.core.util.RelNodeUtil;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.dto.QueryGeneratorContext;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryExtendService;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryGenerator;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.util.Util;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;

@Service("adbQueryGenerator")
@Slf4j
public class AdbQueryGenerator implements QueryGenerator {

    private final QueryExtendService queryExtendService;
    private final SqlDialect sqlDialect;

    @Autowired
    public AdbQueryGenerator(@Qualifier("adbDmlExtendService") QueryExtendService queryExtendService,
                             @Qualifier("adbSqlDialect") SqlDialect sqlDialect) {
        this.queryExtendService = queryExtendService;
        this.sqlDialect = sqlDialect;
    }

    @Override
    public Future<String> mutateQuery(RelRoot relNode,
                                      List<DeltaInformation> deltaInformations,
                                      CalciteContext calciteContext,
                                      EnrichQueryRequest enrichQueryRequest) {
        if (deltaInformations.isEmpty()) {
            log.warn("Deltas list cannot be empty");
        }
        return Future.future(promise -> {
            val generatorContext = getContext(relNode, deltaInformations, calciteContext);
            val extendedQuery = queryExtendService.extendQuery(generatorContext);
            RelNode resultRelNode = null;
            if (RelNodeUtil.isNeedToTrimSortColumns(relNode, extendedQuery)) {
                resultRelNode = RelNodeUtil.trimUnusedSortColumn(calciteContext.getRelBuilder(),
                        extendedQuery,
                        relNode.validatedRowType);
            } else {
                try {
                    resultRelNode = calciteContext.getPlanner()
                            .transform(0, extendedQuery.getTraitSet().replace(EnumerableConvention.INSTANCE),
                                    extendedQuery);
                } catch (Exception e) {
                    promise.fail(new DtmException("Error in converting rel node", e));
                }
            }
            val sqlNodeResult = new NullNotCastableRelToSqlConverter(sqlDialect).visitChild(0, resultRelNode).asStatement();
            val queryResult = Util.toLinux(sqlNodeResult.toSqlString(sqlDialect).getSql()).replaceAll("\n", " ");
            log.debug("sql = " + queryResult);
            promise.complete(queryResult);
        });
    }

    private QueryGeneratorContext getContext(RelRoot relNode,
                                             List<DeltaInformation> deltaInformations,
                                             CalciteContext calciteContext) {
        return new QueryGeneratorContext(
                deltaInformations.iterator(),
                calciteContext.getRelBuilder(),
                relNode,
                true);
    }
}
