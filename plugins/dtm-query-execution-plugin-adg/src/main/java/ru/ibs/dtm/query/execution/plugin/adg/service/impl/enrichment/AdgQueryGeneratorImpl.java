package ru.ibs.dtm.query.execution.plugin.adg.service.impl.enrichment;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Util;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.calcite.CalciteContext;
import ru.ibs.dtm.common.delta.DeltaInformation;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.adg.dto.QueryGeneratorContext;
import ru.ibs.dtm.query.execution.plugin.adg.dto.schema.SchemaDescription;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryExtendService;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryGenerator;

import java.util.List;

@Service("adgQueryGenerator")
@Slf4j
public class AdgQueryGeneratorImpl implements QueryGenerator {
    private final QueryExtendService queryExtendService;
    private final SqlDialect sqlDialect;

    public AdgQueryGeneratorImpl(@Qualifier("adgCalciteDmlQueryExtendService") QueryExtendService queryExtendService,
                                 @Qualifier("adgSqlDialect") SqlDialect sqlDialect) {
        this.queryExtendService = queryExtendService;
        this.sqlDialect = sqlDialect;
    }

    @Override
    public void mutateQuery(RelRoot relNode,
                            List<DeltaInformation> deltaInformations,
                            SchemaDescription schemaDescription,
                            CalciteContext calciteContext,
                            QueryRequest queryRequest,
                            Handler<AsyncResult<String>> handler) {
        if (schemaDescription.getLogicalSchema() == null) {
            handler.handle(Future.failedFuture(String.format("Error defining schema for request %s", relNode.toString())));
            return;
        }
        if (deltaInformations.isEmpty()) {
            log.warn("Deltas list cannot be empty");
        }
        try {
            val generatorContext = getContext(relNode, deltaInformations, calciteContext, queryRequest);
            val extendedQuery = queryExtendService.extendQuery(generatorContext);
            RelNode planAfter = calciteContext.getPlanner().transform(0, extendedQuery.getTraitSet().replace(EnumerableConvention.INSTANCE), extendedQuery);
            SqlNode sqlNodeResult = new RelToSqlConverter(sqlDialect).visitChild(0, planAfter).asStatement();
            String queryResult = Util.toLinux(sqlNodeResult.toSqlString(sqlDialect).getSql()).replaceAll("\n", " ");
            log.debug("sql = " + queryResult);
            handler.handle(Future.succeededFuture(queryResult));
        } catch (Exception e) {
            log.error("Request conversion execution error", e);
            handler.handle(Future.failedFuture(e));
        }
    }

    private QueryGeneratorContext getContext(RelRoot relNode,
                                             List<DeltaInformation> deltaInformations,
                                             CalciteContext calciteContext,
                                             QueryRequest queryRequest) {
        return new QueryGeneratorContext(
                deltaInformations.iterator(),
                queryRequest,
                calciteContext.getRelBuilder(),
                true,
                relNode);
    }
}
