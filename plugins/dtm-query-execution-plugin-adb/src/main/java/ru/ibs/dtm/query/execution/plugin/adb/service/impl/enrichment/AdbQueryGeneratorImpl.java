package ru.ibs.dtm.query.execution.plugin.adb.service.impl.enrichment;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.util.Util;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.calcite.CalciteContext;
import ru.ibs.dtm.common.delta.DeltaInformation;
import ru.ibs.dtm.query.execution.plugin.adb.calcite.schema.dialect.AdbDtmConvention;
import ru.ibs.dtm.query.execution.plugin.adb.dto.QueryGeneratorContext;
import ru.ibs.dtm.query.execution.plugin.adb.service.QueryExtendService;
import ru.ibs.dtm.query.execution.plugin.adb.service.QueryGenerator;

@Service
@Slf4j
public class AdbQueryGeneratorImpl implements QueryGenerator {

    private final QueryExtendService queryExtendService;

    public AdbQueryGeneratorImpl(QueryExtendService queryExtendService) {
        this.queryExtendService = queryExtendService;
    }

    @Override
    public void mutateQuery(RelRoot relNode,
                            List<DeltaInformation> deltaInformations,
                            CalciteContext calciteContext,
                            Handler<AsyncResult<String>> handler) {
        if (deltaInformations.isEmpty()) {
            log.warn("Deltas list cannot be empty");
        }
        try {
            val generatorContext = getContext(relNode, deltaInformations, calciteContext);
            val extendedQuery = queryExtendService.extendQuery(generatorContext);
            val planAfter = calciteContext.getPlanner().transform(0,
                    extendedQuery.getTraitSet().replace(EnumerableConvention.INSTANCE),
                    extendedQuery);
            val sqlNodeResult = new RelToSqlConverter(AdbDtmConvention.getDialect()).visitChild(0, planAfter).asStatement();
            val queryResult = Util.toLinux(sqlNodeResult.toSqlString(AdbDtmConvention.getDialect()).getSql()).replaceAll("\n", " ");
            log.debug("sql = " + queryResult);
            handler.handle(Future.succeededFuture(queryResult));
        } catch (Exception e) {
            log.error("Ошибка исполнения преобразования", e);
            handler.handle(Future.failedFuture(e));
        }
    }

    private QueryGeneratorContext getContext(RelRoot relNode,
                                             List<DeltaInformation> deltaInformations,
                                             CalciteContext calciteContext) {
        return new QueryGeneratorContext(deltaInformations.iterator(),
                calciteContext.getRelBuilder(),
                true,
                relNode);
    }
}
