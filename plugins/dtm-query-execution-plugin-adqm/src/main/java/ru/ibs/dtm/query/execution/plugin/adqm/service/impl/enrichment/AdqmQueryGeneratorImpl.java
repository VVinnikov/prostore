package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.enrichment;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Util;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.CalciteContext;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.schema.dialect.DtmConvention;
import ru.ibs.dtm.query.execution.plugin.adqm.service.QueryExtendService;
import ru.ibs.dtm.query.execution.plugin.adqm.service.QueryGenerator;

@Service
@Slf4j
public class AdqmQueryGeneratorImpl implements QueryGenerator {

    private QueryExtendService queryExtendService;

    public AdqmQueryGeneratorImpl(QueryExtendService queryExtendService) {
        this.queryExtendService = queryExtendService;
    }

    @Override
    public void mutateQuery(RelRoot relNode,
                            Long selectOn,
                            CalciteContext calciteContext,
                            Handler<AsyncResult<String>> handler) {
        if (selectOn == null) {
            log.warn("Параметр selectOn = null использовано значение 0");
        }
        queryExtendService.setRequestBuilder(calciteContext.getRelBuilder(), true);
        queryExtendService.addOption(selectOn == null ? -1 : selectOn);
        try {
            RelNode extendedQuery = queryExtendService.extendQuery(relNode.rel);
            RelNode planAfter = calciteContext.getPlanner().transform(0, extendedQuery.getTraitSet().replace(EnumerableConvention.INSTANCE), extendedQuery);
            SqlNode sqlNodeResult = new RelToSqlConverter(DtmConvention.getDialect()).visitChild(0, planAfter).asStatement();
            String queryResult = Util.toLinux(sqlNodeResult.toSqlString(DtmConvention.getDialect()).getSql()).replaceAll("\n", " ");
            log.debug("sql = " + queryResult);
            handler.handle(Future.succeededFuture(queryResult));
        } catch (Exception e) {
            log.error("Ошибка исполнения преобразования", e);
            handler.handle(Future.failedFuture(e));
        }
    }
}
