package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.enrichment;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.CalciteContext;
import ru.ibs.dtm.query.execution.plugin.adqm.service.QueryParserService;

@Service
@Slf4j
public class AdqmCalciteDMLQueryParserServiceImpl implements QueryParserService {
    private Vertx vertx;

    @Autowired
    public AdqmCalciteDMLQueryParserServiceImpl(@Qualifier("adqmVertx") Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public void parse(QueryRequest querySourceRequest, CalciteContext context, Handler<AsyncResult<RelRoot>> asyncResultHandler) {
        vertx.executeBlocking(it -> {
            try {
                //TODO: доделать получения схемы
                if (querySourceRequest == null || StringUtils.isEmpty(querySourceRequest.getSql())) {
                    log.error("Неопределен запрос {}", querySourceRequest);
                    asyncResultHandler.handle(Future.failedFuture(String.format("Неопределен запрос %s", querySourceRequest)));
                    return;
                }
                String sqlRequest = querySourceRequest.getSql();
                RelRoot relQuery;
                try {
                    SqlNode parse = context.getPlanner().parse(sqlRequest);
                    SqlNode validatedQuery = context.getPlanner().validate(parse);
                    relQuery = context.getPlanner().rel(validatedQuery);
                } catch (Exception e) {
                    log.error("Ошибка разбора запроса", e);
                    it.fail(e);
                    return;
                }
                it.complete(relQuery);
            } catch (Exception e) {
                log.error("Ошибка парсинга запроса", e);
                it.fail(e);
            }
        }, ar -> {
            if (ar.succeeded()) {
                asyncResultHandler.handle(Future.succeededFuture((RelRoot) ar.result()));
            } else {
                log.debug("Ошибка при исполнении метода parse", ar.cause());
                asyncResultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }
}
