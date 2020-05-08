package ru.ibs.dtm.query.execution.plugin.adg.service.impl.mppr;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.plugin.exload.QueryExloadParam;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adg.dto.EnrichQueryRequest;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryEnrichmentService;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtCartridgeClient;
import ru.ibs.dtm.query.execution.plugin.api.dto.MpprKafkaRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.MpprKafkaService;

import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@Service("adgMpprKafkaService")
public class AdgMpprKafkaService implements MpprKafkaService {
  private final QueryEnrichmentService adbQueryEnrichmentService;
  private final TtCartridgeClient ttCartridgeClient;

  @Override
  public void execute(MpprKafkaRequest queryRequest, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
    EnrichQueryRequest enrichQueryRequest = EnrichQueryRequest.generate(queryRequest.getQueryRequest(), queryRequest.getSchema());
    adbQueryEnrichmentService.enrich(enrichQueryRequest, sqlResult -> {
      if (sqlResult.succeeded()) {
        uploadData(queryRequest, asyncResultHandler, sqlResult.result());
      } else {
        log.error("Ошибка при обогащении запроса");
        asyncResultHandler.handle(Future.failedFuture(sqlResult.cause()));
      }
    });
  }

  private void uploadData(MpprKafkaRequest queryRequest,
                          Handler<AsyncResult<QueryResult>> asyncResultHandler,
                          String sql) {
    QueryExloadParam queryExloadParam = queryRequest.getQueryExloadParam();
    ttCartridgeClient.uploadData(sql, queryRequest.getTopic(), queryExloadParam.getChunkSize(), ar -> {
        UUID requestId = queryRequest.getQueryRequest().getRequestId();
        if (ar.succeeded()) {
          log.info("Выгрузка данных из ADG прошла успешно по запросу: {}", requestId);
          asyncResultHandler.handle(Future.succeededFuture(QueryResult.emptyResult()));
        } else {
          String errMsg = String.format("Ошибка выгрузки данных из ADG: %s по запросу %s",
            ar.cause().getMessage(),
            requestId);
          log.error(errMsg);
          asyncResultHandler.handle(Future.failedFuture(new RuntimeException(errMsg, ar.cause())));
        }
      }
    );
  }
}
