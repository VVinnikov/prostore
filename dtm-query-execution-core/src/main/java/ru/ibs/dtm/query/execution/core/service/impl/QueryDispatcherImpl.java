package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.dto.ParsedQueryRequest;
import ru.ibs.dtm.query.execution.core.service.QueryDispatcher;
import ru.ibs.dtm.query.execution.core.service.QueryExecuteService;
import ru.ibs.dtm.query.execution.core.service.SqlProcessingType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class QueryDispatcherImpl implements QueryDispatcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryDispatcherImpl.class);

  private final Map<SqlProcessingType, QueryExecuteService> queryExecuteServices;

  @Autowired
  public QueryDispatcherImpl(List<QueryExecuteService> queryExecuteServices) {
    this.queryExecuteServices = queryExecuteServices.stream()
      .collect(Collectors.toMap(QueryExecuteService::getSqlProcessingType, it -> it));
  }

  @Override
  public void dispatch(ParsedQueryRequest parsedQueryRequest, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
    try {
      queryExecuteServices.get(parsedQueryRequest.getProcessingType())
        .execute(parsedQueryRequest, asyncResultHandler);
    } catch (Exception e) {
      LOGGER.error("Произошла ошибка при диспетчеризации запроса", e);
      asyncResultHandler.handle(Future.failedFuture(e));
    }
  }
}
