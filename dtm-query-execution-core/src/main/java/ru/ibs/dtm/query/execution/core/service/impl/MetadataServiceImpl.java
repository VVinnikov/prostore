package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.service.MetadataService;
import ru.ibs.dtm.query.execution.core.utils.MetaDataQueryPreparer;
import ru.ibs.dtm.query.execution.core.utils.SqlPreparer;

@Service
public class MetadataServiceImpl implements MetadataService {

  private final ServiceDao serviceDao;

  @Autowired
  public MetadataServiceImpl(ServiceDao serviceDao) {
    this.serviceDao = serviceDao;
  }

  public void executeQuery(QueryRequest request, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
    serviceDao.executeQuery(
      SqlPreparer.replaceQuote(MetaDataQueryPreparer.modify(request.getSql())), ar -> {
      if (ar.succeeded()) {
        QueryResult result = new QueryResult(request.getRequestId(),
          new JsonArray(ar.result().getRows()));
        asyncResultHandler.handle(Future.succeededFuture(result));
      } else {
        asyncResultHandler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }
}
