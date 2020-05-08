package ru.ibs.dtm.query.execution.core.utils;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QuerySourceRequest;
import ru.ibs.dtm.common.reader.SourceType;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Определение типа запроса по хинту DATASOURCE_TYPE = ''
 */
@Component
public class HintExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(HintExtractor.class);
  private final Pattern HINT_PATTERN = Pattern.compile(
    "(.*)[\\s]+DATASOURCE_TYPE[\\s]*=[\\s]*([^\\s]+)",
    Pattern.CASE_INSENSITIVE);

  public void extractHint(QueryRequest request, Handler<AsyncResult<QuerySourceRequest>> resultHandler) {
    try {
      QuerySourceRequest sourceRequest = new QuerySourceRequest();
      Matcher matcher = HINT_PATTERN.matcher(request.getSql());
      if (matcher.find()) {
        String newSql = matcher.group(1);
        String dataSource = matcher.group(2);
        QueryRequest newQueryRequest = request.copy();
        newQueryRequest.setSql(newSql);
        sourceRequest.setSourceType(SourceType.valueOf(dataSource));
        sourceRequest.setQueryRequest(newQueryRequest);
      } else {
        LOGGER.info("Не определен хинт для запроса {}", request.getSql());
        sourceRequest.setQueryRequest(request);
      }
      resultHandler.handle(Future.succeededFuture(sourceRequest));
    } catch (Exception e) {
      resultHandler.handle(Future.failedFuture(e));
    }
  }
}
