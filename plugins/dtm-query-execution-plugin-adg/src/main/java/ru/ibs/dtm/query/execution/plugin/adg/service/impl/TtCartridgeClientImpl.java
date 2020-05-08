package ru.ibs.dtm.query.execution.plugin.adg.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import ru.ibs.dtm.query.execution.plugin.adg.configuration.TarantoolCartridgeProperties;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.OperationFile;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.request.*;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.response.ResOperation;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.response.ResStatus;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.response.ResStatusEnum;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtCartridgeClient;

import java.util.HashMap;
import java.util.List;

@Slf4j
@Service
public class TtCartridgeClientImpl implements TtCartridgeClient {

  private final WebClient webClient;
  private final TarantoolCartridgeProperties cartridgeProperties;

  @Autowired
  public TtCartridgeClientImpl(TarantoolCartridgeProperties cartridgeProperties, @Qualifier("adgWebClient") WebClient webClient, ObjectMapper objectMapper) {
    this.cartridgeProperties = cartridgeProperties;
    this.webClient = webClient;
  }

  @Override
  public void getFiles(Handler<AsyncResult<ResOperation>> handler) {
    executePostRequest(new GetFilesOperation(), handler);
  }

  @Override
  public void setFiles(List<OperationFile> files, Handler<AsyncResult<ResOperation>> handler) {
    executePostRequest(new SetFilesOperation(files), handler);
  }

  @Override
  public void getSchema(Handler<AsyncResult<ResOperation>> handler) {
    executePostRequest(new GetSchemaOperation(), handler);
  }

  @Override
  public void setSchema(String yaml, Handler<AsyncResult<ResOperation>> handler) {
    executePostRequest(new SetSchemaOperation(yaml), handler);
  }

  @Override
  public void uploadData(String sql, String topicName, int batchSize, Handler<AsyncResult<ResStatus>> handler) {
    val queryParamMap = new HashMap<String, String>();
    queryParamMap.put("_batch_size", String.valueOf(batchSize));
    queryParamMap.put("_topic", topicName);
    queryParamMap.put("_query", sql);
    executeGetRequest(new GetRequest(cartridgeProperties.getSendQueryUrl(), queryParamMap), handler);
  }

  @SneakyThrows
  private void executeGetRequest(GetRequest request, Handler<AsyncResult<ResStatus>> handler) {
    val httpRequest = webClient.getAbs(cartridgeProperties.getUrl() + request.getRequestUri());
    request.getQueryParamMap().forEach(httpRequest::addQueryParam);
    httpRequest.send(ar -> {
      if (ar.succeeded()) {
        try {
          val res = new JsonObject(ar.result().body()).mapTo(ResStatus.class);
          if (ResStatusEnum.ok == res.getStatus()) {
            handler.handle(Future.succeededFuture(res));
          } else {
            handler.handle(Future.failedFuture(String.format("%s: %s", res.getErrorCode(), res.getError())));
          }
        } catch (Exception e) {
          handler.handle(Future.failedFuture(e));
        }
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  @SneakyThrows
  private void executePostRequest(ReqOperation reqOperation, Handler<AsyncResult<ResOperation>> handler) {
    webClient.postAbs(cartridgeProperties.getUrl() + cartridgeProperties.getAdminApiUrl())
      .sendJson(reqOperation, ar -> {
        if (ar.succeeded()) {
          try {
            ResOperation res = new JsonObject(ar.result().body()).mapTo(ResOperation.class);
            if (CollectionUtils.isEmpty(res.getErrors())) {
              handler.handle(Future.succeededFuture(res));
            } else {
              handler.handle(Future.failedFuture(res.getErrors().get(0).getMessage()));
            }
          } catch (Exception e) {
            handler.handle(Future.failedFuture(e));
          }
        } else {
          handler.handle(Future.failedFuture(ar.cause()));
        }
      });
  }
}
