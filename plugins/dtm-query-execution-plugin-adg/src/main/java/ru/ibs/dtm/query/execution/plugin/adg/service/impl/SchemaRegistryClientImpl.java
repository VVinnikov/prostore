package ru.ibs.dtm.query.execution.plugin.adg.service.impl;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.web.client.WebClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.plugin.adg.configuration.SchemaRegistryProperties;
import ru.ibs.dtm.query.execution.plugin.adg.model.schema.SchemaReq;
import ru.ibs.dtm.query.execution.plugin.adg.service.SchemaRegistryClient;

@Service
public class SchemaRegistryClientImpl implements SchemaRegistryClient {

  private SchemaRegistryProperties properties;
  private WebClient webClient;

  @Autowired
  public SchemaRegistryClientImpl(SchemaRegistryProperties properties, @Qualifier("adgWebClient") WebClient webClient) {
    this.properties = properties;
    this.webClient = webClient;
  }

  @Override
  public void register(String subject, SchemaReq schema, Handler<AsyncResult<Void>> handler) {
    webClient.postAbs(properties.getUrl() + "/subjects/" + subject + "/versions")
      .sendJson(schema, ar -> {
        if (ar.succeeded()) {
          try {
            if (ar.result().statusCode() == HttpResponseStatus.OK.code()) {
              handler.handle(Future.succeededFuture());
            } else {
              handler.handle(Future.failedFuture(ar.result().body().toString()));
            }
          } catch (Exception e) {
            handler.handle(Future.failedFuture(e));
          }
        } else {
          handler.handle(Future.failedFuture(ar.cause()));
        }
      });
  }

  @Override
  public void unregister(String subject, Handler<AsyncResult<Void>> handler) {
    webClient.deleteAbs(properties.getUrl() + "/subjects/" + subject)
      .send(ar -> {
        if (ar.succeeded()) {
          try {
            if (ar.result().statusCode() == HttpResponseStatus.OK.code()) {
              handler.handle(Future.succeededFuture());
            } else {
              handler.handle(Future.failedFuture(ar.result().body().toString()));
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
