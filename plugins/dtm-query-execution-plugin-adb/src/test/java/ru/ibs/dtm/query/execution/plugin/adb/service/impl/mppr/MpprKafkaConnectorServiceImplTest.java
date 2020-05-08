package ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppr;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.buffer.impl.BufferImpl;
import io.vertx.core.http.HttpVersion;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.impl.HttpResponseImpl;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.query.execution.plugin.adb.configuration.properties.ConnectorProperties;
import ru.ibs.dtm.query.execution.plugin.adb.dto.MpprKafkaConnectorRequest;
import ru.ibs.dtm.query.execution.plugin.adb.service.MpprKafkaConnectorService;

import java.net.HttpURLConnection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class MpprKafkaConnectorServiceImplTest {

  private ConnectorProperties connectorProperties;
  private WebClient webClient = mock(WebClient.class);
  private MpprKafkaConnectorService mpprKafkaConnectorService;

  public MpprKafkaConnectorServiceImplTest() {
    connectorProperties = new ConnectorProperties();
    connectorProperties.setHost("localhost");
    connectorProperties.setPort(8090);
    connectorProperties.setUrl("/query");
    mpprKafkaConnectorService = new MpprKafkaConnectorServiceImpl(
      connectorProperties,
      webClient);
  }

  @Test
  void callOk() {
    HttpRequest<Buffer> post = mock(HttpRequest.class);
    when(webClient.post(anyInt(), anyString(), anyString())).thenReturn(post);
    doAnswer(invocation -> {
      Handler<AsyncResult<HttpResponse<Buffer>>> handler = invocation.getArgument(1);
      handler.handle(Future.succeededFuture(new HttpResponseImpl<>(
        HttpVersion.HTTP_1_0,
        HttpURLConnection.HTTP_OK,
        "Ok",
        null,
        null,
        null,
        new BufferImpl().appendBytes("text".getBytes()),
        null
      )));
      return null;
    }).when(post).sendJson(any(), any());

    mpprKafkaConnectorService.call(new MpprKafkaConnectorRequest(), ar -> {
      assertTrue(ar.succeeded());
    });
  }

  @Test
  void callError() {
    String expectedText = "error, test Ok";

    HttpRequest<Buffer> post = mock(HttpRequest.class);
    when(webClient.post(anyInt(), anyString(), anyString())).thenReturn(post);
    doAnswer(invocation -> {
      Handler<AsyncResult<HttpResponse<Buffer>>> handler = invocation.getArgument(1);
      handler.handle(Future.succeededFuture(new HttpResponseImpl<>(
        HttpVersion.HTTP_1_0,
        HttpURLConnection.HTTP_INTERNAL_ERROR,
        "Internal Error",
        null,
        null,
        null,
        new BufferImpl().appendBytes(expectedText.getBytes()),
        null
      )));
      return null;
    }).when(post).sendJson(any(), any());

    mpprKafkaConnectorService.call(new MpprKafkaConnectorRequest(), ar -> {
      assertTrue(!ar.succeeded());
      assertEquals(expectedText, ar.cause().getMessage());
    });
  }
}
