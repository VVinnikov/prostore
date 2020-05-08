package ru.ibs.dtm.query.execution.plugin.adg.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adg.configuration.KafkaProperties;
import ru.ibs.dtm.query.execution.plugin.adg.configuration.kafka.KafkaAdminProperty;
import ru.ibs.dtm.query.execution.plugin.adg.model.QueryResultItem;
import ru.ibs.dtm.query.execution.plugin.adg.model.metadata.ColumnMetadata;
import ru.ibs.dtm.query.execution.plugin.adg.model.metadata.ColumnType;
import ru.ibs.dtm.query.execution.plugin.adg.service.*;
import ru.ibs.dtm.query.execution.plugin.adg.service.impl.ddl.AdgDdlService;
import ru.ibs.dtm.query.execution.plugin.api.dto.DdlRequest;
import ru.ibs.dtm.query.execution.plugin.api.dto.LlrRequest;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static ru.ibs.dtm.query.execution.plugin.adg.constants.Procedures.DROP_SPACE;

public class AdgDdlServiceImpl {

  private TtCartridgeProvider cartridgeProvider = mock(TtCartridgeProvider.class);
  private KafkaTopicService kafkaTopicService = mock(KafkaTopicService.class);
  private KafkaProperties kafkaProperties = mock(KafkaProperties.class);
  private AvroSchemaGenerator schemaGenerator = mock(AvroSchemaGenerator.class);
  private SchemaRegistryClient registryClient = mock(SchemaRegistryClient.class);
  private final QueryExecutorService executorService = mock(QueryExecutorService.class);

  private AdgDdlService adgDdlService = new AdgDdlService(cartridgeProvider, kafkaTopicService, kafkaProperties,
    schemaGenerator, registryClient, executorService);

  @Test
  void testExecuteNotEmptyOk() {
    KafkaAdminProperty kafkaAdminProperty = new KafkaAdminProperty();
    kafkaAdminProperty.setAdgUploadRq("%s.%s.adg.upload.rq");
    kafkaAdminProperty.setAdgUploadRq("%s.%s.adg.upload.rs");
    kafkaAdminProperty.setAdgUploadRq("%s.%s.adg.upload.err");
    when(kafkaProperties.getAdmin()).thenReturn(kafkaAdminProperty);

    doAnswer(invocation -> {
      Handler<AsyncResult<Object>> handler = invocation.getArgument(0);
      handler.handle(Future.succeededFuture());
      return null;
    }).when(executorService).executeProcedure(any(), eq(DROP_SPACE), eq("test_table"));

    doAnswer(invocation -> {
      Handler<AsyncResult<Object>> handler = invocation.getArgument(0);
      handler.handle(Future.succeededFuture());
      return null;
    }).when(kafkaTopicService).delete(any(), any());

    doAnswer(invocation -> {
      Handler<AsyncResult<Object>> handler = invocation.getArgument(0);
      handler.handle(Future.succeededFuture());
      return null;
    }).when(registryClient).unregister(any(), any());

    QueryRequest queryRequest = new QueryRequest();
    queryRequest.setRequestId(UUID.randomUUID());
    queryRequest.setSql("drop table test_table");
    queryRequest.setDatamartMnemonic("test_schema");

    List<ClassField> fields = Collections.singletonList(new ClassField("test_field", "varchar(1)", false, false, ""));
    ClassTable classTable = new ClassTable("test_schema.test_table", fields);

    adgDdlService.execute(new DdlRequest(queryRequest, classTable, false), handler -> {
      assertTrue(handler.succeeded());
    });
  }
}
