package ru.ibs.dtm.query.execution.plugin.adg.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.adg.configuration.KafkaProperties;
import ru.ibs.dtm.query.execution.plugin.adg.configuration.kafka.KafkaAdminProperty;
import ru.ibs.dtm.query.execution.plugin.adg.service.AvroSchemaGenerator;
import ru.ibs.dtm.query.execution.plugin.adg.service.KafkaTopicService;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryExecutorService;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtCartridgeProvider;
import ru.ibs.dtm.query.execution.plugin.adg.service.impl.ddl.AdgDdlService;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DdlRequest;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static ru.ibs.dtm.query.execution.plugin.adg.constants.Procedures.DROP_SPACE;
import static ru.ibs.dtm.query.execution.plugin.api.ddl.DdlType.DROP_TABLE;

public class AdgDdlServiceImpl {

	private TtCartridgeProvider cartridgeProvider = mock(TtCartridgeProvider.class);
	private KafkaTopicService kafkaTopicService = mock(KafkaTopicService.class);
	private KafkaProperties kafkaProperties = mock(KafkaProperties.class);
	private AvroSchemaGenerator schemaGenerator = mock(AvroSchemaGenerator.class);
	private final QueryExecutorService executorService = mock(QueryExecutorService.class);

	private AdgDdlService adgDdlService = new AdgDdlService(cartridgeProvider, kafkaTopicService, kafkaProperties,
			schemaGenerator, executorService);

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
		}).when(executorService).executeProcedure(eq(DROP_SPACE), eq("test_table"));

		doAnswer(invocation -> {
			Handler<AsyncResult<Object>> handler = invocation.getArgument(0);
			handler.handle(Future.succeededFuture());
			return null;
		}).when(kafkaTopicService).delete(any(), any());

		QueryRequest queryRequest = new QueryRequest();
		queryRequest.setRequestId(UUID.randomUUID());
		queryRequest.setSql("drop table test_table");
		queryRequest.setDatamartMnemonic("test_schema");

		List<ClassField> fields = Collections.singletonList(new ClassField("test_field", "varchar(1)", false, 1, 1, ""));
		ClassTable classTable = new ClassTable("test_schema.test_table", fields);

      DdlRequestContext context = new DdlRequestContext(new DdlRequest(queryRequest, classTable));
      context.setDdlType(DROP_TABLE);
      adgDdlService.execute(context, handler -> {
			assertTrue(handler.succeeded());
		});
	}
}
