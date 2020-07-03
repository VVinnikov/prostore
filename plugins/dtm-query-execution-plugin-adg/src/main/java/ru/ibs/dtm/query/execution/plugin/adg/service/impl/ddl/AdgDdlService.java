package ru.ibs.dtm.query.execution.plugin.adg.service.impl.ddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.query.execution.plugin.adg.configuration.KafkaProperties;
import ru.ibs.dtm.query.execution.plugin.adg.configuration.kafka.KafkaAdminProperty;
import ru.ibs.dtm.query.execution.plugin.adg.service.AvroSchemaGenerator;
import ru.ibs.dtm.query.execution.plugin.adg.service.KafkaTopicService;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryExecutorService;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtCartridgeProvider;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DdlRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.DdlService;

import java.util.Arrays;
import java.util.List;

import static ru.ibs.dtm.query.execution.plugin.adg.constants.ColumnFields.*;
import static ru.ibs.dtm.query.execution.plugin.adg.constants.Procedures.DROP_SPACE;

@Slf4j
@Service("adgDdlService")
public class AdgDdlService implements DdlService<Void> {

	private TtCartridgeProvider cartridgeProvider;
	private KafkaTopicService kafkaTopicService;
	private KafkaProperties kafkaProperties;
	private AvroSchemaGenerator schemaGenerator;
	private final QueryExecutorService executorService;

	@Autowired
	public AdgDdlService(TtCartridgeProvider cartridgeProvider, KafkaTopicService kafkaTopicService,
						 @Qualifier("adgKafkaProperties") KafkaProperties kafkaProperties, AvroSchemaGenerator schemaGenerator, QueryExecutorService executorService) {
		this.cartridgeProvider = cartridgeProvider;
		this.kafkaTopicService = kafkaTopicService;
		this.kafkaProperties = kafkaProperties;
		this.schemaGenerator = schemaGenerator;
		this.executorService = executorService;
	}

	@Override
	public void execute(DdlRequestContext context, Handler<AsyncResult<Void>> handler) {

		switch (context.getDdlType()) {
			case DROP_TABLE:
				dropTable(context.getRequest(), handler);
				return;
			case CREATE_SCHEMA:
			case DROP_SCHEMA:
				handler.handle(Future.succeededFuture());
				return;
		}
		applyConfig(context, handler);
	}

	private void applyConfig(DdlRequestContext context, Handler<AsyncResult<Void>> handler) {
		DdlRequest ddl = context.getRequest();
		Future.future((Promise<Void> promise) -> cartridgeProvider.apply(context, promise))
				.compose(d -> Future.future((Promise<Void> promise) ->
						kafkaTopicService.createOrReplace(getTopics(ddl.getClassTable()), promise)))
				.onSuccess(s -> handler.handle(Future.succeededFuture(s)))
				.onFailure(f -> handler.handle(Future.failedFuture(f)));
	}

	private void dropTable(final DdlRequest ddl, final Handler<AsyncResult<Void>> handler) {
		dropSpacesFromDb(ddl.getClassTable())
				.compose(d -> Future.future((Promise<Void> promise) -> cartridgeProvider.delete(ddl.getClassTable(), promise)))
				.compose(d -> Future.future((Promise<Void> promise) -> kafkaTopicService.delete(getTopics(ddl.getClassTable()), promise)))
				.onSuccess(s -> handler.handle(Future.succeededFuture(s)))
				.onFailure(f -> handler.handle(Future.failedFuture(f)));
	}

	private Future<Object> dropSpacesFromDb(final ClassTable classTable) {
		String actualTable = classTable.getName() + ACTUAL_POSTFIX;
		String historyTable = classTable.getName() + HISTORY_POSTFIX;
		String stagingTable = classTable.getName() + STAGING_POSTFIX;

		// TODO It is better to drop all spaces at one, but currently it is not supported by cartridge
		return executorService.executeProcedure(DROP_SPACE, actualTable)
				.compose(f -> executorService.executeProcedure(DROP_SPACE, historyTable))
				.compose(f -> executorService.executeProcedure(DROP_SPACE, stagingTable));
	}

	private List<String> getTopics(ClassTable classTable) {
		KafkaAdminProperty properties = kafkaProperties.getAdmin();
		String adgUploadRq = String.format(properties.getAdgUploadRq(), classTable.getName(), classTable.getSchema());
		String adgUploadRs = String.format(properties.getAdgUploadRs(), classTable.getName(), classTable.getSchema());
		String adgUploadErr = String.format(properties.getAdgUploadErr(), classTable.getName(), classTable.getSchema());
		return Arrays.asList(adgUploadRq, adgUploadRs, adgUploadErr);
	}

	private String getSubject(ClassTable classTable) {
		return String.format(kafkaProperties.getAdmin().getAdgUploadRq(), classTable.getName(), classTable.getSchema())
				.replace(".", "-");
	}
}
