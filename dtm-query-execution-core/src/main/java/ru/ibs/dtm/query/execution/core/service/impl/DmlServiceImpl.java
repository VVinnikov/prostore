package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.QuerySourceRequest;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.core.service.DataSourcePluginService;
import ru.ibs.dtm.query.execution.core.service.MetadataService;
import ru.ibs.dtm.query.execution.core.service.SchemaStorageProvider;
import ru.ibs.dtm.query.execution.core.service.TargetDatabaseDefinitionService;
import ru.ibs.dtm.query.execution.plugin.api.dml.DmlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.LlrRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.DmlService;

@Service("coreDmlService")
public class DmlServiceImpl implements DmlService<QueryResult> {

	private final DataSourcePluginService dataSourcePluginService;
	private final TargetDatabaseDefinitionService targetDatabaseDefinitionService;
	private final SchemaStorageProvider schemaStorageProvider;
	private final MetadataService metadataService;

	@Autowired
	public DmlServiceImpl(DataSourcePluginService dataSourcePluginService,
						  TargetDatabaseDefinitionService targetDatabaseDefinitionService,
						  SchemaStorageProvider schemaStorageProvider,
						  MetadataService metadataService) {
		this.dataSourcePluginService = dataSourcePluginService;
		this.targetDatabaseDefinitionService = targetDatabaseDefinitionService;
		this.schemaStorageProvider = schemaStorageProvider;
		this.metadataService = metadataService;
	}

	@Override
	public void execute(DmlRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
		targetDatabaseDefinitionService.getTargetSource(context.getRequest().getQueryRequest(), ar -> {
			if (ar.succeeded()) {
				QuerySourceRequest querySourceRequest = ar.result();
				if (querySourceRequest.getSourceType() == SourceType.INFORMATION_SCHEMA) {
					metadataService.executeQuery(context.getRequest().getQueryRequest(), asyncResultHandler);
				} else {
					pluginExecute(querySourceRequest, asyncResultHandler);
				}
			} else {
				asyncResultHandler.handle(Future.failedFuture(ar.cause()));
			}
		});
	}

	private void pluginExecute(QuerySourceRequest request,
							   Handler<AsyncResult<QueryResult>> asyncResultHandler) {
		schemaStorageProvider.getLogicalSchema(request.getQueryRequest().getDatamartMnemonic(), schemaAr -> {
			if (schemaAr.succeeded()) {
				JsonObject schema = schemaAr.result();
				dataSourcePluginService.llr(
						request.getSourceType(),
						new LlrRequestContext(new LlrRequest(request.getQueryRequest(), schema)),
						asyncResultHandler);
			} else {
				asyncResultHandler.handle(Future.failedFuture(schemaAr.cause()));
			}
		});
	}
}
