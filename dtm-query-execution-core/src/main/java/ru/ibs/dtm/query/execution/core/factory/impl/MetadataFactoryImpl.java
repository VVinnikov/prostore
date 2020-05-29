package ru.ibs.dtm.query.execution.core.factory.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.factory.MetadataFactory;
import ru.ibs.dtm.query.execution.core.service.DataSourcePluginService;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

import java.util.ArrayList;
import java.util.List;

@Component
public class MetadataFactoryImpl implements MetadataFactory<DdlRequestContext> {

	private ServiceDao serviceDao;
	private DataSourcePluginService dataSourcePluginService;

	@Autowired
	public MetadataFactoryImpl(ServiceDao serviceDao, DataSourcePluginService dataSourcePluginService) {
		this.serviceDao = serviceDao;
		this.dataSourcePluginService = dataSourcePluginService;
	}

	@Override
	public void reflect(String table, Handler<AsyncResult<ClassTable>> handler) {
		serviceDao.getMetadataByTableName(table, ar -> {
			if (ar.succeeded()) {
				ClassTable res = new ClassTable(table, ar.result());
				handler.handle(Future.succeededFuture(res));
			} else {
				handler.handle(Future.failedFuture(ar.cause()));
			}
		});
	}

	@Override
	public void apply(DdlRequestContext context, Handler<AsyncResult<Void>> handler) {
		List<Future> futures = new ArrayList<>();
		dataSourcePluginService.getSourceTypes().forEach(sourceType ->
				futures.add(Future.future(p -> dataSourcePluginService.ddl(
						sourceType,
						context,
						ar -> {
							if (ar.succeeded()) {
								p.complete();
							} else {
								p.fail(ar.cause());
							}
						}))));
		CompositeFuture.all(futures).setHandler(ar -> {
			if (ar.succeeded()) {
				handler.handle(Future.succeededFuture());
			} else {
				handler.handle(Future.failedFuture(ar.cause()));
			}
		});
	}
}
