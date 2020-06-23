package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.factory.MetadataFactory;
import ru.ibs.dtm.query.execution.core.service.DatabaseSynchronizeService;
import ru.ibs.dtm.query.execution.core.service.MetaStorageGeneratorService;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

import static ru.ibs.dtm.query.execution.plugin.api.ddl.DdlType.CREATE_TABLE;
import static ru.ibs.dtm.query.execution.plugin.api.ddl.DdlType.DROP_TABLE;

@Service
@Slf4j
public class DatabaseSynchronizeServiceImpl implements DatabaseSynchronizeService {

	private MetadataFactory<DdlRequestContext> metadataFactory;
	private MetaStorageGeneratorService generatorService;
	private ServiceDao serviceDao;

	@Autowired
	public DatabaseSynchronizeServiceImpl(MetadataFactory<DdlRequestContext> metadataFactory,
										  MetaStorageGeneratorService generatorService,
										  ServiceDao serviceDao) {
		this.metadataFactory = metadataFactory;
		this.generatorService = generatorService;
		this.serviceDao = serviceDao;
	}

	@Override
	public void putForRefresh(DdlRequestContext context,
							  String table,
							  boolean createTopics,
							  Handler<AsyncResult<Void>> handler) {
		metadataFactory.reflect(context, table, ar1 -> {
			if (ar1.succeeded()) {
				ClassTable classTable = ar1.result();
				serviceDao.dropTable(classTable, ar2 -> {
					if (ar2.succeeded()) {
						QueryRequest request = context.getRequest().getQueryRequest();
						classTable.setSchema(request.getDatamartMnemonic());
						classTable.setNameWithSchema(request.getDatamartMnemonic() + "." + classTable.getName());
						context.getRequest().setClassTable(classTable);
						context.setDdlType(CREATE_TABLE);
						applyMetadata(context, handler);
					} else {
						log.debug("Ошибка удаления таблицы в сервисной БД", ar2.cause());
						handler.handle(Future.failedFuture(ar2.cause()));
					}
				});
			} else {
				log.debug("Ошибка получения данных о таблицах из сервисной БД", ar1.cause());
				handler.handle(Future.failedFuture(ar1.cause()));
			}
		});
	}

	@Override
	public void removeTable(DdlRequestContext context, Long datamartId, String tableName, Handler<AsyncResult<Void>> handler) {
		metadataFactory.reflect(context, tableName, ar1 -> {
			if (ar1.succeeded()) {
				ClassTable classTable = ar1.result();
				serviceDao.dropTable(classTable, dropTableResult -> {
					if (dropTableResult.succeeded()) {
						QueryRequest queryRequest = context.getRequest().getQueryRequest();
						classTable.setSchema(queryRequest.getDatamartMnemonic());
						classTable.setNameWithSchema(queryRequest.getDatamartMnemonic() + "." + classTable.getName());

						context.setDdlType(DROP_TABLE);
						context.getRequest().setClassTable(classTable);
						metadataFactory.apply(context, result -> {
							if (result.succeeded()) {
								log.trace("Удаление сущности {} из схемы {}", tableName, queryRequest.getDatamartMnemonic());
								serviceDao.dropEntity(datamartId, tableName)
									.onSuccess(s -> handler.handle(Future.succeededFuture()))
									.onFailure(f -> handler.handle(Future.failedFuture(f)));
							} else {
								handler.handle(Future.failedFuture(result.cause()));
							}
						});
					} else {
						log.debug("Ошибка удаления таблицы в сервисной БД", dropTableResult.cause());
						handler.handle(Future.failedFuture(dropTableResult.cause()));
					}
				});
			} else {
				log.debug("Ошибка получения данных о таблицах из сервисной БД", ar1.cause());
				handler.handle(Future.failedFuture(ar1.cause()));
			}
		});
	}

	private void applyMetadata(DdlRequestContext context, Handler<AsyncResult<Void>> handler) {
		metadataFactory.apply(context, ar1 -> {
			if (ar1.succeeded()) {
				generatorService.save(context, ar2 -> {
					if (ar2.succeeded()) {
						handler.handle(Future.succeededFuture());
					} else {
						log.debug("Ошибка при генерации метаданных", ar2.cause());
						handler.handle(Future.failedFuture(ar2.cause()));
					}
				});
			} else {
				log.debug("Ошибка при отображение таблицы из сервисной БД", ar1.cause());
				handler.handle(Future.failedFuture(ar1.cause()));
			}
		});
	}

}
