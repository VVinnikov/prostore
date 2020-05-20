package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.configuration.jooq.MariaProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.dto.DatamartEntity;
import ru.ibs.dtm.query.execution.core.dto.ParsedQueryRequest;
import ru.ibs.dtm.query.execution.core.factory.MetadataFactory;
import ru.ibs.dtm.query.execution.core.service.DatabaseSynchronizeService;
import ru.ibs.dtm.query.execution.core.service.DdlService;
import ru.ibs.dtm.query.execution.core.utils.SqlPreparer;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.dto.DdlRequest;

import java.util.ArrayList;
import java.util.List;

import static ru.ibs.dtm.query.execution.plugin.api.ddl.DdlQueryType.CREATE_SCHEMA;
import static ru.ibs.dtm.query.execution.plugin.api.ddl.DdlQueryType.DROP_SCHEMA;

@Slf4j
@Service("coreDdlService")
public class DdlServiceImpl implements DdlService {

	private static final Logger logger = LoggerFactory.getLogger(DdlServiceImpl.class);

	private final ServiceDao serviceDao;
	private final CalciteDefinitionService calciteDefinitionService;
	private final DatabaseSynchronizeService databaseSynchronizeService;
	private final MetadataFactory<DdlRequestContext>  metadataFactory;
	private final MariaProperties mariaProperties;
	private final Vertx vertx;

	@Autowired
	public DdlServiceImpl(ServiceDao serviceDao,
						  CalciteDefinitionService calciteDefinitionService,
						  DatabaseSynchronizeService databaseSynchronizeService,
						  MetadataFactory<DdlRequestContext>  metadataFactory, MariaProperties mariaProperties,
						  @Qualifier("coreVertx") Vertx vertx
	) {
		this.serviceDao = serviceDao;
		this.calciteDefinitionService = calciteDefinitionService;
		this.databaseSynchronizeService = databaseSynchronizeService;
		this.metadataFactory = metadataFactory;
		this.mariaProperties = mariaProperties;
		this.vertx = vertx;
	}

	@Override
	public void execute(ParsedQueryRequest parsedQueryRequest, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
		vertx.executeBlocking(it -> {
			try {
				SqlNode node = calciteDefinitionService.processingQuery(parsedQueryRequest.getQueryRequest().getSql());
				it.complete(node);
			} catch (Exception e) {
				logger.error("Ошибка парсинга запроса", e);
				it.fail(e);
			}
		}, ar -> {
			if (ar.succeeded()) {
				execute(parsedQueryRequest.getQueryRequest(), asyncResultHandler, ar);
			} else {
				logger.debug("Ошибка исполнения", ar.cause());
				asyncResultHandler.handle(Future.failedFuture(ar.cause()));
			}
		});
	}

	private void execute(QueryRequest request, Handler<AsyncResult<QueryResult>> handler, AsyncResult<Object> ar) {
		if (ar.result() instanceof SqlDdl) {
			SqlDdl sqlDdl = ((SqlDdl) ar.result());
			String sqlNodeName = sqlDdl.getOperandList().stream().filter(t -> t instanceof SqlIdentifier).findFirst().get().toString();

			switch (sqlDdl.getKind()) {
				case CREATE_SCHEMA:
					createSchema(request, handler, sqlNodeName);
					break;
				case DROP_SCHEMA:
					dropSchema(request, handler, sqlNodeName);
					break;
				case CREATE_TABLE:
					createTable(request, handler, sqlNodeName);
					break;
				case DROP_TABLE:
					dropTable(request, handler, sqlNodeName);
					break;
				case DEFAULT:
					logger.error("Не поддерживаемый тип DDL запроса");
					handler.handle(Future.failedFuture(String.format("Не поддерживаемый тип DDL запроса [%s]", request)));
			}
		} else {
			logger.error("Не поддерживаемый тип запроса");
			handler.handle(Future.failedFuture(String.format("Не поддерживаемый тип запроса [%s]", request)));
		}
	}

	private void dropTable(QueryRequest request, Handler<AsyncResult<QueryResult>> handler, String sqlNodeName) {
		String schema = getSchemaName(request, sqlNodeName);
		String table = getTableName(sqlNodeName);
		request.setDatamartMnemonic(schema);
		dropTable(request, table, containsIfExistsCheck(request.getSql()), handler);
	}

	private String getTableName(String sqlNodeName) {
		int indexComma = sqlNodeName.indexOf(".");
		return sqlNodeName.substring(indexComma + 1);
	}

	private String getSchemaName(QueryRequest request, String sqlNodeName) {
		int indexComma = sqlNodeName.indexOf(".");
		return indexComma == -1 ? request.getDatamartMnemonic() : sqlNodeName.substring(0, indexComma);
	}

	private void dropSchema(QueryRequest request, Handler<AsyncResult<QueryResult>> handler, String sqlNodeName) {
		request.setDatamartMnemonic(sqlNodeName);
		dropDatamart(request, sqlNodeName, handler);
	}

	private void createSchema(QueryRequest request, Handler<AsyncResult<QueryResult>> handler, String sqlNodeName) {
		replaceDatabaseInSql(request);
		metadataFactory.apply(new DdlRequestContext(new DdlRequest(request, CREATE_SCHEMA)), result -> {
			if (result.succeeded()) {
				createDatamart(sqlNodeName, handler);
			} else {
				handler.handle(Future.failedFuture(result.cause()));
			}
		});
	}

	private void replaceDatabaseInSql(QueryRequest request) {
		String sql = request.getSql().replaceAll("(?i) database", " schema");
		request.setSql(sql);
	}

	//TODO проверка наличия if exists

	/**
	 * Проверка, что запрос содержит IF EXISTS
	 *
	 * @param sql - исходный запрос
	 * @return - содержит IF EXISTS
	 */
	private boolean containsIfExistsCheck(String sql) {
		return sql.toLowerCase().contains("if exists");
	}

	private void createTable(QueryRequest request, Handler<AsyncResult<QueryResult>> handler, String sqlNodeName) {
		String schema = getSchemaName(request, sqlNodeName);
		request.setDatamartMnemonic(schema);

		String tableWithSchema = SqlPreparer.getTableWithSchema(mariaProperties.getOptions().getDatabase(), sqlNodeName);
		String sql = SqlPreparer.replaceQuote(SqlPreparer.replaceTableInSql(request.getSql(), tableWithSchema));
		serviceDao.executeUpdate(sql, ar2 -> {
			if (ar2.succeeded()) {
				databaseSynchronizeService.putForRefresh(
						request,
						tableWithSchema,
						true, ar3 -> {
							if (ar3.succeeded()) {
								handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
							} else {
								logger.error("Ошибка синхронизации {}", tableWithSchema, ar3.cause());
								handler.handle(Future.failedFuture(ar3.cause()));
							}
						});
			} else {
				logger.error("Ошибка исполнения запроса {}", sql, ar2.cause());
				handler.handle(Future.failedFuture(ar2.cause()));
			}
		});
	}


	private void createDatamart(String datamartName, Handler<AsyncResult<QueryResult>> handler) {
		serviceDao.findDatamart(datamartName, datamartResult -> {
			if (datamartResult.succeeded()) {
				log.error("База данных {} уже существует", datamartName);
				handler.handle(Future.failedFuture(String.format("База данных [%s] уже существует", datamartName)));
			} else {
				serviceDao.insertDatamart(datamartName, insertResult -> {
					if (insertResult.succeeded()) {
						log.debug("Создана новая витрина {}", datamartName);
						handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
					} else {
						log.error("Ошибка при создании витрины {}", datamartName, insertResult.cause());
						handler.handle(Future.failedFuture(insertResult.cause()));
					}
				});
			}
		});
	}

	private void dropTable(QueryRequest request, String tableName, boolean ifExists, Handler<AsyncResult<QueryResult>> handler) {
		serviceDao.findDatamart(request.getDatamartMnemonic(), datamartResult -> {
			if (datamartResult.succeeded()) {
				serviceDao.findEntity(datamartResult.result(), tableName, entityResult -> {
					if (entityResult.succeeded()) {
						databaseSynchronizeService.removeTable(request, datamartResult.result(), tableName, removeResult -> {
							if (removeResult.succeeded()) {
								handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
							} else {
								handler.handle(Future.failedFuture(removeResult.cause()));
							}
						});
					} else {
						if (ifExists) {
							handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
							return;
						}
						final String msg = "Логической таблицы " + tableName + " не существует (не найдена сущность)";
						logger.error(msg);
						handler.handle(Future.failedFuture(msg));
					}
				});
			} else {
				handler.handle(Future.failedFuture(datamartResult.cause()));
			}
		});
	}

	private void dropDatamart(QueryRequest request, String datamartName, Handler<AsyncResult<QueryResult>> handler) {
		serviceDao.findDatamart(datamartName, datamartResult -> {
			if (datamartResult.succeeded()) {
				serviceDao.getEntitiesMeta(datamartName, entitiesMetaResult -> {
					if (entitiesMetaResult.succeeded()) {
						//удаляем все таблицы
						dropAllTables(entitiesMetaResult.result(), resultTableDelete -> {
							if (resultTableDelete.succeeded()) {
								//удаляем физическую витрину
								replaceDatabaseInSql(request);
								metadataFactory.apply(new DdlRequestContext(new DdlRequest(request, DROP_SCHEMA)), result -> {
									if (result.succeeded()) {
										//удаляем логическую витрину
										serviceDao.dropDatamart(datamartResult.result(), ar2 -> {
											if (ar2.succeeded()) {
												handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
											} else {
												handler.handle(Future.failedFuture(ar2.cause()));
											}
										});
									} else {
										handler.handle(Future.failedFuture(result.cause()));
									}
								});
							} else {
								handler.handle(Future.failedFuture(resultTableDelete.cause()));
							}
						});
					} else {
						handler.handle(Future.failedFuture(entitiesMetaResult.cause()));
					}
				});
			} else {
				handler.handle(Future.failedFuture(datamartResult.cause()));
			}
		});

	}

	/**
	 * Запуск асинхронного вызова удаления таблиц
	 *
	 * @param entities - список таблиц на удаление
	 * @param handler  - обработчик
	 */
	private void dropAllTables(List<DatamartEntity> entities, Handler<AsyncResult<QueryResult>> handler) {
		List<Future> futures = new ArrayList<>();
		entities.forEach(entity -> {
			futures.add(Future.future(p -> {
				QueryRequest requestDeleteTable = new QueryRequest();
				requestDeleteTable.setDatamartMnemonic(entity.getDatamartMnemonic());
				requestDeleteTable.setSql("DROP TABLE IF EXISTS " + entity.getDatamartMnemonic() + "." + entity.getMnemonic());
				dropTable(requestDeleteTable, entity.getMnemonic(), true,
						ar -> {
							if (ar.succeeded()) {
								p.complete();
							} else {
								p.fail(ar.cause());
							}
						});
			}));
		});
		CompositeFuture.all(futures).setHandler(ar -> {
			if (ar.succeeded()) {
				handler.handle(Future.succeededFuture());
			} else {
				handler.handle(Future.failedFuture(ar.cause()));
			}
		});
	}
}
