package ru.ibs.dtm.query.execution.core.dao.impl;

import io.github.jklingsporn.vertx.jooq.classic.async.AsyncClassicGenericQueryExecutor;
import io.github.jklingsporn.vertx.jooq.shared.internal.QueryResult;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.ext.sql.ResultSet;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Select;
import org.jooq.generated.dtmservice.tables.records.UploadExternalTableRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;
import ru.ibs.dtm.common.delta.DeltaLoadStatus;
import ru.ibs.dtm.common.dto.ActualDeltaRequest;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.plugin.exload.Type;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.dto.*;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaRecord;
import ru.ibs.dtm.query.execution.core.dto.eddl.CreateDownloadExternalTableQuery;
import ru.ibs.dtm.query.execution.core.dto.eddl.CreateUploadExternalTableQuery;
import ru.ibs.dtm.query.execution.core.dto.eddl.DropUploadExternalTableQuery;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoField.*;
import static org.jooq.generated.dtmservice.Tables.*;
import static org.jooq.generated.information_schema.Tables.COLUMNS;
import static org.jooq.generated.information_schema.Tables.TABLES;
import static org.jooq.impl.DSL.max;

@Repository
@Slf4j
public class ServiceDaoImpl implements ServiceDao {

    private static final String SCHEMA_TABLE_COLUMN_NAME = "COLUMN_NAME";
    private static final String SCHEMA_TABLE_COLUMN_TYPE = "COLUMN_TYPE";
    private static final String SCHEMA_TABLE_IS_NULLABLE = "IS_NULLABLE";
    private static final String SCHEMA_TABLE_COLUMN_KEY = "COLUMN_KEY";
    private static final String SCHEMA_TABLE_COLUMN_DEFAULT = "COLUMN_DEFAULT";

    private static final DateTimeFormatter LOCAL_DATE_TIME = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .toFormatter();

    private final AsyncClassicGenericQueryExecutor executor;

    @Autowired
    public ServiceDaoImpl(@Qualifier("coreQueryExecutor") AsyncClassicGenericQueryExecutor executor) {
        this.executor = executor;
    }

    @Override
    public void findDatamart(String name, Handler<AsyncResult<Long>> resultHandler) {
        executor.query(dsl -> dsl
                .select(DATAMARTS_REGISTRY.DATAMART_ID)
                .from(DATAMARTS_REGISTRY)
                .where(DATAMARTS_REGISTRY.DATAMART_MNEMONICS.equalIgnoreCase(name))).setHandler(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(ar.result().hasResults()
                        ? Future.succeededFuture(ar.result().get(DATAMARTS_REGISTRY.DATAMART_ID))
                        : Future.failedFuture(String.format("Витрина не найдена: [%s]", name)));
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void dropDatamart(Long id, Handler<AsyncResult<Void>> resultHandler) {
        executor.execute(dsl -> dsl
                .deleteFrom(DATAMARTS_REGISTRY)
                .where(DATAMARTS_REGISTRY.DATAMART_ID.eq(id))
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void insertDatamart(String name, Handler<AsyncResult<Void>> resultHandler) {
        executor.execute(dsl -> dsl
                .insertInto(DATAMARTS_REGISTRY)
                .set(DATAMARTS_REGISTRY.DATAMART_MNEMONICS, name))
                .setHandler(ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture());
                    } else {
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    @Override
    public void insertEntity(Long datamartId, String name, Handler<AsyncResult<Void>> resultHandler) {
        executor.execute(dsl -> dsl
                .insertInto(ENTITIES_REGISTRY)
                .set(ENTITIES_REGISTRY.DATAMART_ID, datamartId)
                .set(ENTITIES_REGISTRY.ENTITY_MNEMONICS, name)
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void findEntity(Long datamartId, String name, Handler<AsyncResult<Long>> resultHandler) {
        executor.query(dsl -> dsl
                .select(ENTITIES_REGISTRY.ENTITY_ID)
                .from(ENTITIES_REGISTRY)
                .where(ENTITIES_REGISTRY.DATAMART_ID.eq(datamartId))
                .and(ENTITIES_REGISTRY.ENTITY_MNEMONICS.equalIgnoreCase(name))
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(ar.result().hasResults()
                        ? Future.succeededFuture(ar.result().get(ENTITIES_REGISTRY.ENTITY_ID))
                        : Future.failedFuture(String.format("Таблица не найдена: [%s]", name)));
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void dropEntity(Long datamartId, String name, Handler<AsyncResult<Void>> resultHandler) {
        executor.execute(dsl -> dsl
                .deleteFrom(ENTITIES_REGISTRY)
                .where(ENTITIES_REGISTRY.DATAMART_ID.eq(datamartId))
                .and(ENTITIES_REGISTRY.ENTITY_MNEMONICS.equalIgnoreCase(name))
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void insertAttribute(Long entityId, String name, Integer typeId, Integer length, Handler<AsyncResult<Void>> resultHandler) {
        executor.execute(dsl -> dsl
                .insertInto(ATTRIBUTES_REGISTRY)
                .set(ATTRIBUTES_REGISTRY.ENTITY_ID, entityId)
                .set(ATTRIBUTES_REGISTRY.ATTR_MNEMONICS, name)
                .set(ATTRIBUTES_REGISTRY.LENGTH, length)
                .set(ATTRIBUTES_REGISTRY.DATA_TYPE_ID, typeId)
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void dropAttribute(Long entityId, Handler<AsyncResult<Void>> resultHandler) {
        executor.execute(dsl -> dsl
                .deleteFrom(ATTRIBUTES_REGISTRY)
                .where(ATTRIBUTES_REGISTRY.ENTITY_ID.eq(entityId))
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void selectType(String name, Handler<AsyncResult<Integer>> resultHandler) {
        executor.query(dsl -> dsl
                .select(DATA_TYPES_REGISTRY.DATA_TYPE_ID)
                .from(DATA_TYPES_REGISTRY)
                .where(DATA_TYPES_REGISTRY.DATA_TYPE_MNEMONICS.equalIgnoreCase(name))
        ).setHandler(ar -> {
            if (ar.succeeded())
                resultHandler.handle(ar.result().hasResults()
                        ? Future.succeededFuture(ar.result().get(DATA_TYPES_REGISTRY.DATA_TYPE_ID))
                        : Future.failedFuture(String.format("Тип не найден: [%s]", name)));
            else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void getDatamartMeta(Handler<AsyncResult<List<DatamartInfo>>> resultHandler) {
        executor.query(dsl -> dsl
                .select(DATAMARTS_REGISTRY.DATAMART_ID, DATAMARTS_REGISTRY.DATAMART_MNEMONICS)
                .from(DATAMARTS_REGISTRY)).setHandler(ar -> {
            if (ar.succeeded()) {
                if (ar.result().unwrap() instanceof ResultSet) {
                    ResultSet rows = ar.result().unwrap();
                    List<DatamartInfo> datamartInfoList = new ArrayList<>();
                    rows.getRows().forEach(it ->
                            datamartInfoList.add(new DatamartInfo(
                                    it.getInteger(DATAMARTS_REGISTRY.DATAMART_ID.getName()),
                                    it.getString(DATAMARTS_REGISTRY.DATAMART_MNEMONICS.getName())
                            ))
                    );
                    resultHandler.handle(Future.succeededFuture(datamartInfoList));
                } else {
                    resultHandler.handle(Future.failedFuture("Невозможно получить метаданные"));
                }
            } else
                resultHandler.handle(Future.failedFuture(ar.cause()));
        });
    }

    public void getEntitiesMeta(String datamartMnemonic, Handler<AsyncResult<List<DatamartEntity>>> resultHandler) {
        executor.query(dsl -> dsl
                .select(ENTITIES_REGISTRY.ENTITY_ID, ENTITIES_REGISTRY.ENTITY_MNEMONICS, DATAMARTS_REGISTRY.DATAMART_MNEMONICS)
                .from(ENTITIES_REGISTRY)
                .join(DATAMARTS_REGISTRY)
                .on(ENTITIES_REGISTRY.DATAMART_ID.eq(DATAMARTS_REGISTRY.DATAMART_ID))
                .where(DATAMARTS_REGISTRY.DATAMART_MNEMONICS.equalIgnoreCase(datamartMnemonic))
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                if (ar.result().unwrap() instanceof ResultSet) {
                    List<DatamartEntity> datamartEntityList = new ArrayList<>();
                    ResultSet rows = ar.result().unwrap();
                    rows.getRows().forEach(it ->
                            datamartEntityList.add(new DatamartEntity(
                                    it.getInteger(ENTITIES_REGISTRY.ENTITY_ID.getName()),
                                    it.getString(ENTITIES_REGISTRY.ENTITY_MNEMONICS.getName()),
                                    it.getString(DATAMARTS_REGISTRY.DATAMART_MNEMONICS.getName())
                            ))
                    );
                    log.info("Найдено {} сущностей для витрины: {}", datamartEntityList.size(), datamartMnemonic);
                    resultHandler.handle(Future.succeededFuture(datamartEntityList));
                } else {
                    resultHandler.handle(Future.failedFuture(String.format("Невозможно получить сущности для витрины %s", datamartMnemonic)));
                }
            } else
                resultHandler.handle(Future.failedFuture(ar.cause()));
        });

    }

    public void getAttributesMeta(String datamartMnemonic, String entityMnemonic, Handler<AsyncResult<List<EntityAttribute>>> resultHandler) {
        executor.query(dsl -> dsl
                .select(ATTRIBUTES_REGISTRY.ATTR_ID,
                        ATTRIBUTES_REGISTRY.ATTR_MNEMONICS,
                        ATTRIBUTES_REGISTRY.LENGTH,
                        ATTRIBUTES_REGISTRY.ACCURACY,
                        DATA_TYPES_REGISTRY.DATA_TYPE_MNEMONICS,
                        DATAMARTS_REGISTRY.DATAMART_MNEMONICS,
                        ENTITIES_REGISTRY.ENTITY_MNEMONICS
                )
                .from(ATTRIBUTES_REGISTRY)
                .join(DATA_TYPES_REGISTRY).on(ATTRIBUTES_REGISTRY.DATA_TYPE_ID.eq(DATA_TYPES_REGISTRY.DATA_TYPE_ID))
                .join(ENTITIES_REGISTRY).on(ENTITIES_REGISTRY.ENTITY_ID.eq(ATTRIBUTES_REGISTRY.ENTITY_ID))
                .join(DATAMARTS_REGISTRY).on(DATAMARTS_REGISTRY.DATAMART_ID.eq(ENTITIES_REGISTRY.DATAMART_ID))
                .where(DATAMARTS_REGISTRY.DATAMART_MNEMONICS.equalIgnoreCase(datamartMnemonic)).and(ENTITIES_REGISTRY.ENTITY_MNEMONICS.equalIgnoreCase(entityMnemonic))
        ).setHandler(ar -> {
            if (ar.succeeded() && ar.result().unwrap() instanceof ResultSet) {
                List<EntityAttribute> res = new ArrayList<>();
                ResultSet rows;
                rows = ar.result().unwrap();
                rows.getRows().forEach(it ->
                        res.add(new EntityAttribute(
                                it.getInteger(ATTRIBUTES_REGISTRY.ATTR_ID.getName()),
                                it.getString(ATTRIBUTES_REGISTRY.ATTR_MNEMONICS.getName()),
                                it.getString(DATA_TYPES_REGISTRY.DATA_TYPE_MNEMONICS.getName()),
                                it.getInteger(ATTRIBUTES_REGISTRY.LENGTH.getName()),
                                it.getInteger(ATTRIBUTES_REGISTRY.ACCURACY.getName()),
                                it.getString(ENTITIES_REGISTRY.ENTITY_MNEMONICS.getName()),
                                it.getString(DATAMARTS_REGISTRY.DATAMART_MNEMONICS.getName())
                        ))
                );
                log.info("Найдено {} атрибутов для сущности: '{}' схемы: '{}'.", res.size(), entityMnemonic, datamartMnemonic);
                resultHandler.handle(Future.succeededFuture(res));
            } else {
                log.error("Невозможно получить атрибуты метаданных: {}", ar.cause().getMessage());
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void getMetadataByTableName(String tableName, Handler<AsyncResult<List<ClassField>>> resultHandler) {
        int indexComma = tableName.indexOf(".");
        String schema = indexComma != -1 ? tableName.substring(0, indexComma) : "test";
        String table = tableName.substring(indexComma + 1);
        executor.query(dsl -> dsl.select()
                .from(COLUMNS)
                .join(TABLES)
                .on(COLUMNS.TABLE_NAME.eq(TABLES.TABLE_NAME)).and(COLUMNS.TABLE_SCHEMA.eq(TABLES.TABLE_SCHEMA))
                // TODO: пока так, нужен upscale sql
                .where(TABLES.TABLE_NAME.eq(table))
                .and(TABLES.TABLE_SCHEMA.equalIgnoreCase(schema))
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                QueryResult result = ar.result();
                ResultSet resultSet = result.unwrap();
                List<ClassField> classFieldList = new ArrayList<>();
                resultSet.getRows().forEach(row -> {
                    classFieldList.add(
                            new ClassField(row.getString(SCHEMA_TABLE_COLUMN_NAME),
                                    row.getString(SCHEMA_TABLE_COLUMN_TYPE),
                                    row.getString(SCHEMA_TABLE_IS_NULLABLE).contains("YES"),
                                    row.getString(SCHEMA_TABLE_COLUMN_KEY).contains("PRI"),
                                    row.getString(SCHEMA_TABLE_COLUMN_DEFAULT)));
                });
                resultHandler.handle(Future.succeededFuture(classFieldList));
            } else {
                log.error("Невозможно получить метаданные таблицы: {}", ar.cause().getMessage());
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void executeUpdate(String sql, Handler<AsyncResult<List<Void>>> resultHandler) {
        executor.execute(dsl -> dsl.query(sql)
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                log.debug("Исполнен запрос(executeUpdate) sql: {}, результат: {}", sql, ar.result());
                resultHandler.handle(Future.succeededFuture());
            } else {
                log.error("Ошибка при исполнении запроса(executeUpdate) sql: {}", sql, ar.cause());
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void dropTable(ClassTable classTable, Handler<AsyncResult<Void>> resultHandler) {
        executor.execute(dsl -> dsl.dropTableIfExists(classTable.getName())).setHandler(ar -> {
            if (ar.succeeded()) {
                log.debug("Удаление таблицы [{}] успешно завершено", classTable.getNameWithSchema());
                resultHandler.handle(Future.succeededFuture());
            } else {
                log.error("Ошибка удаления таблицы [{}]", classTable.getNameWithSchema(), ar.cause());
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void executeQuery(String sql, Handler<AsyncResult<ResultSet>> resultHandler) {
        executor.query(dsl -> dsl.resultQuery(sql))
                .setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.debug("Исполнен запрос(executeQuery) sql: {}, результат: {}", sql, ar.result());
                        if (ar.result().unwrap() instanceof ResultSet) {
                            resultHandler.handle(Future.succeededFuture(ar.result().unwrap()));
                        } else {
                            log.error("Невозможно получить результат запроса(executeQuery) sql: {}", sql, ar.cause());
                            resultHandler.handle(Future.failedFuture(String.format("Невозможно получить результат выполнения запроса [%s]", sql)));
                        }
                    } else {
                        log.error("Ошибка при исполнении запроса(executeQuery) sql: {}", sql, ar.cause());
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    @Override
    public void insertDownloadExternalTable(CreateDownloadExternalTableQuery downloadExternalTableQuery, Handler<AsyncResult<Void>> resultHandler) {
        findDatamart(downloadExternalTableQuery.getSchemaName(), datamartHandler -> {
            if (datamartHandler.succeeded()) {
                Long datamartId = datamartHandler.result();
                executor.execute(dsl -> dsl.insertInto(DOWNLOAD_EXTERNAL_TABLE)
                        .set(DOWNLOAD_EXTERNAL_TABLE.SCHEMA_ID, datamartId)
                        .set(DOWNLOAD_EXTERNAL_TABLE.TABLE_NAME, downloadExternalTableQuery.getTableName())
                        .set(DOWNLOAD_EXTERNAL_TABLE.TYPE_ID, downloadExternalTableQuery.getLocationType().ordinal())
                        .set(DOWNLOAD_EXTERNAL_TABLE.LOCATION, downloadExternalTableQuery.getLocationPath())
                        .set(DOWNLOAD_EXTERNAL_TABLE.FORMAT_ID, downloadExternalTableQuery.getFormat().ordinal())
                        .set(DOWNLOAD_EXTERNAL_TABLE.CHUNK_SIZE, downloadExternalTableQuery.getChunkSize())
                )
                        .setHandler(ar -> {
                            if (ar.succeeded()) {
                                resultHandler.handle(Future.succeededFuture());
                            } else {
                                resultHandler.handle(Future.failedFuture(ar.cause()));
                            }
                        });
            } else {
                resultHandler.handle(Future.failedFuture(datamartHandler.cause()));
            }
        });
    }


    @Override
    public void dropDownloadExternalTable(String datamart,
                                          String tableName,
                                          Handler<AsyncResult<Void>> resultHandler) {
        Future.future((Promise<DownloadExtTableRecord> promise) -> {
            findDownloadExternalTable(datamart, tableName.toLowerCase(), promise);
        })
                .compose(deTable -> Future.future((Promise<Long> promise) -> {
                    dropTableAttributesByTableId(deTable.getId(), ar -> {
                        if (ar.succeeded()) {
                            promise.complete(deTable.getId());
                        } else {
                            promise.fail(ar.cause());
                        }
                    });
                }))
                .compose(detId -> Future.future((Promise<Integer> promise) -> dropDownloadExternalTable(detId, promise)))
                .onSuccess(success -> resultHandler.handle(Future.succeededFuture()))
                .onFailure(fail -> resultHandler.handle(Future.failedFuture(fail)));
    }

    private void dropTableAttributesByTableId(Long detId, Handler<AsyncResult<Integer>> handler) {
        executor.execute(dsl -> dsl.deleteFrom(DOWNLOAD_EXTERNAL_TABLE_ATTRIBUTE)
                .where(DOWNLOAD_EXTERNAL_TABLE_ATTRIBUTE.DET_ID.eq(detId)))
                .setHandler(handler);
    }

    private void dropDownloadExternalTable(Long id, Handler<AsyncResult<Integer>> handler) {
        executor.execute(dsl -> dsl.deleteFrom(DOWNLOAD_EXTERNAL_TABLE)
                .where(DOWNLOAD_EXTERNAL_TABLE.ID.eq(id)))
                .setHandler(handler);
    }

    @Override
    public void findDownloadExternalTable(String datamartMnemonic, String table, Handler<AsyncResult<DownloadExtTableRecord>> resultHandler) {
        log.debug("Поиск внешней таблицы {}.{}, начало", datamartMnemonic, table);
    /*
     select det.id from download_external_table det
     inner join datamarts_registry dr on dr.datamart_id=det.schema_id
     where dr.datamart_mnemonics='test' and det.table_name='tblExt';
     */
        executor.query(dsl -> dsl
                .select(DOWNLOAD_EXTERNAL_TABLE.ID,
                        DOWNLOAD_EXTERNAL_TYPE.NAME,
                        DOWNLOAD_EXTERNAL_TABLE.LOCATION,
                        DOWNLOAD_EXTERNAL_FORMAT.NAME,
                        DOWNLOAD_EXTERNAL_TABLE.CHUNK_SIZE
                )
                .from(DOWNLOAD_EXTERNAL_TABLE)
                .join(DATAMARTS_REGISTRY).on(DATAMARTS_REGISTRY.DATAMART_ID.eq(DOWNLOAD_EXTERNAL_TABLE.SCHEMA_ID))
                .join(DOWNLOAD_EXTERNAL_TYPE).on(DOWNLOAD_EXTERNAL_TYPE.ID.eq(DOWNLOAD_EXTERNAL_TABLE.TYPE_ID))
                .join(DOWNLOAD_EXTERNAL_FORMAT).on(DOWNLOAD_EXTERNAL_FORMAT.ID.eq(DOWNLOAD_EXTERNAL_TABLE.FORMAT_ID))
                .where(DOWNLOAD_EXTERNAL_TABLE.TABLE_NAME.equalIgnoreCase(table))
                .and(DATAMARTS_REGISTRY.DATAMART_MNEMONICS.equalIgnoreCase(datamartMnemonic))
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                final QueryResult result = ar.result();
                final boolean found = result.hasResults();
                if (!found) {
                    log.error("Поиск внешней таблицы {}.{}, результат: не найдена", datamartMnemonic, table);
                    resultHandler.handle(
                            Future.failedFuture(String.format("Внешняя таблица %s.%s не найдена", datamartMnemonic, table)));
                    return;
                }
                final Long downloadExtTableId = result.get(DOWNLOAD_EXTERNAL_TABLE.ID);
                final String locationType = result.get(1, String.class); // 1 и 3 поле -- конфликт по имени (name)
                final String locationPath = result.get(DOWNLOAD_EXTERNAL_TABLE.LOCATION);
                final String format = result.get(3, String.class);
                final Integer chunkSize = result.get(DOWNLOAD_EXTERNAL_TABLE.CHUNK_SIZE);

                DownloadExtTableRecord record = new DownloadExtTableRecord();
                record.setId(downloadExtTableId);
                record.setDatamart(datamartMnemonic);
                record.setTableName(table);
                record.setLocationType(Type.findByName(locationType));
                record.setLocationPath(locationPath);
                record.setFormat(Format.findByName(format));
                record.setChunkSize(chunkSize);

                log.debug("Поиск внешней таблицы {}.{}, результат (id): {}", datamartMnemonic, table, downloadExtTableId);
                resultHandler.handle(Future.succeededFuture(record));
            } else {
                log.error("Поиск внешней таблицы {}.{}, ошибка {}", datamartMnemonic, table, ar.cause().getMessage());
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void insertDownloadQuery(UUID id, Long detId, String sql, Handler<AsyncResult<Void>> resultHandler) {
        log.debug("INSERT в таблицу запросов, начало. id: {}, detId: {}, sql: {}", id, detId, sql);
        executor.execute(dsl -> dsl
                .insertInto(DOWNLOAD_QUERY)
                .set(DOWNLOAD_QUERY.ID, id.toString())
                .set(DOWNLOAD_QUERY.DET_ID, detId)
                .set(DOWNLOAD_QUERY.SQL_QUERY, sql))
                .setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.debug("INSERT в таблицу запросов успешен. id: {}, detId: {}, sql: {}", id, detId, sql);
                        resultHandler.handle(Future.succeededFuture());
                    } else {
                        log.error("INSERT в таблицу запросов не успешен. id: {}, detId: {}, sql: {}, error: {}",
                                id, detId, sql, ar.cause().getMessage());
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    @Override
    public void getDeltaOnDateTime(ActualDeltaRequest actualDeltaRequest, Handler<AsyncResult<Long>> resultHandler) {
    /* из постановки:
      SELECT MAX(sinId)
      FROM loaded_deltas_table
      WHERE SysDate <= 'дата-время' AND Status = 1 AND datamartMnemonics = 'datamart'
     */
        final String datamart = actualDeltaRequest.getDatamart();
        final String dateTime = actualDeltaRequest.getDateTime();
        log.debug("Получение дельты витрины {} на {}, начало", datamart, dateTime);
        executor.query(dsl -> getDeltaByDatamartAndDateSelect(dsl, actualDeltaRequest)).setHandler(ar -> {
            if (ar.succeeded()) {
                final Long delta = ar.result().get(0, Long.class);
                log.debug("Дельта витрины {} на дату {}: {}", datamart, dateTime, delta);
                resultHandler.handle(Future.succeededFuture(delta));
            } else {
                log.error("Невозможно получить дельту витрины {} на дату {}: {}",
                        datamart, dateTime, ar.cause().getMessage());
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void getDeltasOnDateTimes(List<ActualDeltaRequest> actualDeltaRequests, Handler<AsyncResult<List<Long>>> resultHandler) {
        log.debug("Получение {} дельт, начало", actualDeltaRequests.size());
        if (actualDeltaRequests.isEmpty()) {
            log.warn("Список запросов на дельты должен быть не пуст.");
            resultHandler.handle(Future.succeededFuture(Collections.emptyList()));
            return;
        }
        executor.query(dsl -> getUnionOfDeltaByDatamartAndDateSelects(dsl, actualDeltaRequests)).setHandler(ar -> {
            if (ar.succeeded()) {
                log.debug("Получение {} дельт, запрос выполнен", actualDeltaRequests.size());
                final List<Long> result = ar.result().stream()
                        .map(queryResult -> queryResult.get(0, Long.class))
                        .collect(Collectors.toList());
                log.debug("Получение {} дельт, результат: {}", actualDeltaRequests.size(), result);
                resultHandler.handle(Future.succeededFuture(result));
            } else {
                log.error("Получение {} дельт, ошибка: {}", actualDeltaRequests.size(), ar.cause().getMessage());
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void getDeltaHotByDatamart(String datamartMnemonic, Handler<AsyncResult<DeltaRecord>> resultHandler) {
        executor.query(dsl -> dsl.select(max(DELTA_DATA.LOAD_ID),
                DELTA_DATA.DATAMART_MNEMONICS,
                DELTA_DATA.SYS_DATE,
                DELTA_DATA.STATUS_DATE,
                DELTA_DATA.SIN_ID,
                DELTA_DATA.LOAD_PROC_ID,
                DELTA_DATA.STATUS)
                .from(DELTA_DATA)
                .where(DELTA_DATA.DATAMART_MNEMONICS.eq(datamartMnemonic))
                .groupBy(DELTA_DATA.LOAD_ID))
                .setHandler(ar -> {
                    initQueryDeltaResult(datamartMnemonic, resultHandler, ar);
                });
    }

    private Select<Record1<Long>> getUnionOfDeltaByDatamartAndDateSelects(DSLContext dsl, List<ActualDeltaRequest> actualDeltaRequests) {
        return actualDeltaRequests.stream()
                .map(adr -> getDeltaByDatamartAndDateSelect(dsl, adr))
                .reduce(Select::unionAll)
                .get();
    }

    private Select<Record1<Long>> getDeltaByDatamartAndDateSelect(DSLContext dsl, ActualDeltaRequest actualDeltaRequest) {
        return dsl.select(max(DELTA_DATA.SIN_ID))
                .from(DELTA_DATA)
                .where(DELTA_DATA.DATAMART_MNEMONICS.equalIgnoreCase(actualDeltaRequest.getDatamart()))
                .and(DELTA_DATA.SYS_DATE.le(LocalDateTime.from(LOCAL_DATE_TIME.parse(actualDeltaRequest.getDateTime()))));
    }

    @Override
    public void getDeltaActualBySinIdAndDatamart(String datamartMnemonic, Long sinId, Handler<AsyncResult<DeltaRecord>> resultHandler) {
        executor.query(dsl -> dsl.select(
                DELTA_DATA.LOAD_ID,
                DELTA_DATA.DATAMART_MNEMONICS,
                DELTA_DATA.SYS_DATE,
                DELTA_DATA.STATUS_DATE,
                DELTA_DATA.SIN_ID,
                DELTA_DATA.LOAD_PROC_ID,
                DELTA_DATA.STATUS)
                .from(DELTA_DATA)
                .where(DELTA_DATA.DATAMART_MNEMONICS.eq(datamartMnemonic)
                        .and(DELTA_DATA.SIN_ID.eq(sinId))
                        .and(DELTA_DATA.STATUS.eq(DeltaLoadStatus.SUCCESS.ordinal()))))
                .setHandler(ar -> {
                    initQueryDeltaResult(datamartMnemonic, resultHandler, ar);
                });
    }

    private void initQueryDeltaResult(String datamartMnemonic, Handler<AsyncResult<DeltaRecord>> resultHandler, AsyncResult<QueryResult> ar) {
        if (ar.succeeded()) {
            final QueryResult result = ar.result();
            if (result.hasResults()) {
                DeltaRecord record = createDeltaRecord(result);
                resultHandler.handle(Future.succeededFuture(record));
            } else {
                resultHandler.handle(Future.succeededFuture(null));
            }
        } else {
            log.error("Поиск дельты для витрины {}, ошибка {}", datamartMnemonic, ar.cause().getMessage());
            resultHandler.handle(Future.failedFuture(ar.cause()));
        }
    }

    @NotNull
    private DeltaRecord createDeltaRecord(QueryResult result) {
        return new DeltaRecord(
                result.get(0, Long.class),
                result.get(1, String.class),
                result.get(2, LocalDateTime.class),
                result.get(3, LocalDateTime.class),
                result.get(4, Long.class),
                result.get(5, String.class),
                DeltaLoadStatus.values()[result.get(6, Integer.class)]
        );
    }

    @Override
    public void insertDelta(DeltaRecord delta, Handler<AsyncResult<Void>> resultHandler) {
        executor.execute(dsl -> dsl.insertInto(DELTA_DATA)
                .set(DELTA_DATA.DATAMART_MNEMONICS, delta.getDatamartMnemonic())
                .set(DELTA_DATA.SYS_DATE, delta.getSysDate())
                .set(DELTA_DATA.STATUS_DATE, delta.getStatusDate())
                .set(DELTA_DATA.SIN_ID, delta.getSinId())
                .set(DELTA_DATA.LOAD_PROC_ID, delta.getLoadProcId())
                .set(DELTA_DATA.STATUS, delta.getStatus().ordinal()))
                .setHandler(ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture());
                    } else {
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    @Override
    public void updateDelta(DeltaRecord delta, Handler<AsyncResult<Void>> resultHandler) {
        executor.execute(dsl -> dsl.update(DELTA_DATA)
                .set(DELTA_DATA.SYS_DATE, delta.getSysDate())
                .set(DELTA_DATA.STATUS_DATE, delta.getStatusDate())
                .set(DELTA_DATA.STATUS, delta.getStatus().ordinal())
                .where(DELTA_DATA.LOAD_ID.eq(delta.getLoadId())))
                .setHandler(ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture());
                    } else {
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    @Override
    public void findDownloadExternalTableAttributes(Long detId, Handler<AsyncResult<List<DownloadExternalTableAttribute>>> resultHandler) {
        executor.query(dsl -> dsl
                .select(DOWNLOAD_EXTERNAL_TABLE_ATTRIBUTE.COLUMN_NAME
                        , DOWNLOAD_EXTERNAL_TABLE_ATTRIBUTE.DET_ID
                        , DOWNLOAD_EXTERNAL_TABLE_ATTRIBUTE.DATA_TYPE
                        , DOWNLOAD_EXTERNAL_TABLE_ATTRIBUTE.ORDER_NUM
                )
                .from(DOWNLOAD_EXTERNAL_TABLE_ATTRIBUTE)
                .where(DOWNLOAD_EXTERNAL_TABLE_ATTRIBUTE.DET_ID.eq(detId))
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                QueryResult result = ar.result();
                ResultSet resultSet = result.unwrap();
                val tableAttributes = new ArrayList<DownloadExternalTableAttribute>();
                resultSet.getRows().forEach(row -> {
                    tableAttributes.add(
                            new DownloadExternalTableAttribute(row.getString(DOWNLOAD_EXTERNAL_TABLE_ATTRIBUTE.COLUMN_NAME.getName()),
                                    row.getString(DOWNLOAD_EXTERNAL_TABLE_ATTRIBUTE.DATA_TYPE.getName()),
                                    row.getInteger(DOWNLOAD_EXTERNAL_TABLE_ATTRIBUTE.ORDER_NUM.getName()),
                                    row.getLong(DOWNLOAD_EXTERNAL_TABLE_ATTRIBUTE.DET_ID.getName())));
                });
                resultHandler.handle(Future.succeededFuture(tableAttributes));
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void insertUploadExternalTable(CreateUploadExternalTableQuery query, Handler<AsyncResult<Void>> handler) {
        findDatamart(query.getSchemaName(), datamartHandler -> {
            if (datamartHandler.succeeded()) {
                Long datamartId = datamartHandler.result();
                executor.execute(dsl -> dsl.insertInto(UPLOAD_EXTERNAL_TABLE)
                        .set(UPLOAD_EXTERNAL_TABLE.DATAMART_ID, datamartId)
                        .set(UPLOAD_EXTERNAL_TABLE.TABLE_NAME, query.getTableName())
                        .set(UPLOAD_EXTERNAL_TABLE.TYPE_ID, query.getLocationType().ordinal())
                        .set(UPLOAD_EXTERNAL_TABLE.LOCATION_PATH, query.getLocationPath())
                        .set(UPLOAD_EXTERNAL_TABLE.FORMAT_ID, query.getFormat().ordinal())
                        .set(UPLOAD_EXTERNAL_TABLE.TABLE_SCHEMA, query.getTableSchema())
                        .set(UPLOAD_EXTERNAL_TABLE.MESSAGE_LIMIT, query.getMessageLimit())
                )
                        .setHandler(ar -> {
                            if (ar.succeeded()) {
                                handler.handle(Future.succeededFuture());
                            } else {
                                handler.handle(Future.failedFuture(ar.cause()));
                            }
                        });
            } else {
                handler.handle(Future.failedFuture(datamartHandler.cause()));
            }
        });
    }

    @Override
    public void dropUploadExternalTable(DropUploadExternalTableQuery query, Handler<AsyncResult<Void>> resultHandler) {
        Future.future((Promise<UploadExternalTableRecord> promise) -> findUploadExternalTable(query.getSchemaName(), query.getTableName().toLowerCase(), promise))
                .compose(uploadExtTableRec -> Future.future((Promise<Integer> promise) -> dropUploadExternalTable(uploadExtTableRec.getId(), promise)))
                .onSuccess(success -> resultHandler.handle(Future.succeededFuture()))
                .onFailure(fail -> resultHandler.handle(Future.failedFuture(fail)));
    }

    private void findUploadExternalTable(String schemaName, String tableName, Handler<AsyncResult<UploadExternalTableRecord>> resultHandler) {
        executor.query(dsl -> dsl
                .select(UPLOAD_EXTERNAL_TABLE.ID,
                        DATAMARTS_REGISTRY.DATAMART_ID,
                        UPLOAD_EXTERNAL_TABLE.TABLE_NAME,
                        UPLOAD_EXTERNAL_TABLE.TYPE_ID,
                        UPLOAD_EXTERNAL_TABLE.LOCATION_PATH,
                        UPLOAD_EXTERNAL_TABLE.FORMAT_ID,
                        UPLOAD_EXTERNAL_TABLE.TABLE_SCHEMA,
                        UPLOAD_EXTERNAL_TABLE.MESSAGE_LIMIT
                )
                .from(UPLOAD_EXTERNAL_TABLE)
                .join(DATAMARTS_REGISTRY).on(DATAMARTS_REGISTRY.DATAMART_ID.eq(UPLOAD_EXTERNAL_TABLE.DATAMART_ID))
                .where(UPLOAD_EXTERNAL_TABLE.TABLE_NAME.equalIgnoreCase(tableName))
                .and(DATAMARTS_REGISTRY.DATAMART_MNEMONICS.equalIgnoreCase(schemaName))
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                final QueryResult result = ar.result();
                final boolean found = result.hasResults();
                if (!found) {
                    log.error("Поиск внешней таблицы {}.{}, результат: не найдена", schemaName, tableName);
                    resultHandler.handle(
                            Future.failedFuture(String.format("Внешняя таблица %s.%s не найдена", schemaName, tableName)));
                    return;
                }
                UploadExternalTableRecord record = createUploadExternalTableRecord(result);
                resultHandler.handle(Future.succeededFuture(record));
            } else {
                log.error("Поиск внешней таблицы {}.{}, ошибка {}", schemaName, tableName, ar.cause().getMessage());
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @NotNull
    private UploadExternalTableRecord createUploadExternalTableRecord(QueryResult result) {
        final Long uploadExtTableId = result.get(UPLOAD_EXTERNAL_TABLE.ID);
        final Long datamartId = result.get(1, Long.class);
        final String tabName = result.get(2, String.class);
        final Integer locationType = result.get(3, Integer.class);
        final String locationPath = result.get(UPLOAD_EXTERNAL_TABLE.LOCATION_PATH);
        final Integer format = result.get(5, Integer.class);
        final String schema = result.get(6, String.class);
        final Integer messageLimit = result.get(UPLOAD_EXTERNAL_TABLE.MESSAGE_LIMIT);

        UploadExternalTableRecord record = new UploadExternalTableRecord();
        record.setId(uploadExtTableId);
        record.setDatamartId(datamartId);
        record.setTableName(tabName);
        record.setTypeId(locationType);
        record.setLocationPath(locationPath);
        record.setFormatId(format);
        record.setTableSchema(schema);
        record.setMessageLimit(messageLimit);
        return record;
    }

    private void dropUploadExternalTable(Long id, Handler<AsyncResult<Integer>> handler) {
        executor.execute(dsl -> dsl.deleteFrom(UPLOAD_EXTERNAL_TABLE)
                .where(UPLOAD_EXTERNAL_TABLE.ID.eq(id)))
                .setHandler(handler);
    }
}
