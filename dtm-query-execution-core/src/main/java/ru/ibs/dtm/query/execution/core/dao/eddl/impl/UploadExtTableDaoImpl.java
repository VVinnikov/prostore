package ru.ibs.dtm.query.execution.core.dao.eddl.impl;

import io.github.jklingsporn.vertx.jooq.classic.async.AsyncClassicGenericQueryExecutor;
import io.github.jklingsporn.vertx.jooq.shared.internal.QueryResult;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.plugin.exload.Type;
import ru.ibs.dtm.query.execution.core.dao.eddl.UploadExtTableDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.DatamartDao;
import ru.ibs.dtm.query.execution.core.dto.eddl.CreateUploadExternalTableQuery;
import ru.ibs.dtm.query.execution.core.dto.eddl.DropUploadExternalTableQuery;
import ru.ibs.dtm.query.execution.core.dto.edml.UploadExtTableRecord;

import static org.jooq.generated.dtmservice.Tables.DATAMARTS_REGISTRY;
import static org.jooq.generated.dtmservice.Tables.UPLOAD_EXTERNAL_TABLE;

@Repository
@Slf4j
public class UploadExtTableDaoImpl implements UploadExtTableDao {

    private final AsyncClassicGenericQueryExecutor executor;
    private final DatamartDao datamartDao;

    @Autowired
    public UploadExtTableDaoImpl(@Qualifier("coreQueryExecutor") AsyncClassicGenericQueryExecutor executor, DatamartDao datamartDao) {
        this.executor = executor;
        this.datamartDao = datamartDao;
    }

    @Override
    public void findUploadExternalTable(String schemaName, String tableName, Handler<AsyncResult<UploadExtTableRecord>> resultHandler) {
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
                UploadExtTableRecord record = createUploadExternalTableRecord(result);
                resultHandler.handle(Future.succeededFuture(record));
            } else {
                log.error("Поиск внешней таблицы {}.{}, ошибка {}", schemaName, tableName, ar.cause().getMessage());
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @NotNull
    private UploadExtTableRecord createUploadExternalTableRecord(QueryResult result) {
        final Long uploadExtTableId = result.get(UPLOAD_EXTERNAL_TABLE.ID);
        final Long datamartId = result.get(1, Long.class);
        final String tabName = result.get(2, String.class);
        final Integer locationType = result.get(3, Integer.class);
        final String locationPath = result.get(UPLOAD_EXTERNAL_TABLE.LOCATION_PATH);
        final Integer format = result.get(5, Integer.class);
        final String schema = result.get(6, String.class);
        final Integer messageLimit = result.get(UPLOAD_EXTERNAL_TABLE.MESSAGE_LIMIT);

        UploadExtTableRecord record = new UploadExtTableRecord();
        record.setId(uploadExtTableId);
        record.setDatamartId(datamartId);
        record.setTableName(tabName);
        record.setLocationType(Type.values()[locationType]);
        record.setLocationPath(locationPath);
        record.setFormat(Format.values()[format]);
        record.setTableSchema(JsonObject.mapFrom(schema));
        record.setMessageLimit(messageLimit);
        return record;
    }

    @Override
    public void insertUploadExternalTable(CreateUploadExternalTableQuery query, Handler<AsyncResult<Void>> resultHandler) {
        datamartDao.findDatamart(query.getSchemaName(), datamartHandler -> {
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
                ).setHandler(ar -> {
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
    public void dropUploadExternalTable(DropUploadExternalTableQuery query, Handler<AsyncResult<Void>> resultHandler) {
        Future.future((Promise<UploadExtTableRecord> promise) -> findUploadExternalTable(query.getSchemaName(),
                query.getTableName().toLowerCase(), promise))
                .compose(uploadExtTableRec -> Future.future((Promise<Void> promise) ->
                        this.dropUploadExternalTableById(uploadExtTableRec.getId(), promise)))
                .onSuccess(success -> resultHandler.handle(Future.succeededFuture()))
                .onFailure(fail -> resultHandler.handle(Future.failedFuture(fail)));
    }

    @Override
    public void dropUploadExternalTableById(Long uploadExtTableId, Handler<AsyncResult<Void>> resultHandler) {
        executor.execute(dsl -> dsl.deleteFrom(UPLOAD_EXTERNAL_TABLE)
                .where(UPLOAD_EXTERNAL_TABLE.ID.eq(uploadExtTableId)))
                .setHandler(ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture());
                    } else {
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }
}
