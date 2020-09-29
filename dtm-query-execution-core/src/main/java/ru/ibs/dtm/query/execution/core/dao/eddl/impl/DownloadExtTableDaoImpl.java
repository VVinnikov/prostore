package ru.ibs.dtm.query.execution.core.dao.eddl.impl;

import io.github.jklingsporn.vertx.jooq.classic.async.AsyncClassicGenericQueryExecutor;
import io.github.jklingsporn.vertx.jooq.shared.internal.QueryResult;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;
import org.springframework.util.StringUtils;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.plugin.exload.Type;
import ru.ibs.dtm.query.execution.core.dao.eddl.DownloadExtTableAttributeDao;
import ru.ibs.dtm.query.execution.core.dao.eddl.DownloadExtTableDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.DatamartDao;
import ru.ibs.dtm.query.execution.core.dto.eddl.CreateDownloadExternalTableQuery;
import ru.ibs.dtm.query.execution.core.dto.edml.DownloadExtTableRecord;

import static org.jooq.generated.dtmservice.Tables.*;

@Repository
@Slf4j
public class DownloadExtTableDaoImpl implements DownloadExtTableDao {

    private final AsyncClassicGenericQueryExecutor executor;
    private final DatamartDao datamartDao;
    private final DownloadExtTableAttributeDao downloadExtTableAttributeDao;

    @Autowired
    public DownloadExtTableDaoImpl(@Qualifier("coreQueryExecutor") AsyncClassicGenericQueryExecutor executor,
                                   DatamartDao datamartDao, DownloadExtTableAttributeDao downloadExtTableAttributeDao) {
        this.executor = executor;
        this.datamartDao = datamartDao;
        this.downloadExtTableAttributeDao = downloadExtTableAttributeDao;
    }

    @Override
    public void insertDownloadExternalTable(CreateDownloadExternalTableQuery downloadExternalTableQuery, Handler<AsyncResult<Void>> resultHandler) {
        datamartDao.findDatamart(downloadExternalTableQuery.getSchemaName(), datamartHandler -> {
            if (datamartHandler.succeeded()) {
                Long datamartId = datamartHandler.result();
                executor.execute(dsl -> dsl.insertInto(DOWNLOAD_EXTERNAL_TABLE)
                        .set(DOWNLOAD_EXTERNAL_TABLE.SCHEMA_ID, datamartId)
                        .set(DOWNLOAD_EXTERNAL_TABLE.TABLE_NAME, downloadExternalTableQuery.getTableName())
                        .set(DOWNLOAD_EXTERNAL_TABLE.TYPE_ID, downloadExternalTableQuery.getLocationType().ordinal())
                        .set(DOWNLOAD_EXTERNAL_TABLE.LOCATION, downloadExternalTableQuery.getLocationPath())
                        .set(DOWNLOAD_EXTERNAL_TABLE.FORMAT_ID, downloadExternalTableQuery.getFormat().ordinal())
                        .set(DOWNLOAD_EXTERNAL_TABLE.CHUNK_SIZE, downloadExternalTableQuery.getChunkSize())
                        .set(DOWNLOAD_EXTERNAL_TABLE.TABLE_SCHEMA, downloadExternalTableQuery.getTableSchema())
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
    public void dropDownloadExternalTable(String datamart, String tableName, Handler<AsyncResult<Void>> resultHandler) {
        Future.future((Promise<DownloadExtTableRecord> promise) -> {
            findDownloadExternalTable(datamart, tableName.toLowerCase(), promise);
        })
                .compose(deTable -> Future.future((Promise<Long> promise) -> {
                    downloadExtTableAttributeDao.dropDownloadExtTableAttributesByTableId(deTable.getId(), ar -> {
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

    private void dropDownloadExternalTable(Long id, Handler<AsyncResult<Integer>> handler) {
        executor.execute(dsl -> dsl.deleteFrom(DOWNLOAD_EXTERNAL_TABLE)
                .where(DOWNLOAD_EXTERNAL_TABLE.ID.eq(id)))
                .setHandler(handler);
    }

    @Override
    public void findDownloadExternalTable(String datamartMnemonic, String table, Handler<AsyncResult<DownloadExtTableRecord>> resultHandler) {
        executor.query(dsl -> dsl
                .select(DOWNLOAD_EXTERNAL_TABLE.ID,
                        DOWNLOAD_EXTERNAL_TYPE.NAME,
                        DOWNLOAD_EXTERNAL_TABLE.LOCATION,
                        DOWNLOAD_EXTERNAL_FORMAT.NAME,
                        DOWNLOAD_EXTERNAL_TABLE.CHUNK_SIZE,
                        DATAMARTS_REGISTRY.DATAMART_ID,
                        DOWNLOAD_EXTERNAL_TABLE.TABLE_SCHEMA
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
                    log.error("Search external table {}.{}, Result: not found", datamartMnemonic, table);
                    resultHandler.handle(
                            Future.failedFuture(String.format("External table %s.%s not found", datamartMnemonic, table)));
                    return;
                }
                final Long downloadExtTableId = result.get(DOWNLOAD_EXTERNAL_TABLE.ID);
                final String locationType = result.get(1, String.class); // 1 и 3 поле -- конфликт по имени (name)
                final String locationPath = result.get(DOWNLOAD_EXTERNAL_TABLE.LOCATION);
                final String format = result.get(3, String.class);
                final Integer chunkSize = result.get(DOWNLOAD_EXTERNAL_TABLE.CHUNK_SIZE);
                final Long datamartId = result.get(5, Long.class);
                String tableSchema = result.get(6, String.class);
                tableSchema = StringUtils.hasText(tableSchema) ? tableSchema : "";

                DownloadExtTableRecord record = new DownloadExtTableRecord();
                record.setId(downloadExtTableId);
                record.setDatamartId(datamartId);
                record.setTableName(table);
                record.setLocationType(Type.findByName(locationType));
                record.setLocationPath(locationPath);
                record.setFormat(Format.findByName(format));
                record.setChunkSize(chunkSize);
                record.setTableSchema(new JsonObject(tableSchema));

                log.debug("Search external table {}.{}, Result (id): {}", datamartMnemonic, table, downloadExtTableId);
                resultHandler.handle(Future.succeededFuture(record));
            } else {
                log.error("Search external table {}.{}, Error {}", datamartMnemonic, table, ar.cause().getMessage());
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }
}
