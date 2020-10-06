package ru.ibs.dtm.query.execution.core.dao.eddl.impl;

import io.github.jklingsporn.vertx.jooq.classic.async.AsyncClassicGenericQueryExecutor;
import io.github.jklingsporn.vertx.jooq.shared.internal.QueryResult;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;
import org.springframework.util.StringUtils;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.plugin.exload.Type;
import ru.ibs.dtm.query.execution.core.dao.eddl.DownloadExtTableDao;
import ru.ibs.dtm.query.execution.core.dto.edml.DownloadExtTableRecord;

import static org.jooq.generated.dtmservice.Tables.*;

@Repository
@Slf4j
public class DownloadExtTableDaoImpl implements DownloadExtTableDao {

    private final AsyncClassicGenericQueryExecutor executor;

    @Autowired
    public DownloadExtTableDaoImpl(@Qualifier("coreQueryExecutor") AsyncClassicGenericQueryExecutor executor) {
        this.executor = executor;
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
