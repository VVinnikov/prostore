package io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppr;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adb.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.factory.impl.MetadataSqlFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.adb.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.exception.MpprDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprPluginRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.MpprKafkaService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service("adbMpprKafkaService")
public class AdbMpprKafkaService implements MpprKafkaService {

    private final QueryEnrichmentService adbQueryEnrichmentService;
    private final MetadataSqlFactory metadataSqlFactory;
    private final AdbQueryExecutor adbQueryExecutor;

    @Autowired
    public AdbMpprKafkaService(QueryEnrichmentService adbQueryEnrichmentService,
                               MetadataSqlFactory metadataSqlFactory,
                               AdbQueryExecutor adbQueryExecutor) {
        this.adbQueryEnrichmentService = adbQueryEnrichmentService;
        this.metadataSqlFactory = metadataSqlFactory;
        this.adbQueryExecutor = adbQueryExecutor;
    }

    @Override
    public Future<QueryResult> execute(MpprPluginRequest request) {
        return Future.future(promise -> {
            val schema = request.getDatamartMnemonic();
            val table = MetadataSqlFactoryImpl.WRITABLE_EXTERNAL_TABLE_PREF +
                    request.getRequestId().toString().replaceAll("-", "_");
            adbQueryExecutor.executeUpdate(metadataSqlFactory.createWritableExtTableSqlQuery(request.getMpprRequest()))
                    .compose(v -> enrichQuery(request))
                    .compose(enrichedQuery -> insertIntoWritableExtTableSqlQuery(schema, table, enrichedQuery))
                    .compose(v -> dropWritableExtTableSqlQuery(schema, table))
                    .onSuccess(success -> promise.complete(QueryResult.emptyResult()))
                    .onFailure(err -> dropWritableExtTableSqlQuery(schema, table)
                            .onComplete(dropResult -> {
                                if (dropResult.failed()) {
                                    log.error("Failed to drop writable external table {}.{}", schema, table);
                                }
                                promise.fail(new MpprDatasourceException(
                                        String.format("Failed to unload data from datasource by request %s",
                                                request.getMpprRequest()),
                                        err));
                            }));
        });
    }

    private Future<Void> dropWritableExtTableSqlQuery(String schema, String table) {
        return adbQueryExecutor.executeUpdate(
                metadataSqlFactory.dropWritableExtTableSqlQuery(schema,
                        table));
    }

    private Future<Void> insertIntoWritableExtTableSqlQuery(String schema, String table, String sql) {
        return adbQueryExecutor.executeUpdate(
                metadataSqlFactory.insertIntoWritableExtTableSqlQuery(schema,
                        table,
                        sql));
    }

    private Future<String> enrichQuery(MpprPluginRequest request) {
        return adbQueryEnrichmentService.enrich(
                EnrichQueryRequest.generate(request.getMpprRequest().getQueryRequest(),
                        request.getMpprRequest().getLogicalSchema(), request.getSqlNode()));
    }
}
