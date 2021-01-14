package io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppr;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adb.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.factory.impl.MetadataSqlFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.adb.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.exception.MpprDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.MpprRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.MpprKafkaService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service("adbMpprKafkaService")
public class AdbMpprKafkaService implements MpprKafkaService<QueryResult> {

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
    public Future<QueryResult> execute(MpprRequestContext context) {
        return Future.future(promise -> {
            val request = context.getRequest();
            val schema = request.getQueryRequest().getDatamartMnemonic();
            val table = MetadataSqlFactoryImpl.WRITABLE_EXTERNAL_TABLE_PREF +
                    request.getQueryRequest().getRequestId().toString().replaceAll("-", "_");
            adbQueryExecutor.executeUpdate(metadataSqlFactory.createWritableExtTableSqlQuery(request))
                    .compose(v -> enrichQuery(context, request))
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
                                                context.getRequest()),
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

    private Future<String> enrichQuery(MpprRequestContext context, MpprRequest request) {
        return adbQueryEnrichmentService.enrich(
                EnrichQueryRequest.generate(request.getQueryRequest(),
                        request.getLogicalSchema(),
                        context.getQuery()));
    }
}
