package io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppr;

import io.arenadata.dtm.async.AsyncHandler;
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
import org.springframework.stereotype.Service;

@Slf4j
@Service("adbMpprKafkaService")
public class AdbMpprKafkaService implements MpprKafkaService<QueryResult> {

    private final QueryEnrichmentService adbQueryEnrichmentService;
    private final MetadataSqlFactory metadataSqlFactory;
    private final AdbQueryExecutor adbQueryExecutor;

    public AdbMpprKafkaService(QueryEnrichmentService adbQueryEnrichmentService,
                               MetadataSqlFactory metadataSqlFactory,
                               AdbQueryExecutor adbQueryExecutor) {
        this.adbQueryEnrichmentService = adbQueryEnrichmentService;
        this.metadataSqlFactory = metadataSqlFactory;
        this.adbQueryExecutor = adbQueryExecutor;
    }

    @Override
    public void execute(MpprRequestContext context, AsyncHandler<QueryResult> handler) {
        val request = context.getRequest();
        val schema = request.getQueryRequest().getDatamartMnemonic();
        val table = MetadataSqlFactoryImpl.WRITABLE_EXTERNAL_TABLE_PREF +
                request.getQueryRequest().getRequestId().toString().replaceAll("-", "_");
        createWritableExtTable(request)
                .compose(v -> getEnrichedQuery(request))
                .compose(sql -> executeInsert(schema, table, sql))
                .compose(v -> dropWritableExtTable(schema, table))
                .onSuccess(success -> handler.handleSuccess(QueryResult.emptyResult()))
                .onFailure(err -> {
                    dropWritableExtTable(schema, table)
                            .onComplete(dropResult -> {
                                if (dropResult.failed()) {
                                    log.error("Failed to drop writable external table {}.{}", schema, table);
                                }
                                handler.handleError(new MpprDatasourceException(
                                        String.format("Failed to unload data from datasource by request %s",
                                                context.getRequest()),
                                        err));
                            });
                });
    }

    private Future<String> getEnrichedQuery(MpprRequest request) {
        return Future.future(promise -> adbQueryEnrichmentService.enrich(
                EnrichQueryRequest.generate(request.getQueryRequest(), request.getLogicalSchema()), promise));
    }

    private Future<Void> createWritableExtTable(MpprRequest request) {
        return Future.future(p -> adbQueryExecutor.executeUpdate(metadataSqlFactory.createWritableExtTableSqlQuery(request), p));
    }

    private Future<Void> executeInsert(String schema, String table, String enrichedSql) {
        return Future.future(p -> {
            adbQueryExecutor.executeUpdate(metadataSqlFactory.insertIntoWritableExtTableSqlQuery(schema, table, enrichedSql), p);
        });
    }

    private Future<Void> dropWritableExtTable(String schema, String table) {
        return Future.future(p -> {
            adbQueryExecutor.executeUpdate(metadataSqlFactory.dropWritableExtTableSqlQuery(schema, table), p);
        });
    }


}
