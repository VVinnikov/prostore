package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.SystemMetadata;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.dml.DmlType;
import io.arenadata.dtm.query.calcite.core.extension.dml.SqlUseSchema;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.exception.datamart.DatamartNotExistsException;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsService;
import io.arenadata.dtm.query.execution.core.utils.ParseQueryUtils;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.api.dml.DmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.dml.DmlExecutor;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class UseSchemaDmlExecutor implements DmlExecutor<QueryResult> {

    public static final String SCHEMA_COLUMN_NAME = "schema";
    private final DatamartDao datamartDao;
    private final ParseQueryUtils parseQueryUtils;
    private final MetricsService<RequestMetrics> metricsService;

    @Autowired
    public UseSchemaDmlExecutor(ServiceDbFacade serviceDbFacade,
                                ParseQueryUtils parseQueryUtils,
                                MetricsService<RequestMetrics> metricsService) {
        this.datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
        this.parseQueryUtils = parseQueryUtils;
        this.metricsService = metricsService;
    }

    @Override
    public Future<QueryResult> execute(DmlRequestContext context) {
        return sendMetricsAndExecute(context);
    }

    private Future<QueryResult> sendMetricsAndExecute(DmlRequestContext context) {
        return Future.future(promise -> {
            String datamart = parseQueryUtils.getDatamartName(((SqlUseSchema) context.getQuery()).getOperandList());
            datamartDao.existsDatamart(datamart)
                .onComplete(metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                    SqlProcessingType.DML,
                    context.getMetrics(),
                    ar -> {
                        if (ar.succeeded()) {
                            if (ar.result()) {
                                promise.complete(createQueryResult(context, datamart));
                            } else {
                                promise.fail(new DatamartNotExistsException(datamart));
                            }
                        } else {
                            promise.fail(ar.cause());
                        }
                    }));
        });
    }

    private QueryResult createQueryResult(DmlRequestContext context, String datamart) {
        Map<String, Object> rowMap = new HashMap<>();
        rowMap.put(SCHEMA_COLUMN_NAME, datamart);
        return QueryResult.builder()
            .metadata(Collections.singletonList(new ColumnMetadata(SCHEMA_COLUMN_NAME, SystemMetadata.SCHEMA, ColumnType.VARCHAR)))
            .requestId(context.getRequest().getQueryRequest().getRequestId())
            .result(Collections.singletonList(rowMap))
            .build();
    }

    @Override
    public DmlType getType() {
        return DmlType.USE;
    }
}
