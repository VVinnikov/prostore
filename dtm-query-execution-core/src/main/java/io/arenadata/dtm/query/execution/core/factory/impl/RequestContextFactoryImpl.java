package io.arenadata.dtm.query.execution.core.factory.impl;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.RequestStatus;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.calcite.core.extension.config.SqlConfigCall;
import io.arenadata.dtm.query.calcite.core.extension.delta.SqlDeltaCall;
import io.arenadata.dtm.query.execution.core.factory.RequestContextFactory;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.config.ConfigRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.dml.DmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.eddl.EddlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.ConfigRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.DmlRequest;
import lombok.val;
import org.apache.calcite.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class RequestContextFactoryImpl implements RequestContextFactory<RequestContext<? extends DatamartRequest>, QueryRequest> {
    private final SqlDialect sqlDialect;
    private final DtmConfig dtmSettings;

    @Autowired
    public RequestContextFactoryImpl(@Qualifier("coreSqlDialect") SqlDialect sqlDialect,
                                     DtmConfig dtmSettings) {
        this.sqlDialect = sqlDialect;
        this.dtmSettings = dtmSettings;
    }

    @Override
    public RequestContext<? extends DatamartRequest> create(QueryRequest request, SqlNode node) {
        val changedQueryRequest = changeSql(request, node);
        if (isConfigRequest(node)) {
            return new ConfigRequestContext(new ConfigRequest(request), (SqlConfigCall) node);
        } else if (isDdlRequest(node)) {
            switch (node.getKind()) {
                case OTHER_DDL:
                    return new EddlRequestContext(
                            createRequestMetrics(request),
                            new DatamartRequest(changedQueryRequest));
                default:
                    return new DdlRequestContext(
                            createRequestMetrics(request),
                            new DdlRequest(changedQueryRequest), node);
            }
        } else if (node instanceof SqlDeltaCall) {
            return new DeltaRequestContext(
                    createRequestMetrics(request),
                    new DatamartRequest(changedQueryRequest));
        }

        switch (node.getKind()) {
            case INSERT:
                return new EdmlRequestContext(
                        createRequestMetrics(request),
                        new DatamartRequest(changedQueryRequest), (SqlInsert) node);
            default:
                return new DmlRequestContext(
                        createRequestMetrics(request),
                        new DmlRequest(changedQueryRequest), node);
        }
    }

    private RequestMetrics createRequestMetrics(QueryRequest request) {
        return RequestMetrics.builder()
                .startTime(LocalDateTime.now(dtmSettings.getTimeZone()))
                .requestId(request.getRequestId())
                .status(RequestStatus.IN_PROCESS)
                .isActive(true)
                .build();
    }

    private boolean isConfigRequest(SqlNode node) {
        return node instanceof SqlConfigCall;
    }

    private boolean isDdlRequest(SqlNode node) {
        return node instanceof SqlDdl || node instanceof SqlAlter;
    }

    private QueryRequest changeSql(QueryRequest request, SqlNode node) {
        request.setSql(node.toSqlString(sqlDialect).getSql());
        return request;
    }

}
