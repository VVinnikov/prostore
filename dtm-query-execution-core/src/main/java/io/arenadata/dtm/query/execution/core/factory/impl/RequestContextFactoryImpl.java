package io.arenadata.dtm.query.execution.core.factory.impl;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.RequestStatus;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.extension.check.SqlCheckCall;
import io.arenadata.dtm.query.calcite.core.extension.config.SqlConfigCall;
import io.arenadata.dtm.query.calcite.core.extension.ddl.truncate.SqlBaseTruncate;
import io.arenadata.dtm.query.calcite.core.extension.delta.SqlDeltaCall;
import io.arenadata.dtm.query.calcite.core.extension.dml.SqlDataSourceTypeGetter;
import io.arenadata.dtm.query.execution.core.configuration.AppConfiguration;
import io.arenadata.dtm.query.execution.core.dto.CoreRequestContext;
import io.arenadata.dtm.query.execution.core.dto.check.CheckContext;
import io.arenadata.dtm.query.execution.core.dto.config.ConfigRequestContext;
import io.arenadata.dtm.query.execution.core.dto.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.dto.delta.operation.DeltaRequestContext;
import io.arenadata.dtm.query.execution.core.dto.dml.DmlRequest;
import io.arenadata.dtm.query.execution.core.dto.dml.DmlRequestContext;
import io.arenadata.dtm.query.execution.core.dto.eddl.EddlRequestContext;
import io.arenadata.dtm.query.execution.core.dto.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.core.factory.RequestContextFactory;
import io.arenadata.dtm.query.execution.plugin.api.request.ConfigRequest;
import lombok.val;
import org.apache.calcite.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Optional;

@Component
public class RequestContextFactoryImpl implements RequestContextFactory<CoreRequestContext<? extends DatamartRequest, ? extends SqlNode>, QueryRequest> {
    private final SqlDialect sqlDialect;
    private final AppConfiguration coreConfiguration;

    @Autowired
    public RequestContextFactoryImpl(@Qualifier("coreSqlDialect") SqlDialect sqlDialect,
                                     AppConfiguration coreConfiguration) {
        this.sqlDialect = sqlDialect;
        this.coreConfiguration = coreConfiguration;
    }

    @Override
    public CoreRequestContext<? extends DatamartRequest, ? extends SqlNode> create(QueryRequest request,
                                                                                   SqlNode node) {
        //val changedQueryRequest = changeSql(request, node);
        val envName = coreConfiguration.getEnvName();
        if (isConfigRequest(node)) {
            return ConfigRequestContext.builder()
                    .request(new ConfigRequest(request))
                    .envName(envName)
                    .metrics(createRequestMetrics(request))
                    .sqlConfigCall((SqlConfigCall) node)
                    .build();
        } else if (isDdlRequest(node)) {
            switch (node.getKind()) {
                case OTHER_DDL:
                    if (node instanceof SqlBaseTruncate) {
                        return new DdlRequestContext(
                                createRequestMetrics(request),
                                new DatamartRequest(request),
                                node,
                                null,
                                envName);
                    } else {
                        return EddlRequestContext.builder()
                                .request(new DatamartRequest(request))
                                .envName(envName)
                                .metrics(createRequestMetrics(request))
                                .sqlNode(node)
                                .build();
                    }
                default:
                    return new DdlRequestContext(
                            createRequestMetrics(request),
                            new DatamartRequest(request),
                            node,
                            null,
                            envName);
            }
        } else if (node instanceof SqlDeltaCall) {
            return new DeltaRequestContext(
                    createRequestMetrics(request),
                    new DatamartRequest(request),
                    envName,
                    (SqlDeltaCall) node);
        } else if (SqlKind.CHECK.equals(node.getKind())) {
            SqlCheckCall sqlCheckCall = (SqlCheckCall) node;
            Optional.ofNullable(sqlCheckCall.getSchema()).ifPresent(request::setDatamartMnemonic);
            return CheckContext.builder()
                    .request(new DatamartRequest(request))
                    .envName(envName)
                    .metrics(createRequestMetrics(request))
                    .checkType(sqlCheckCall.getType())
                    .sqlCheckCall(sqlCheckCall)
                    .build();
        }

        switch (node.getKind()) {
            case INSERT:
                return new EdmlRequestContext(
                        createRequestMetrics(request),
                        new DatamartRequest(request),
                        (SqlInsert) node,
                        envName);
            default:
                return DmlRequestContext.builder()
                        .request(new DmlRequest(request))
                        .envName(envName)
                        .metrics(createRequestMetrics(request))
                        .sourceType(getDmlSourceType(node))
                        .sqlNode(node)
                        .build();
        }
    }

    private SourceType getDmlSourceType(SqlNode node) {
        if (node instanceof SqlDataSourceTypeGetter) {
            SqlCharStringLiteral dsTypeNode = ((SqlDataSourceTypeGetter) node).getDatasourceType();
            if (dsTypeNode != null) {
                return SourceType.valueOfAvailable(dsTypeNode.getNlsString().getValue());
            }
        }
        return null;
    }

    private RequestMetrics createRequestMetrics(QueryRequest request) {
        return RequestMetrics.builder()
                .startTime(LocalDateTime.now(coreConfiguration.dtmSettings().getTimeZone()))
                .requestId(request.getRequestId())
                .status(RequestStatus.IN_PROCESS)
                .isActive(true)
                .build();
    }

    private boolean isConfigRequest(SqlNode node) {
        return node instanceof SqlConfigCall;
    }

    private boolean isDdlRequest(SqlNode node) {
        return node instanceof SqlDdl || node instanceof SqlAlter || node instanceof SqlBaseTruncate;
    }

    private QueryRequest changeSql(QueryRequest request, SqlNode node) {
        request.setSql(node.toSqlString(sqlDialect).getSql());
        return request;
    }

}
