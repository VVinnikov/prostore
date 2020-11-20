package io.arenadata.dtm.query.execution.core.factory.impl;

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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class RequestContextFactoryImpl implements RequestContextFactory<RequestContext<? extends DatamartRequest>, QueryRequest> {
    private final SqlDialect sqlDialect;

    public RequestContextFactoryImpl(@Qualifier("coreSqlDialect") SqlDialect sqlDialect) {
        this.sqlDialect = sqlDialect;
    }

    @Override
    public RequestContext<? extends DatamartRequest> create(QueryRequest request, SqlNode node) {
        val changedQueryRequest = changeSql(request, node);
        if (isConfigRequest(node)) {
            return new ConfigRequestContext(new ConfigRequest(request), (SqlConfigCall) node);
        } else if (isDdlRequest(node)) {
            switch (node.getKind()) {
                case OTHER_DDL:
                    return new EddlRequestContext(new DatamartRequest(changedQueryRequest));
                default:
                    return new DdlRequestContext(new DdlRequest(changedQueryRequest), node);
            }
        } else if (node instanceof SqlDeltaCall) {
            return new DeltaRequestContext(new DatamartRequest(changedQueryRequest));
        }

        switch (node.getKind()) {
            case INSERT:
                return new EdmlRequestContext(new DatamartRequest(changedQueryRequest), (SqlInsert) node);
            default:
                return new DmlRequestContext(new DmlRequest(changedQueryRequest), node);
        }
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
