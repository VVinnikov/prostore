package io.arenadata.dtm.query.execution.core.factory.impl;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlUseSchema;
import io.arenadata.dtm.query.calcite.core.extension.delta.SqlBeginDelta;
import io.arenadata.dtm.query.calcite.core.extension.delta.SqlCommitDelta;
import io.arenadata.dtm.query.calcite.core.extension.delta.function.SqlGetDeltaByDateTime;
import io.arenadata.dtm.query.calcite.core.extension.delta.function.SqlGetDeltaByNum;
import io.arenadata.dtm.query.calcite.core.extension.delta.function.SqlGetDeltaHot;
import io.arenadata.dtm.query.calcite.core.extension.delta.function.SqlGetDeltaOk;
import io.arenadata.dtm.query.calcite.core.extension.delta.SqlRollbackDelta;
import io.arenadata.dtm.query.execution.core.factory.RequestContextFactory;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.dml.DmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.eddl.EddlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
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
        if (isDdlRequest(node)) {
            switch (node.getKind()) {
                case OTHER_DDL:
                    return new EddlRequestContext(new DatamartRequest(changedQueryRequest));
                default:
                    return new DdlRequestContext(new DdlRequest(changedQueryRequest), node);
            }
        } else if (isDeltaRequest(node)) {
            return new DeltaRequestContext(new DatamartRequest(changedQueryRequest));
        } else if (node instanceof SqlUseSchema) {
            return new DdlRequestContext(new DdlRequest(changedQueryRequest), node);
        }

        switch (node.getKind()) {
            case INSERT:
                return new EdmlRequestContext(new DatamartRequest(changedQueryRequest), (SqlInsert) node);
            default:
                return new DmlRequestContext(new DmlRequest(changedQueryRequest), node);
        }
    }

    private boolean isDdlRequest(SqlNode node) {
        return node instanceof SqlDdl || node instanceof SqlAlter;
    }

    private boolean isDeltaRequest(SqlNode node) {
        return node instanceof SqlBeginDelta
                || node instanceof SqlCommitDelta
                || node instanceof SqlGetDeltaOk
                || node instanceof SqlGetDeltaHot
                || node instanceof SqlGetDeltaByDateTime
                || node instanceof SqlGetDeltaByNum
                || node instanceof SqlRollbackDelta;
    }

    private QueryRequest changeSql(QueryRequest request, SqlNode node) {
        request.setSql(node.toSqlString(sqlDialect).getSql());
        return request;
    }

}
