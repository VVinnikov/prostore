package ru.ibs.dtm.query.execution.core.factory.impl;

import lombok.val;
import org.apache.calcite.sql.*;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.calcite.core.extension.ddl.SqlUseSchema;
import ru.ibs.dtm.query.calcite.core.extension.delta.SqlBeginDelta;
import ru.ibs.dtm.query.calcite.core.extension.delta.SqlCommitDelta;
import ru.ibs.dtm.query.execution.core.factory.RequestContextFactory;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.dml.DmlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.eddl.EddlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;
import ru.ibs.dtm.query.execution.plugin.api.request.DdlRequest;
import ru.ibs.dtm.query.execution.plugin.api.request.DmlRequest;

@Component
public class RequestContextFactoryImpl implements RequestContextFactory<RequestContext<? extends DatamartRequest>, QueryRequest> {
    private final SqlDialect sqlDialect;

    public RequestContextFactoryImpl(@Qualifier("coreSqlDialect") SqlDialect sqlDialect) {
        this.sqlDialect = sqlDialect;
    }

    @Override
    public RequestContext<? extends DatamartRequest> create(QueryRequest request, SqlNode node) {
        val changedQueryRequest = changeSql(request, node);
        if (node instanceof SqlDdl || node instanceof SqlAlter) {
            switch (node.getKind()) {
                case OTHER_DDL:
                    return new EddlRequestContext(new DatamartRequest(changedQueryRequest));
                default:
                    return new DdlRequestContext(new DdlRequest(changedQueryRequest), node);
            }
        } else if (node instanceof SqlBeginDelta || node instanceof SqlCommitDelta) {
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

    private QueryRequest changeSql(QueryRequest request, SqlNode node) {
        request.setSql(node.toSqlString(sqlDialect).getSql());
        return request;
    }

}
