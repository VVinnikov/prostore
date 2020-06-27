package ru.ibs.dtm.query.execution.core.factory.impl;

import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.core.calcite.delta.SqlBeginDelta;
import ru.ibs.dtm.query.execution.core.calcite.delta.SqlCommitDelta;
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

    @Override
    public RequestContext<? extends DatamartRequest> create(QueryRequest request, SqlNode node) {
        if (node instanceof SqlDdl) {
            switch (node.getKind()) {
                case OTHER_DDL:
                    return new EddlRequestContext(new DatamartRequest(request));
                default:
                    return new DdlRequestContext(new DdlRequest(request), node);
            }
        } else if (node instanceof SqlBeginDelta || node instanceof SqlCommitDelta) {
            return new DeltaRequestContext(new DatamartRequest(request));
        }

        switch (node.getKind()) {
            case INSERT:
                return new EdmlRequestContext(new DatamartRequest(request), (SqlInsert) node);
            default:
                return new DmlRequestContext(new DmlRequest(request), node);
        }
    }

}
