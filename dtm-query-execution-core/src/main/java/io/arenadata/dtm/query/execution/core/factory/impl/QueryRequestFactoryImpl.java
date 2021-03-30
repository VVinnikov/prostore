package io.arenadata.dtm.query.execution.core.factory.impl;

import io.arenadata.dtm.common.reader.InputQueryRequest;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.core.factory.QueryRequestFactory;
import org.springframework.stereotype.Component;

@Component
public class QueryRequestFactoryImpl implements QueryRequestFactory {

    @Override
    public QueryRequest create(InputQueryRequest inputQueryRequest) {
        return QueryRequest.builder()
                .requestId(inputQueryRequest.getRequestId())
                .datamartMnemonic(inputQueryRequest.getDatamartMnemonic())
                .sql(inputQueryRequest.getSql())
                .parameters(inputQueryRequest.getParameters())
                .isPrepare(!inputQueryRequest.isExecutable())//FIXME to more understandable init
                .build();
    }
}
