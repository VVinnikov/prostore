package io.arenadata.dtm.query.execution.core.factory.impl;

import io.arenadata.dtm.common.reader.InputQueryRequest;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.core.configuration.AppConfiguration;
import io.arenadata.dtm.query.execution.core.factory.QueryRequestFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class QueryRequestFactoryImpl implements QueryRequestFactory {

    private final AppConfiguration configuration;

    @Autowired
    public QueryRequestFactoryImpl(AppConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public QueryRequest create(InputQueryRequest inputQueryRequest) {
        return QueryRequest.builder()
                .requestId(inputQueryRequest.getRequestId())
                .datamartMnemonic(inputQueryRequest.getDatamartMnemonic())
                .sql(inputQueryRequest.getSql())
                .parameters(inputQueryRequest.getParameters())
                .envName(configuration.getEnvName())
                .build();
    }
}
