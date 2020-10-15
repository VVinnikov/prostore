package ru.ibs.dtm.query.execution.core.factory.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.InputQueryRequest;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.core.configuration.AppConfiguration;
import ru.ibs.dtm.query.execution.core.factory.QueryRequestFactory;

import java.util.UUID;

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
                .requestId(UUID.randomUUID())
                .datamartMnemonic(inputQueryRequest.getDatamartMnemonic())
                .sql(inputQueryRequest.getSql())
                .parameters(inputQueryRequest.getParameters())
                .envName(configuration.getEnvName())
                .build();
    }
}
