package io.arenadata.dtm.query.execution.plugin.adqm.factory.impl;

import io.arenadata.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.service.impl.ddl.AdqmCreateTableQueries;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.CreateTableQueriesFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AdqmCreateTableQueriesFactory implements CreateTableQueriesFactory<AdqmCreateTableQueries> {
    private final DdlProperties ddlProperties;
    private final AppConfiguration appConfiguration;

    @Autowired
    public AdqmCreateTableQueriesFactory(DdlProperties ddlProperties,
                                         AppConfiguration appConfiguration) {
        this.ddlProperties = ddlProperties;
        this.appConfiguration = appConfiguration;
    }

    @Override
    public AdqmCreateTableQueries create(DdlRequestContext context) {
        return new AdqmCreateTableQueries(context, ddlProperties, appConfiguration);
    }
}
