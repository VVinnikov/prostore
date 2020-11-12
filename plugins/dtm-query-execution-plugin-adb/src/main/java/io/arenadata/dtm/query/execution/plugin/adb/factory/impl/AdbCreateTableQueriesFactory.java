package io.arenadata.dtm.query.execution.plugin.adb.factory.impl;

import io.arenadata.dtm.query.execution.plugin.adb.service.impl.ddl.AdbCreateTableQueries;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.CreateTableQueriesFactory;
import org.springframework.stereotype.Component;

@Component
public class AdbCreateTableQueriesFactory implements CreateTableQueriesFactory<AdbCreateTableQueries> {

    @Override
    public AdbCreateTableQueries create(DdlRequestContext context) {
        return new AdbCreateTableQueries(context);
    }
}
