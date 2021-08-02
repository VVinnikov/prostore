package io.arenadata.dtm.query.execution.plugin.adp.init.service;

import io.arenadata.dtm.query.execution.plugin.adp.db.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.PluginInitializationService;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import static io.arenadata.dtm.query.execution.plugin.adp.base.factory.hash.AdpFunctionFactory.createInt32HashFunction;

@Service("adpInitializationService")
public class AdpInitializationService implements PluginInitializationService {

    private final DatabaseExecutor databaseExecutor;

    public AdpInitializationService(@Qualifier("adpQueryExecutor") DatabaseExecutor databaseExecutor) {
        this.databaseExecutor = databaseExecutor;
    }

    @Override
    public Future<Void> execute() {
        return databaseExecutor.executeUpdate(createInt32HashFunction());
    }
}
