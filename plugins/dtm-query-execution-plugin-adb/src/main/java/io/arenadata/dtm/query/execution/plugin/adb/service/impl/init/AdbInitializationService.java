package io.arenadata.dtm.query.execution.plugin.adb.service.impl.init;

import io.arenadata.dtm.query.execution.plugin.adb.factory.AdbHashFunctionFactory;
import io.arenadata.dtm.query.execution.plugin.adb.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.PluginInitializationService;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("adbInitializationService")
public class AdbInitializationService implements PluginInitializationService {

    private final DatabaseExecutor databaseExecutor;
    private final AdbHashFunctionFactory hashFunctionFactory;

    @Autowired
    public AdbInitializationService(DatabaseExecutor databaseExecutor,
                                    AdbHashFunctionFactory hashFunctionFactory) {
        this.databaseExecutor = databaseExecutor;
        this.hashFunctionFactory = hashFunctionFactory;
    }

    @Override
    public Future<Void> execute() {
        return databaseExecutor.executeUpdate(hashFunctionFactory.createInt32HashFunction());
    }
}
