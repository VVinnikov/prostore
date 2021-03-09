package io.arenadata.dtm.query.execution.plugin.adb.service.impl.check;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.version.VersionInfo;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adb.factory.AdbVersionInfoFactory;
import io.arenadata.dtm.query.execution.plugin.adb.factory.AdbVersionQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.adb.factory.impl.AdbVersionQueriesFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.adb.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckVersionRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckVersionService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Service("adbCheckVersionService")
public class AdbCheckVersionService implements CheckVersionService {

    private final DatabaseExecutor databaseExecutor;
    private final AdbVersionQueriesFactory versionQueriesFactory;
    private final AdbVersionInfoFactory versionInfoFactory;

    @Autowired
    public AdbCheckVersionService(DatabaseExecutor databaseExecutor,
                                  AdbVersionQueriesFactory versionQueriesFactory,
                                  AdbVersionInfoFactory versionInfoFactory) {
        this.databaseExecutor = databaseExecutor;
        this.versionQueriesFactory = versionQueriesFactory;
        this.versionInfoFactory = versionInfoFactory;
    }

    @Override
    public Future<List<VersionInfo>> checkVersion(CheckVersionRequest request) {
        return Future.future(promise -> {
            val columnMetadata = createColumnMetadata();
            CompositeFuture.join(Arrays.asList(databaseExecutor.execute(versionQueriesFactory.createAdbVersionQuery(), columnMetadata),
                    databaseExecutor.execute(versionQueriesFactory.createFdwVersionQuery(), columnMetadata),
                    databaseExecutor.execute(versionQueriesFactory.createPxfVersionQuery(), columnMetadata)))
                    .onSuccess(result -> {
                        List<Map<String, Object>> resultList = new ArrayList<>();
                        List<List<Map<String, Object>>> list = result.list();
                        list.forEach(resultList::addAll);
                        promise.complete(versionInfoFactory.create(resultList));
                    })
                    .onFailure(promise::fail);
        });
    }

    private List<ColumnMetadata> createColumnMetadata() {
        return Arrays.asList(ColumnMetadata.builder()
                        .name(AdbVersionQueriesFactoryImpl.COMPONENT_NAME_COLUMN)
                        .type(ColumnType.VARCHAR)
                        .build(),
                ColumnMetadata.builder()
                        .name(AdbVersionQueriesFactoryImpl.VERSION_COLUMN)
                        .type(ColumnType.VARCHAR)
                        .build()
        );
    }
}
