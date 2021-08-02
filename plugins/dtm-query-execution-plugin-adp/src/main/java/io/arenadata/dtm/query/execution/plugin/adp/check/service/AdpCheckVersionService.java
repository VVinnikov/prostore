package io.arenadata.dtm.query.execution.plugin.adp.check.service;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.version.VersionInfo;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adp.check.factory.AdpVersionQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.adp.connector.service.AdpConnectorClient;
import io.arenadata.dtm.query.execution.plugin.adp.db.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckVersionRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckVersionService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adp.check.factory.AdpVersionQueriesFactory.*;

@Service("adpCheckVersionService")
@Slf4j
public class AdpCheckVersionService implements CheckVersionService {

    private final DatabaseExecutor databaseExecutor;
    private final AdpConnectorClient connectorClient;
    private final List<ColumnMetadata> metadata;

    @Autowired
    public AdpCheckVersionService(DatabaseExecutor databaseExecutor,
                                  AdpConnectorClient connectorClient) {
        this.databaseExecutor = databaseExecutor;
        this.connectorClient = connectorClient;
        metadata = createColumnMetadata();
    }

    @Override
    public Future<List<VersionInfo>> checkVersion(CheckVersionRequest request) {
        return CompositeFuture.join(databaseExecutor.execute(createAdpVersionQuery(), metadata)
                        .map(this::createResult),
                getConnectorVersions())
                .map(result -> {
                    List<List<VersionInfo>> list = result.list();
                    return list.stream()
                            .flatMap(List::stream)
                            .collect(Collectors.toList());
                });
    }

    private Future<List<VersionInfo>> getConnectorVersions() {
        return CompositeFuture.join(connectorClient.getMpprVersion(), connectorClient.getMppwVersion())
                .map(result -> {
                    List<List<VersionInfo>> list = result.list();
                    return list.stream()
                            .flatMap(List::stream)
                            .collect(Collectors.toList());
                });
    }

    private List<ColumnMetadata> createColumnMetadata() {
        return Arrays.asList(ColumnMetadata.builder()
                        .name(AdpVersionQueriesFactory.COMPONENT_NAME_COLUMN)
                        .type(ColumnType.VARCHAR)
                        .build(),
                ColumnMetadata.builder()
                        .name(AdpVersionQueriesFactory.VERSION_COLUMN)
                        .type(ColumnType.VARCHAR)
                        .build()
        );
    }

    private List<VersionInfo> createResult(List<Map<String, Object>> rows) {
        return rows.stream()
                .map(row -> new VersionInfo(row.get(COMPONENT_NAME_COLUMN).toString(),
                        row.get(VERSION_COLUMN).toString()))
                .collect(Collectors.toList());
    }
}
