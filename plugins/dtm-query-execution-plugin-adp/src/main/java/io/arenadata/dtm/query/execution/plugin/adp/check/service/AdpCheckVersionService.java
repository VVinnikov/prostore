package io.arenadata.dtm.query.execution.plugin.adp.check.service;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.version.VersionInfo;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adp.base.properties.AdpMpprProperties;
import io.arenadata.dtm.query.execution.plugin.adp.base.properties.AdpMppwProperties;
import io.arenadata.dtm.query.execution.plugin.adp.check.factory.AdpVersionQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.adp.db.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckVersionRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckVersionService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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
    private final AdpMpprProperties mpprProperties;
    private final AdpMppwProperties mppwProperties;
    private final WebClient webClient;
    private final List<ColumnMetadata> metadata;

    @Autowired
    public AdpCheckVersionService(DatabaseExecutor databaseExecutor,
                                  AdpMpprProperties mpprProperties,
                                  AdpMppwProperties mppwProperties,
                                  @Qualifier("adpWebClient") WebClient webClient) {
        this.databaseExecutor = databaseExecutor;
        this.mpprProperties = mpprProperties;
        this.mppwProperties = mppwProperties;
        this.webClient = webClient;
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
        return CompositeFuture.join(executeGetVersionRequest(mpprProperties.getRestVersionUrl()).map(this::handleResponse),
                executeGetVersionRequest(mppwProperties.getRestVersionUrl()).map(this::handleResponse))
                .map(result -> {
                    List<List<VersionInfo>> list = result.list();
                    return list.stream()
                            .flatMap(List::stream)
                            .collect(Collectors.toList());
                });
    }

    private Future<HttpResponse<Buffer>> executeGetVersionRequest(String url) {
        return Future.future(promise -> {
            log.debug("Send request to [{}]", url);
            webClient.getAbs(url)
                    .send(promise);
        });
    }

    private List<VersionInfo> handleResponse(HttpResponse<Buffer> response) {
        log.trace("Handle response [{}]", response);
        val statusCode = response.statusCode();
        if (statusCode == 200) {
            return response.bodyAsJsonArray().stream()
                    .map(o -> Json.decodeValue(o.toString(), VersionInfo.class))
                    .collect(Collectors.toList());
        } else {
            throw new DtmException("Error in receiving version info");
        }
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
