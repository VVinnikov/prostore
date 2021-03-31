package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.check;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.version.VersionInfo;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.AdqmMpprProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.AdqmMppwProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.factory.AdqmVersionInfoFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.factory.AdqmVersionQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.factory.impl.AdqmVersionQueriesFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Service("adqmCheckVersionService")
@Slf4j
public class AdqmCheckVersionService implements CheckVersionService {

    private final DatabaseExecutor databaseExecutor;
    private final AdqmVersionQueriesFactory versionQueriesFactory;
    private final AdqmVersionInfoFactory versionInfoFactory;
    private final AdqmMpprProperties mpprProperties;
    private final AdqmMppwProperties mppwProperties;
    private final WebClient webClient;
    private final List<ColumnMetadata> metadata;

    @Autowired
    public AdqmCheckVersionService(@Qualifier("adqmQueryExecutor") DatabaseExecutor databaseExecutor,
                                   AdqmVersionQueriesFactory versionQueriesFactory,
                                   AdqmVersionInfoFactory versionInfoFactory,
                                   AdqmMpprProperties mpprProperties,
                                   AdqmMppwProperties mppwProperties,
                                   @Qualifier("adqmWebClient") WebClient webClient) {
        this.databaseExecutor = databaseExecutor;
        this.versionQueriesFactory = versionQueriesFactory;
        this.versionInfoFactory = versionInfoFactory;
        this.mpprProperties = mpprProperties;
        this.mppwProperties = mppwProperties;
        this.webClient = webClient;
        metadata = createColumnMetadata();
    }

    @Override
    public Future<List<VersionInfo>> checkVersion(CheckVersionRequest request) {
        return Future.future(promise -> {
            CompositeFuture.join(databaseExecutor.execute(versionQueriesFactory.createAdqmVersionQuery(), metadata)
                            .map(versionInfoFactory::create),
                    getConnectorVersions())
                    .onSuccess(result -> {
                        List<List<VersionInfo>> list = result.list();
                        promise.complete(list.stream()
                                .flatMap(List::stream)
                                .collect(Collectors.toList()));
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<List<VersionInfo>> getConnectorVersions() {
        return Future.future(promise -> {
            CompositeFuture.join(executeGetVersionRequest(mpprProperties.getVersionUrl()).compose(this::handleResponse),
                    executeGetVersionRequest(mppwProperties.getVersionUrl()).compose(this::handleResponse))
                    .onSuccess(result -> {
                        List<List<VersionInfo>> list = result.list();
                        promise.complete(list.stream()
                                .flatMap(List::stream)
                                .collect(Collectors.toList()));
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<HttpResponse<Buffer>> executeGetVersionRequest(String uri) {
        return Future.future(promise -> {
            log.debug("Send request to [{}]", uri);
            webClient.getAbs(uri)
                    .send(promise);
        });
    }

    private Future<List<VersionInfo>> handleResponse(HttpResponse<Buffer> response) {
        return Future.future(promise -> {
            log.trace("Handle response [{}]", response);
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                List<VersionInfo> successResponse = response.bodyAsJsonArray().stream()
                        .map(o -> Json.decodeValue(o.toString(), VersionInfo.class))
                        .collect(Collectors.toList());
                promise.complete(successResponse);
            } else {
                promise.fail(new DtmException("Error in receiving version info"));
            }
        });
    }

    private List<ColumnMetadata> createColumnMetadata() {
        return Arrays.asList(ColumnMetadata.builder()
                        .name(AdqmVersionQueriesFactoryImpl.COMPONENT_NAME_COLUMN)
                        .type(ColumnType.VARCHAR)
                        .build(),
                ColumnMetadata.builder()
                        .name(AdqmVersionQueriesFactoryImpl.VERSION_COLUMN)
                        .type(ColumnType.VARCHAR)
                        .build()
        );
    }
}