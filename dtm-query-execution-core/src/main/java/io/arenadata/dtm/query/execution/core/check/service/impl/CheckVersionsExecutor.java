package io.arenadata.dtm.query.execution.core.check.service.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.version.VersionInfo;
import io.arenadata.dtm.kafka.core.configuration.properties.KafkaProperties;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.execution.core.plugin.configuration.properties.ActivePluginsProperties;
import io.arenadata.dtm.query.execution.core.check.dto.CheckContext;
import io.arenadata.dtm.query.execution.core.check.factory.CheckVersionQueryResultFactory;
import io.arenadata.dtm.query.execution.core.check.service.CheckExecutor;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckVersionRequest;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.info.BuildProperties;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Service("checkVersionsExecutor")
@Slf4j
public class CheckVersionsExecutor implements CheckExecutor {
    private static final String CORE_COMPONENT_NAME = "query-execution-core";
    private final DataSourcePluginService dataSourcePluginService;
    private final CheckVersionQueryResultFactory queryResultFactory;
    private final WebClient webClient;
    private final ActivePluginsProperties activePluginsProperties;
    private final KafkaProperties kafkaProperties;
    private final BuildProperties buildProperties;

    @Autowired
    public CheckVersionsExecutor(DataSourcePluginService dataSourcePluginService,
                                 CheckVersionQueryResultFactory queryResultFactory,
                                 @Qualifier("coreWebClient") WebClient webClient,
                                 ActivePluginsProperties activePluginsProperties,
                                 @Qualifier("coreKafkaProperties") KafkaProperties kafkaProperties,
                                 BuildProperties buildProperties) {
        this.dataSourcePluginService = dataSourcePluginService;
        this.queryResultFactory = queryResultFactory;
        this.webClient = webClient;
        this.activePluginsProperties = activePluginsProperties;
        this.kafkaProperties = kafkaProperties;
        this.buildProperties = buildProperties;
    }

    @Override
    public Future<QueryResult> execute(CheckContext context) {
        return Future.future(promise -> {
            List<VersionInfo> versions = new ArrayList<>();
            versions.add(new VersionInfo(CORE_COMPONENT_NAME, buildProperties.getVersion()));
            CompositeFuture.join(getVersionsFutures(context))
                    .onSuccess(result -> {
                        result.list().forEach(versionList -> {
                            if (versionList != null) {
                                List<VersionInfo> versionInfos = (List<VersionInfo>) versionList;
                                versions.addAll(versionInfos);
                            }
                        });
                        promise.complete(queryResultFactory.create(versions));
                    })
                    .onFailure(promise::fail);
        });
    }

    @NotNull
    private List<Future> getVersionsFutures(CheckContext context) {
        List<Future> componentsVersionsFutures = new ArrayList<>();
        activePluginsProperties.getActive().forEach(ds ->
                componentsVersionsFutures.add(dataSourcePluginService.checkVersion(ds,
                        context.getMetrics(),
                        new CheckVersionRequest(context.getRequest().getQueryRequest().getRequestId(),
                                context.getEnvName(),
                                context.getRequest().getQueryRequest().getDatamartMnemonic()
                        ))));
        componentsVersionsFutures.add(getStatusMonitorVersion());
        return componentsVersionsFutures;
    }

    private Future<List<VersionInfo>> getStatusMonitorVersion() {
        return executeGetVersionRequest(kafkaProperties.getStatusMonitor().getVersionUrl())
                .compose(this::handleResponse)
                .map(Collections::singletonList);
    }

    private Future<HttpResponse<Buffer>> executeGetVersionRequest(String uri) {
        return Future.future(promise -> {
            log.debug("Send request to [{}]", uri);
            webClient.getAbs(uri)
                    .send(promise);
        });
    }


    private Future<VersionInfo> handleResponse(HttpResponse<Buffer> response) {
        return Future.future(promise -> {
            log.trace("Handle response [{}]", response);
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                val successResponse = response.bodyAsJson(VersionInfo.class);
                promise.complete(successResponse);
            } else {
                promise.fail(new DtmException("Error in receiving version info"));
            }
        });
    }

    @Override
    public CheckType getType() {
        return CheckType.VERSIONS;
    }

}
