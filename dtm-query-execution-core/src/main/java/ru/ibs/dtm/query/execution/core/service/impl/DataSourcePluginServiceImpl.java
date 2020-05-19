package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.plugin.core.PluginRegistry;
import org.springframework.plugin.core.config.EnablePluginRegistries;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.core.service.DataSourcePluginService;
import ru.ibs.dtm.query.execution.plugin.api.DtmDataSourcePlugin;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.dto.CalcQueryCostRequest;
import ru.ibs.dtm.query.execution.plugin.api.dto.LlrRequest;
import ru.ibs.dtm.query.execution.plugin.api.dto.MpprKafkaRequest;

import java.util.Set;
import java.util.stream.Collectors;

@Service
@EnablePluginRegistries({DtmDataSourcePlugin.class})
public class DataSourcePluginServiceImpl implements DataSourcePluginService {

    private final PluginRegistry<DtmDataSourcePlugin, SourceType> pluginRegistry;
    private final Set<SourceType> sourceTypes;

    @Autowired
    public DataSourcePluginServiceImpl(
            @Qualifier("dtmDataSourcePluginRegistry") PluginRegistry<DtmDataSourcePlugin, SourceType> pluginRegistry) {
        this.pluginRegistry = pluginRegistry;
        this.sourceTypes = pluginRegistry.getPlugins().stream()
                .map(DtmDataSourcePlugin::getSourceType)
                .collect(Collectors.toSet());
    }

    public Set<SourceType> getSourceTypes() {
        return sourceTypes;
    }

    public void ddl(SourceType sourceType,
                    DdlRequestContext request,
                    Handler<AsyncResult<Void>> asyncResultHandler) {
        getPlugin(sourceType).ddl(request, asyncResultHandler);
    }

    public void llr(SourceType sourceType,
                    LlrRequest request,
                    Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        getPlugin(sourceType).llr(request, asyncResultHandler);
    }

    public void mpprKafka(SourceType sourceType,
                          MpprKafkaRequest request,
                          Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        getPlugin(sourceType).mpprKafka(request, asyncResultHandler);
    }

    public void calcQueryCost(SourceType sourceType,
                              CalcQueryCostRequest request,
                              Handler<AsyncResult<Integer>> asyncResultHandler) {
        getPlugin(sourceType).calcQueryCost(request, asyncResultHandler);
    }

    private DtmDataSourcePlugin getPlugin(SourceType sourceType) {
        return pluginRegistry.getRequiredPluginFor(sourceType);
    }
}
