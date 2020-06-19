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
import ru.ibs.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppr.MpprRequestContext;

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
                    DdlRequestContext context,
                    Handler<AsyncResult<Void>> asyncResultHandler) {
        getPlugin(sourceType).ddl(context, asyncResultHandler);
    }

    public void llr(SourceType sourceType,
                    LlrRequestContext context,
                    Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        getPlugin(sourceType).llr(context, asyncResultHandler);
    }

    public void mpprKafka(SourceType sourceType,
                          MpprRequestContext context,
                          Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        getPlugin(sourceType).mpprKafka(context, asyncResultHandler);
    }

    public void calcQueryCost(SourceType sourceType,
                              QueryCostRequestContext context,
                              Handler<AsyncResult<Integer>> asyncResultHandler) {
        getPlugin(sourceType).calcQueryCost(context, asyncResultHandler);
    }

    private DtmDataSourcePlugin getPlugin(SourceType sourceType) {
        return pluginRegistry.getRequiredPluginFor(sourceType);
    }
}
