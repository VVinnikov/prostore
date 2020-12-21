package io.arenadata.dtm.query.execution.plugin.adg.service.impl.ddl;

import io.arenadata.dtm.query.execution.plugin.adg.AdgDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.exception.DdlDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service("adgDdlService")
public class AdgDdlService implements DdlService<Void> {

    private final Map<SqlKind, DdlExecutor<Void>> ddlExecutors = new HashMap<>();

    @Override
    @CacheEvict(value = AdgDataSourcePlugin.ADG_DATAMART_CACHE, key = "#context.getDatamartName()")
    public Future<Void> execute(DdlRequestContext context) {
        return Future.future(promise -> {
            SqlNode query = context.getQuery();
            if (query == null) {
                promise.fail(new DdlDatasourceException("Ddl query is null!"));
                return;
            }
            if (ddlExecutors.containsKey(query.getKind())) {
                ddlExecutors.get(query.getKind()).execute(context, query.getKind().lowerName)
                        .onComplete(promise);
            } else {
                promise.fail(new DdlDatasourceException(String.format("Unknown DDL: %s", query)));
            }
        });
    }

    @Override
    public void addExecutor(DdlExecutor<Void> executor) {
        ddlExecutors.put(executor.getSqlKind(), executor);
    }
}
