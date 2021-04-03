package io.arenadata.dtm.query.execution.plugin.adg.ddl.service;

import io.arenadata.dtm.query.execution.plugin.adg.base.service.AdgDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.exception.DdlDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service("adgDdlService")
public class AdgDdlService implements DdlService<Void> {

    private final Map<SqlKind, DdlExecutor<Void>> ddlExecutors = new HashMap<>();

    @Override
    @CacheEvict(value = AdgDataSourcePlugin.ADG_DATAMART_CACHE, key = "#request.getDatamartMnemonic()")
    public Future<Void> execute(DdlRequest request) {
        return Future.future(promise -> {
            SqlKind sqlKind = request.getSqlKind();
            if (ddlExecutors.containsKey(sqlKind)) {
                ddlExecutors.get(sqlKind).execute(request)
                        .onComplete(promise);
            } else {
                promise.fail(new DdlDatasourceException(String.format("Unknown DDL: %s", sqlKind)));
            }
        });
    }

    @Override
    public void addExecutor(DdlExecutor<Void> executor) {
        ddlExecutors.put(executor.getSqlKind(), executor);
    }
}
