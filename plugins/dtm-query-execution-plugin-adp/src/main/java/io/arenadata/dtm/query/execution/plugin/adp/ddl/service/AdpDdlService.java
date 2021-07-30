package io.arenadata.dtm.query.execution.plugin.adp.ddl.service;

import io.arenadata.dtm.query.execution.plugin.adp.base.service.AdpDtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.exception.DdlDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlService;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlKind;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service("adpDdlService")
public class AdpDdlService implements DdlService<Void> {

    private final Map<SqlKind, DdlExecutor<Void>> ddlExecutors = new HashMap<>();

    @Override
    @CacheEvict(value = AdpDtmDataSourcePlugin.ADP_DATAMART_CACHE, key = "#request.getDatamartMnemonic()")
    public Future<Void> execute(DdlRequest request) {
        SqlKind sqlKind = request.getSqlKind();
        if (ddlExecutors.containsKey(sqlKind)) {
            return ddlExecutors.get(sqlKind)
                    .execute(request);
        } else {
            return Future.failedFuture(new DdlDatasourceException(String.format("Unknown DDL: %s", sqlKind)));
        }
    }

    @Override
    public void addExecutor(DdlExecutor<Void> executor) {
        ddlExecutors.put(executor.getSqlKind(), executor);
    }
}
