package io.arenadata.dtm.query.execution.plugin.adg.check.service;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.adg.base.constants.ColumnFields;
import io.arenadata.dtm.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.adg.base.utils.AdgUtils;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Request;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckDataService;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Set;

@Service("adgCheckDataService")
public class AdgCheckDataService implements CheckDataService {
    private final AdgCartridgeClient adgCartridgeClient;

    @Autowired
    public AdgCheckDataService(AdgCartridgeClient adgCartridgeClient) {
        this.adgCartridgeClient = adgCartridgeClient;
    }

    @Override
    public Future<Long> checkDataByCount(CheckDataByCountRequest request) {
        Entity entity = request.getEntity();
        return getCheckSum(request.getEnvName(), entity.getSchema(), entity.getName(), request.getSysCn(),
                null);
    }

    @Override
    public Future<Long> checkDataByHashInt32(CheckDataByHashInt32Request request) {
        Entity entity = request.getEntity();
        return getCheckSum(request.getEnvName(), entity.getSchema(), entity.getName(), request.getSysCn(),
                request.getColumns());
    }

    private Future<Long> getCheckSum(String env,
                                     String schema,
                                     String table,
                                     Long sysCn,
                                     Set<String> columns) {
        return adgCartridgeClient.getCheckSumByInt32Hash(
                AdgUtils.getSpaceName(env, schema, table, ColumnFields.ACTUAL_POSTFIX),
                AdgUtils.getSpaceName(env, schema, table, ColumnFields.HISTORY_POSTFIX),
                sysCn,
                columns
        );
    }
}
