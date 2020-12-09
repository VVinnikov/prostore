package io.arenadata.dtm.query.execution.plugin.adg.service.impl.check;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.adg.constants.ColumnFields;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.adg.utils.AdgUtils;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountParams;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Params;
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
    public Future<Long> checkDataByCount(CheckDataByCountParams params) {
        Entity entity = params.getEntity();
        return getCheckSum(params.getEnv(), entity.getSchema(), entity.getName(), params.getSysCn(),
                null);
    }

    @Override
    public Future<Long> checkDataByHashInt32(CheckDataByHashInt32Params params) {
        Entity entity = params.getEntity();
        return getCheckSum(params.getEnv(), entity.getSchema(), entity.getName(), params.getSysCn(),
                params.getColumns());
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
