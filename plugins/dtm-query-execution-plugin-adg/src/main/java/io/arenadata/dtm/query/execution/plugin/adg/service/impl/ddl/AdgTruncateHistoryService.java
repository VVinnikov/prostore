package io.arenadata.dtm.query.execution.plugin.adg.service.impl.ddl;

import io.arenadata.dtm.query.execution.plugin.adg.factory.AdgTruncateHistoryConditionFactory;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.adg.utils.AdgUtils;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;

import static io.arenadata.dtm.query.execution.plugin.adg.constants.ColumnFields.ACTUAL_POSTFIX;
import static io.arenadata.dtm.query.execution.plugin.adg.constants.ColumnFields.HISTORY_POSTFIX;

@Service("adgTruncateHistoryService")
public class AdgTruncateHistoryService implements TruncateHistoryService {
    private final AdgCartridgeClient adgCartridgeClient;
    private final AdgTruncateHistoryConditionFactory conditionFactory;

    @Autowired
    public AdgTruncateHistoryService(AdgCartridgeClient adgCartridgeClient,
                                     AdgTruncateHistoryConditionFactory adgTruncateHistoryConditionFactory) {
        this.adgCartridgeClient = adgCartridgeClient;
        this.conditionFactory = adgTruncateHistoryConditionFactory;
    }

    @Override
    public Future<Void> truncateHistory(TruncateHistoryRequest params) {
        return conditionFactory.create(params)
                .compose(conditions -> params.getSysCn().isPresent()
                        ? deleteSpaceTuples(params, HISTORY_POSTFIX, conditions)
                        : deleteSpaceTuplesWithoutSysCn(params, conditions));
    }

    private Future<Void> deleteSpaceTuples(TruncateHistoryRequest params, String postfix, String conditions) {
        String spaceName = AdgUtils.getSpaceName(params.getEnvName(), params.getEntity().getSchema(),
                params.getEntity().getName(), postfix);
        return adgCartridgeClient.deleteSpaceTuples(spaceName, conditions.isEmpty() ? null : conditions);
    }

    private Future<Void> deleteSpaceTuplesWithoutSysCn(TruncateHistoryRequest params,
                                                       String conditions) {
        return Future.future(promise -> CompositeFuture.join(Arrays.asList(
                deleteSpaceTuples(params, ACTUAL_POSTFIX, conditions),
                deleteSpaceTuples(params, HISTORY_POSTFIX, conditions)
        ))
                .onSuccess(result -> promise.complete())
                .onFailure(promise::fail));
    }
}
