package io.arenadata.dtm.query.execution.plugin.adg.ddl.service;

import io.arenadata.dtm.query.execution.plugin.adg.ddl.factory.AdgTruncateHistoryConditionFactory;
import io.arenadata.dtm.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.adg.base.utils.AdgUtils;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;

import static io.arenadata.dtm.query.execution.plugin.adg.base.utils.ColumnFields.ACTUAL_POSTFIX;
import static io.arenadata.dtm.query.execution.plugin.adg.base.utils.ColumnFields.HISTORY_POSTFIX;

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
    public Future<Void> truncateHistory(TruncateHistoryRequest request) {
        return conditionFactory.create(request)
                .compose(conditions -> request.getSysCn().isPresent()
                        ? deleteSpaceTuples(request, HISTORY_POSTFIX, conditions)
                        : deleteSpaceTuplesWithoutSysCn(request, conditions));
    }

    private Future<Void> deleteSpaceTuples(TruncateHistoryRequest request, String postfix, String conditions) {
        String spaceName = AdgUtils.getSpaceName(request.getEnvName(), request.getEntity().getSchema(),
                request.getEntity().getName(), postfix);
        return adgCartridgeClient.deleteSpaceTuples(spaceName, conditions.isEmpty() ? null : conditions);
    }

    private Future<Void> deleteSpaceTuplesWithoutSysCn(TruncateHistoryRequest request,
                                                       String conditions) {
        return Future.future(promise -> CompositeFuture.join(Arrays.asList(
                deleteSpaceTuples(request, ACTUAL_POSTFIX, conditions),
                deleteSpaceTuples(request, HISTORY_POSTFIX, conditions)
        ))
                .onSuccess(result -> promise.complete())
                .onFailure(promise::fail));
    }
}
