package io.arenadata.dtm.query.execution.plugin.adp.mppw.kafka.service.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adp.connector.AdpConnectorClient;
import io.arenadata.dtm.query.execution.plugin.adp.mppw.dto.AdpConnectorMppwStopRequest;
import io.arenadata.dtm.query.execution.plugin.adp.mppw.dto.AdpTransferDataRequest;
import io.arenadata.dtm.query.execution.plugin.adp.mppw.kafka.service.AdpMppwRequestExecutor;
import io.arenadata.dtm.query.execution.plugin.adp.mppw.transfer.AdpTransferDataService;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service("adpStopMppwRequestExecutor")
public class AdpStopMppwRequestExecutor implements AdpMppwRequestExecutor {

    private final AdpConnectorClient connectorClient;
    private final AdpTransferDataService transferDataService;

    public AdpStopMppwRequestExecutor(AdpConnectorClient connectorClient,
                                      AdpTransferDataService transferDataService) {
        this.connectorClient = connectorClient;
        this.transferDataService = transferDataService;
    }

    @Override
    public Future<QueryResult> execute(MppwKafkaRequest request) {
        return Future.future(promise -> {
            log.info("[ADP] Trying to stop MPPW, request: [{}]", request);
            val connectorRequest = new AdpConnectorMppwStopRequest(request.getRequestId().toString(), request.getTopic());
            connectorClient.stopMppw(connectorRequest)
                    .compose(v -> transferDataService.transferData(createRequest(request)))
                    .onSuccess(v -> {
                        log.info("[ADP] Mppw stopped successfully");
                        promise.complete(QueryResult.emptyResult());
                    })
                    .onFailure(t -> {
                        log.error("[ADP] Mppw failed to stop", t);
                        promise.fail(t);
                    });
        });
    }

    private AdpTransferDataRequest createRequest(MppwKafkaRequest request) {
        List<String> allFields = request.getSourceEntity().getFields().stream()
                .sorted(Comparator.comparingInt(EntityField::getOrdinalPosition))
                .map(EntityField::getName)
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(allFields)) {
            throw new DtmException("No fields in source entity");
        }

        if (CollectionUtils.isEmpty(request.getPrimaryKeys())) {
            throw new DtmException("No primary fields in request");
        }

        return AdpTransferDataRequest.builder()
                .datamart(request.getDatamartMnemonic())
                .sysCn(request.getSysCn())
                .tableName(request.getDestinationTableName())
                .primaryKeys(request.getPrimaryKeys())
                .allFields(allFields)
                .build();
    }
}
