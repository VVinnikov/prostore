package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.reader.InformationSchemaView;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.service.InformationSchemaService;
import io.arenadata.dtm.query.execution.core.service.dml.InformationSchemaDefinitionService;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class InformationSchemaDefinitionServiceImpl implements InformationSchemaDefinitionService {

    private final InformationSchemaService informationSchemaService;

    @Autowired
    public InformationSchemaDefinitionServiceImpl(InformationSchemaService informationSchemaService) {
        this.informationSchemaService = informationSchemaService;
    }

    @Override
    public Future<QuerySourceRequest> tryGetInformationSchemaRequest(QuerySourceRequest request) {
        return Future.future(promise -> {
            if (isInformationSchemaRequest(request)) {
                val queryRequestWithSourceType = request.getQueryRequest().copy();
                queryRequestWithSourceType.setSourceType(SourceType.INFORMATION_SCHEMA);
                val result = new QuerySourceRequest(
                        queryRequestWithSourceType,
                        SourceType.INFORMATION_SCHEMA);
                result.setLogicalSchema(Collections.singletonList(
                        new Datamart(InformationSchemaView.DTM_SCHEMA_NAME, true,
                                new ArrayList<>(informationSchemaService.getEntities().values()))));
                promise.complete(result);
            } else {
                promise.complete(request);
            }
        });
    }

    private boolean isInformationSchemaRequest(QuerySourceRequest request) {
        Set<String> unicSchemes = request.getQueryRequest().getDeltaInformations().stream()
                .map(DeltaInformation::getSchemaName)
                .map(String::toUpperCase)
                .collect(Collectors.toSet());

        boolean informationSchemaExists = unicSchemes.contains(InformationSchemaView.DTM_SCHEMA_NAME);

        if (unicSchemes.size() > 1 && informationSchemaExists) {
            throw new IllegalArgumentException("Simultaneous query to the information schema and user schema isn't supported");
        } else {
            return informationSchemaExists;
        }
    }
}
