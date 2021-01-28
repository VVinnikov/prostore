package io.arenadata.dtm.query.execution.core.service.dml;

import io.arenadata.dtm.common.delta.DeltaInformation;

import java.util.List;

public interface InformationSchemaDefinitionService {

    boolean isInformationSchemaRequest(List<DeltaInformation> deltaInformations);
}
