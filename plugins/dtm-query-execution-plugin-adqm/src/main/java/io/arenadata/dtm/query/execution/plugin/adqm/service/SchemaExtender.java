package io.arenadata.dtm.query.execution.plugin.adqm.service;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;

import java.util.List;

/**
 * Extender interface for obtaining physical schemas from logical
 */
public interface SchemaExtender {
    List<Datamart> generatePhysicalSchema(List<Datamart> logicalSchemas, QueryRequest request);
}
