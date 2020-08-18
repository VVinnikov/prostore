package ru.ibs.dtm.query.execution.plugin.adg.service;

import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;

import java.util.List;

/**
 * Extender interface for obtaining physical schemas from logical
 */
public interface SchemaExtender {
    List<Datamart> generatePhysicalSchema(List<Datamart> logicalSchemas, QueryRequest request);
}
