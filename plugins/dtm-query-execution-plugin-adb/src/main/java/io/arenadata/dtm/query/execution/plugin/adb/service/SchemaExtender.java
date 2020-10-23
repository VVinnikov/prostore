package io.arenadata.dtm.query.execution.plugin.adb.service;

import io.arenadata.dtm.query.execution.model.metadata.Datamart;

import java.util.List;

/**
 * Extender interface for obtaining physical schemas from logical
 */
public interface SchemaExtender {
    List<Datamart> generatePhysicalSchemas(List<Datamart> logicalSchemas);
}
