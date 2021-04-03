package io.arenadata.dtm.query.execution.plugin.adg.enrichment.service;

import io.arenadata.dtm.query.execution.model.metadata.Datamart;

import java.util.List;

/**
 * Extender interface for obtaining physical schemas from logical
 */
public interface SchemaExtender {
    Datamart createPhysicalSchema(Datamart logicalSchema, String systemName);
}
