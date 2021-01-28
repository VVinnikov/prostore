package io.arenadata.dtm.query.execution.plugin.adb.service;

import io.arenadata.dtm.query.execution.model.metadata.Datamart;

/**
 * Extender interface for obtaining physical schemas from logical
 */
public interface SchemaExtender {
    Datamart createPhysicalSchema(Datamart schema);
}
