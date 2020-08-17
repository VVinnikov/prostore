package ru.ibs.dtm.query.execution.plugin.adb.service;

import ru.ibs.dtm.query.execution.model.metadata.Datamart;

import java.util.List;

/**
 * Extender interface for obtaining physical schemas from logical
 */
public interface SchemaExtender {
    List<Datamart> generatePhysicalSchemas(List<Datamart> logicalSchemas);
}
