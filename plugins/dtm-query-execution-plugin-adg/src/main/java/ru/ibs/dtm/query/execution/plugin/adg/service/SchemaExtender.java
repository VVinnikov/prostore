package ru.ibs.dtm.query.execution.plugin.adg.service;

import ru.ibs.dtm.query.execution.plugin.adg.model.metadata.Datamart;

/**
 * Интерфейс экстендера для получения физической схемы из логической
 * */
public interface SchemaExtender {
  Datamart generatePhysicalSchema(Datamart datamart);
}
