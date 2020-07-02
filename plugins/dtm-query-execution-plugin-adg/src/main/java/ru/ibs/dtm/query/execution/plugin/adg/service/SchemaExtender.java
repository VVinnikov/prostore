package ru.ibs.dtm.query.execution.plugin.adg.service;

import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;

/**
 * Интерфейс экстендера для получения физической схемы из логической
 * */
public interface SchemaExtender {
  Datamart generatePhysicalSchema(Datamart datamart, QueryRequest queryRequest);
}
