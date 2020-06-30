package ru.ibs.dtm.query.execution.plugin.adqm.service;

import ru.ibs.dtm.query.execution.plugin.adqm.model.metadata.Datamart;

/*
 * Интерфейс экстендера для получения физической схемы из логической
 * */
public interface SchemaExtender {
    Datamart generatePhysicalSchema(Datamart datamart);
}
