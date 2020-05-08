package ru.ibs.dtm.query.execution.plugin.adg.factory;

import org.apache.calcite.schema.SchemaPlus;
import ru.ibs.dtm.query.execution.plugin.adg.calcite.schema.QueryableSchema;
import ru.ibs.dtm.query.execution.plugin.adg.model.metadata.Datamart;

/**
 * Фабрика создания кастомной схемы
 */
public interface SchemaFactory {

  /**
   * Cоздание новой схемы
   * @param parentSchema - основная схема
   * @param datamart - новая схема витрины
   * @return кастомизированая схема
   */
  QueryableSchema create(SchemaPlus parentSchema, Datamart datamart);
}
