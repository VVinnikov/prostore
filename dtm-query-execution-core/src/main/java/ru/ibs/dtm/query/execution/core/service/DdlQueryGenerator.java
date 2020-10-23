package ru.ibs.dtm.query.execution.core.service;

import ru.ibs.dtm.common.model.ddl.Entity;

public interface DdlQueryGenerator {

    /**
     * Generate CREATE TABLE query upon entity meta-data
     *
     * @param entity
     * @return generated CREATE TABLE query
     */
    String generateCreateTableQuery(Entity entity);

    /**
     * Generate CREATE VIEW query upon entity meta-data
     *
     * @param entity
     * @return generated CREATE VIEW query
     */
    String generateCreateViewQuery(Entity entity);
}
