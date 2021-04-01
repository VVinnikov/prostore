package io.arenadata.dtm.query.execution.core.metadata.service;

import io.arenadata.dtm.common.model.ddl.Entity;
import org.apache.calcite.sql.SqlCreate;

public interface MetadataCalciteGenerator {

    /**
     * Преобразование nodeList(столбцы таблицы) calcite в объекты метаданных витрины
     *
     * @return
     */
    Entity generateTableMetadata(SqlCreate sqlCreate);
}
