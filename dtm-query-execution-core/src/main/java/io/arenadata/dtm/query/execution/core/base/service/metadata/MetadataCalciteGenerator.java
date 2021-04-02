package io.arenadata.dtm.query.execution.core.base.service.metadata;

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
