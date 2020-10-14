package ru.ibs.dtm.query.execution.core.service.metadata;

import org.apache.calcite.sql.SqlCreate;
import ru.ibs.dtm.common.model.ddl.Entity;

public interface MetadataCalciteGenerator {

    /**
     * Преобразование nodeList(столбцы таблицы) calcite в объекты метаданных витрины
     *
     * @return
     */
    Entity generateTableMetadata(SqlCreate sqlCreate);
}
