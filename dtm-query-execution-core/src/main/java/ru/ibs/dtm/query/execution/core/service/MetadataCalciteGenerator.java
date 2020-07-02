package ru.ibs.dtm.query.execution.core.service;

import org.apache.calcite.sql.SqlCreate;
import ru.ibs.dtm.common.model.ddl.ClassTable;

public interface MetadataCalciteGenerator {

    /**
     * Преобразование nodeList(столбцы таблицы) calcite в объекты метаданных витрины
     *
     * @return
     */
    ClassTable generateTableMetadata(SqlCreate sqlCreate);
}
