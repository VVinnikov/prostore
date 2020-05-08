package ru.ibs.dtm.query.execution.plugin.adg.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import ru.ibs.dtm.query.execution.plugin.adg.model.metadata.ColumnMetadata;

import java.util.List;

/**
 * Результат выполнения запроса
 */
@Data
@AllArgsConstructor
public class QueryResultItem {
  List<ColumnMetadata> metadata;
  List<List<?>> dataSet;
}
