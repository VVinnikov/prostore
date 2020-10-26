package io.arenadata.dtm.query.execution.plugin.adg.model;

import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import lombok.AllArgsConstructor;
import lombok.Data;

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
