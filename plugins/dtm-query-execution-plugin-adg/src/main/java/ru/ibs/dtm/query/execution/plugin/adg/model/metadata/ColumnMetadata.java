package ru.ibs.dtm.query.execution.plugin.adg.model.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ColumnMetadata {
  String name;
  ColumnType type;
}
