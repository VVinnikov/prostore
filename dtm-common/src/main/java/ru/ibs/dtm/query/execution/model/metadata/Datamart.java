package ru.ibs.dtm.query.execution.model.metadata;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Описание схемы SchemaDescription
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Datamart implements Serializable {
  private UUID id;
  /*Имя схемы*/
  private String mnemonic;
  /*Описание таблиц в схеме*/
  private List<DatamartTable> datamartTables;
}



