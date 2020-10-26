package io.arenadata.dtm.query.execution.core.dto.eddl;

/**
 * Запрос удаления внешней таблицы
 */
public class DropDownloadExternalTableQuery extends EddlQuery {

  public DropDownloadExternalTableQuery() {
    super(EddlAction.DROP_DOWNLOAD_EXTERNAL_TABLE);
  }

  public DropDownloadExternalTableQuery(String schemaName, String tableName) {
    super(EddlAction.DROP_DOWNLOAD_EXTERNAL_TABLE, schemaName, tableName);
  }
}
