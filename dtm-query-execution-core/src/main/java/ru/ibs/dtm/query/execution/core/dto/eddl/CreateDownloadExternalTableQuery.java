package ru.ibs.dtm.query.execution.core.dto.eddl;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.plugin.exload.Type;

/**
 * Запрос создания внешней таблицы выгрузки
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class CreateDownloadExternalTableQuery extends EddlQuery {

  /**
   * Тип
   */
  private Type locationType;

  /**
   * путь
   */
  private String locationPath;

  /**
   * Формат
   */
  private Format format;

  /**
   * Размер чанка
   */
  private Integer chunkSize;

  /**
   * Avro schema in json format
   */
  private String tableSchema;

  public CreateDownloadExternalTableQuery() {
    super(EddlAction.CREATE_DOWNLOAD_EXTERNAL_TABLE);
  }

  public CreateDownloadExternalTableQuery(String schemaName,
                                          String tableName,
                                          Type locationType,
                                          String locationPath,
                                          Format format,
                                          Integer chunkSize,
                                          String tableSchema) {
    super(EddlAction.CREATE_DOWNLOAD_EXTERNAL_TABLE, schemaName, tableName);
    this.locationType = locationType;
    this.locationPath = locationPath;
    this.format = format;
    this.chunkSize = chunkSize;
    this.tableSchema = tableSchema;
  }
}
