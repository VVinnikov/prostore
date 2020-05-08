package ru.ibs.dtm.query.execution.core.dto.eddl;

import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.plugin.exload.Type;

/**
 * Запрос создания внешней таблицы выгрузки
 */
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

  public CreateDownloadExternalTableQuery() {
    super(EddlAction.CREATE_DOWNLOAD_EXTERNAL_TABLE);
  }

  public CreateDownloadExternalTableQuery(String schemaName,
                                          String tableName,
                                          Type locationType,
                                          String locationPath,
                                          Format format,
                                          Integer chunkSize) {
    super(EddlAction.CREATE_DOWNLOAD_EXTERNAL_TABLE, schemaName, tableName);
    this.locationType = locationType;
    this.locationPath = locationPath;
    this.format = format;
    this.chunkSize = chunkSize;
  }

  public Type getLocationType() {
    return locationType;
  }

  public void setLocationType(Type locationType) {
    this.locationType = locationType;
  }

  public String getLocationPath() {
    return locationPath;
  }

  public void setLocationPath(String locationPath) {
    this.locationPath = locationPath;
  }

  public Format getFormat() {
    return format;
  }

  public void setFormat(Format format) {
    this.format = format;
  }

  public Integer getChunkSize() {
    return chunkSize;
  }

  public void setChunkSize(Integer chunkSize) {
    this.chunkSize = chunkSize;
  }
}
