package ru.ibs.dtm.query.execution.core.dto;

import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.plugin.exload.Type;

/**
 * Запись из внешней таблицы выгрузки
 */
public class DownloadExtTableRecord {
  private Long id;
  private String datamart;
  private String tableName;
  private Type locationType;
  private String locationPath;
  private Format format;
  private Integer chunkSize;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getDatamart() {
    return datamart;
  }

  public void setDatamart(String datamart) {
    this.datamart = datamart;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
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
