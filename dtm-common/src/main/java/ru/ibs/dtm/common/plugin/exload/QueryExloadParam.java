package ru.ibs.dtm.common.plugin.exload;

import java.util.Objects;
import java.util.UUID;

/**
 * Параметры выполнения выгрузки
 */
public class QueryExloadParam {

  /**
   * Идентификатор
   */
  private UUID id;

  /**
   * Схема данных
   */
  private String datamart;

  /**
   * Наименование таблицы
   */
  private String tableName;

  /**
   * sql запрос
   */
  private String sqlQuery;

  /**
   * Тип назначения выгрузки
   */
  private Type locationType;

  /**
   * Путь назначения выгрузки
   */
  private String locationPath;

  /**
   * Формат
   */
  private Format format;

  /**
   * Количество записей в chunk
   */
  private Integer chunkSize;

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
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

  public String getSqlQuery() {
    return sqlQuery;
  }

  public void setSqlQuery(String sqlQuery) {
    this.sqlQuery = sqlQuery;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    QueryExloadParam that = (QueryExloadParam) o;
    return Objects.equals(getId(), that.getId()) &&
      Objects.equals(getDatamart(), that.getDatamart()) &&
      Objects.equals(getTableName(), that.getTableName()) &&
      Objects.equals(getSqlQuery(), that.getSqlQuery()) &&
      getLocationType() == that.getLocationType() &&
      Objects.equals(getLocationPath(), that.getLocationPath()) &&
      getFormat() == that.getFormat() &&
      Objects.equals(getChunkSize(), that.getChunkSize());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), getDatamart(), getTableName(), getSqlQuery(),
      getLocationType(), getLocationPath(), getFormat(), getChunkSize());
  }

  @Override
  public String toString() {
    return "QueryExloadParam{" +
      "id=" + id +
      ", datamart='" + datamart + '\'' +
      ", tableName='" + tableName + '\'' +
      ", sqlQuery='" + sqlQuery + '\'' +
      ", locationType=" + locationType +
      ", locationPath='" + locationPath + '\'' +
      ", format=" + format +
      ", chunkSize=" + chunkSize +
      '}';
  }
}
