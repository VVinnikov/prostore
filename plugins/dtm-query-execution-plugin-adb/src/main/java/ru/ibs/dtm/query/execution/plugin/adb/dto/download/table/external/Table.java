package ru.ibs.dtm.query.execution.plugin.adb.dto.download.table.external;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.vertx.sqlclient.Row;

import java.util.Objects;

/**
 * Внешняя таблица вырузки
 */
@JsonPropertyOrder({"schema", "tableName", "type", "location", "format"})
public class Table {

    public Table() {
    }

    public Table(String schema, String tableName, Type type, String location, Format format) {
        this.schema = schema;
        this.tableName = tableName;
        this.type = type;
        this.location = location;
        this.format = format;
    }

    public Table(Row row) {
        this(row.getString("schema"),
                row.getString("tableName"),
                Type.values()[row.getInteger("type")],
                row.getString("location"),
                Format.values()[row.getInteger("format")]
        );
    }

    /**
     * Наименование схемы данных.
   */
  private String schema;
  /**
   * Наименование внешней таблицы выгрузки
   */
  private String tableName;
  /**
   * Тип (kafkaTopic | hdfsLocation | csvFile)
   */
  private Type type;
  /**
   * Путь (имя топика Kafka | путь HDFS | путь к CSV файлу)
   */
  private String location;
  /**
   * Формат ('AVRO' | 'СSV' | 'TEXT')
   */
  private Format format;


  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public Format getFormat() {
    return format;
  }

  public void setFormat(Format format) {
    this.format = format;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Table table = (Table) o;
    return Objects.equals(schema, table.schema) &&
      Objects.equals(tableName, table.tableName) &&
      type == table.type &&
      Objects.equals(location, table.location) &&
      format == table.format;
  }

  @Override
  public int hashCode() {
    return Objects.hash(schema, tableName, type, location, format);
  }

  @Override
  public String toString() {
    return "Table{" +
      "schema='" + schema + '\'' +
      ", tableName='" + tableName + '\'' +
      ", type=" + type +
      ", location='" + location + '\'' +
      ", format=" + format +
      '}';
  }
}
