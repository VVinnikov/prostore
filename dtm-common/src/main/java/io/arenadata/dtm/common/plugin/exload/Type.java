package io.arenadata.dtm.common.plugin.exload;

import com.fasterxml.jackson.annotation.JsonFormat;

@JsonFormat(shape = JsonFormat.Shape.NUMBER_INT)
public enum Type {
  CSV_FILE("file"),
  HDFS_LOCATION("hdfs"),
  KAFKA_TOPIC("kafka");

  private String name;

  Type(String name) {
    this.name = name;
  }

  public static Type findByName(String name) {
    for (Type type : Type.values()) {
      if (type.name.equalsIgnoreCase(name)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Не найден соответствующий тип для " + name);
  }

  public String getName() {
    return name;
  }
}
