package ru.ibs.dtm.common.schema.codec.type;

import org.apache.avro.LogicalType;

import java.time.LocalDateTime;

public class LocalDateTimeLogicalType extends LogicalType {

  public LocalDateTimeLogicalType() {
    super(LocalDateTime.class.getSimpleName());
  }

  public static LocalDateTimeLogicalType INSTANCE = new LocalDateTimeLogicalType();
}
