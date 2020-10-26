package io.arenadata.dtm.common.schema.codec.type;

import org.apache.avro.LogicalType;

import java.time.LocalDate;

public class LocalDateLogicalType extends LogicalType {
  public LocalDateLogicalType() {
    super(LocalDate.class.getSimpleName());
  }

  public static LocalDateLogicalType INSTANCE = new LocalDateLogicalType();
}
