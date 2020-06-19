package ru.ibs.dtm.common.schema.codec.type;

import org.apache.avro.LogicalType;

import java.math.BigDecimal;

public class BigDecimalLogicalType  extends LogicalType {

  public BigDecimalLogicalType() {
    super(BigDecimal.class.getSimpleName());
  }

  public static BigDecimalLogicalType INSTANCE = new BigDecimalLogicalType();
}
