package ru.ibs.dtm.common.schema.codec.conversion;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import ru.ibs.dtm.common.schema.codec.type.BigDecimalLogicalType;

import java.math.BigDecimal;

public class BigDecimalConversion extends Conversion<BigDecimal> {

  @Override
  public Class<BigDecimal> getConvertedType() {
    return BigDecimal.class;
  }

  @Override
  public String getLogicalTypeName() {
    return BigDecimalLogicalType.INSTANCE.getName();
  }

  @Override
  public Schema getRecommendedSchema() {
    return BigDecimalLogicalType.INSTANCE.addToSchema(Schema.create(Schema.Type.STRING));
  }

  @Override
  public CharSequence toCharSequence(BigDecimal value, Schema schema, LogicalType type) {
    return value.toString();
  }

  @Override
  public BigDecimal fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
    return new BigDecimal(value.toString());
  }
}
