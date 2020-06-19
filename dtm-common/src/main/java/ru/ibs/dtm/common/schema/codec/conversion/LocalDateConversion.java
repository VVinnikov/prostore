package ru.ibs.dtm.common.schema.codec.conversion;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import ru.ibs.dtm.common.schema.codec.type.LocalDateLogicalType;

import java.time.LocalDate;

public class LocalDateConversion extends Conversion<LocalDate> {
    private LocalDateConversion() {
        super();
    }

    public static LocalDateConversion getInstance() {
        return LocalDateConversion.LocalDateConversionHolder.INSTANCE;
    }

    @Override
    public Class<LocalDate> getConvertedType() {
        return LocalDate.class;
    }

    @Override
    public String getLogicalTypeName() {
        return LocalDateLogicalType.INSTANCE.getName();
    }

    @Override
    public Schema getRecommendedSchema() {
        return LocalDateLogicalType.INSTANCE.addToSchema(Schema.create(Schema.Type.INT));
    }

    @Override
    public Integer toInt(LocalDate value, Schema schema, LogicalType type) {
        return (int) value.toEpochDay();
    }

    @Override
    public LocalDate fromInt(Integer value, Schema schema, LogicalType type) {
        return LocalDate.ofEpochDay(value.longValue());
    }

    @Override
    public LocalDate fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
        return LocalDate.parse(value);
    }

    @Override
    public CharSequence toCharSequence(LocalDate value, Schema schema, LogicalType type) {
        return value.toString();
    }

    private static class LocalDateConversionHolder {
        private static final LocalDateConversion INSTANCE = new LocalDateConversion();
    }
}
