package ru.ibs.dtm.common.schema.codec.conversion;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import ru.ibs.dtm.common.schema.codec.type.LocalDateLogicalType;
import ru.ibs.dtm.common.schema.codec.type.LocalTimeLogicalType;

import java.time.LocalTime;

public class LocalTimeConversion extends Conversion<LocalTime> {

    private LocalTimeConversion() {
        super();
    }

    public static LocalTimeConversion getInstance() {
        return LocalTimeConversion.LocalTimeConversionHolder.INSTANCE;
    }

    @Override
    public Class<LocalTime> getConvertedType() {
        return LocalTime.class;
    }

    @Override
    public String getLogicalTypeName() {
        return LocalDateLogicalType.INSTANCE.getName();
    }

    @Override
    public Schema getRecommendedSchema() {
        return LocalTimeLogicalType.INSTANCE.addToSchema(Schema.create(Schema.Type.LONG));
    }

    @Override
    public Long toLong(LocalTime value, Schema schema, LogicalType type) {
        return value.toNanoOfDay();
    }

    @Override
    public LocalTime fromLong(Long value, Schema schema, LogicalType type) {
        return LocalTime.ofNanoOfDay(value);
    }

    @Override
    public LocalTime fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
        return LocalTime.parse(value);
    }

    @Override
    public CharSequence toCharSequence(LocalTime value, Schema schema, LogicalType type) {
        return value.toString();
    }

    private static class LocalTimeConversionHolder {
        private static final LocalTimeConversion INSTANCE = new LocalTimeConversion();
    }
}
