package ru.ibs.dtm.common.schema.codec.conversion;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import ru.ibs.dtm.common.schema.codec.type.LocalDateTimeLogicalType;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.regex.Pattern;

public class LocalDateTimeConversion extends Conversion<LocalDateTime> {
    private static final Pattern LOCAL_DATE_TIME_PATTERN
            = Pattern.compile("^\\d\\d\\d\\d-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d(.\\d\\d\\d|)$");

    private LocalDateTimeConversion() {
        super();
    }

    public static LocalDateTimeConversion getInstance() {
        return LocalDateTimeConversionHolder.INSTANCE;
    }

    @Override
    public Class<LocalDateTime> getConvertedType() {
        return LocalDateTime.class;
    }

    @Override
    public String getLogicalTypeName() {
        return LocalDateTimeLogicalType.INSTANCE.getName();
    }

    @Override
    public Schema getRecommendedSchema() {
        return LocalDateTimeLogicalType.INSTANCE.addToSchema(Schema.create(Schema.Type.STRING));
    }

    @Override
    public Long toLong(LocalDateTime value, Schema schema, LogicalType type) {
        return value.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    @Override
    public LocalDateTime fromLong(Long value, Schema schema, LogicalType type) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(value), ZoneId.systemDefault());
    }

    @Override
    public LocalDateTime fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
        return LOCAL_DATE_TIME_PATTERN.matcher(value).matches() ? LocalDateTime.parse(value) : ZonedDateTime.parse(value).toLocalDateTime();
    }

    @Override
    public CharSequence toCharSequence(LocalDateTime value, Schema schema, LogicalType type) {
        return value.toString();
    }

    private static class LocalDateTimeConversionHolder {
        private static final LocalDateTimeConversion INSTANCE = new LocalDateTimeConversion();
    }
}
