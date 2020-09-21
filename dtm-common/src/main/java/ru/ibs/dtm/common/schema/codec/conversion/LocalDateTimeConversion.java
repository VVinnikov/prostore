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
    private static final Pattern TIME_ZONE_PATTERN
            = Pattern.compile("(?:Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9]|\\w+/\\w+)$");

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
        return LocalDateTimeLogicalType.INSTANCE.addToSchema(Schema.create(Schema.Type.LONG));
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
        return TIME_ZONE_PATTERN.matcher(value).find() ? ZonedDateTime.parse(value).toLocalDateTime() : LocalDateTime.parse(value);
    }

    @Override
    public CharSequence toCharSequence(LocalDateTime value, Schema schema, LogicalType type) {
        return value.toString();
    }

    private static class LocalDateTimeConversionHolder {
        private static final LocalDateTimeConversion INSTANCE = new LocalDateTimeConversion();
    }
}
