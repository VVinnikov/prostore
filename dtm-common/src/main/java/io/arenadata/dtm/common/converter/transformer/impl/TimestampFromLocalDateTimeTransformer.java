package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.util.Collection;
import java.util.Collections;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TimestampFromLocalDateTimeTransformer extends AbstractColumnTransformer<Long, LocalDateTime> {

    private ZoneId zoneId = ZoneId.of("UTC");

    @Override
    public Long transformValue(LocalDateTime value) {
        if (value != null) {
            Instant instant = getInstant(value);
            return instant.getLong(ChronoField.INSTANT_SECONDS) * 1000L * 1000L + instant.getLong(ChronoField.MICRO_OF_SECOND);
        }
        return null;
    }

    private Instant getInstant(LocalDateTime value) {
        return value.atZone(zoneId).toInstant();
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Collections.singletonList(LocalDateTime.class);
    }

    @Override
    public ColumnType getType() {
        return ColumnType.TIMESTAMP;
    }
}
