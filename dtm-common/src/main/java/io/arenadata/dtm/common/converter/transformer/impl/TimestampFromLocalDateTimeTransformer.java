package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
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
            int micros = instant.getNano() / 1000;
            return instant.toEpochMilli() / 1000 * 1000000 + micros;
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
