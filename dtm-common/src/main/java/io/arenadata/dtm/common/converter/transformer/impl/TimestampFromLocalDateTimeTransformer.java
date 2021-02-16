package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

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
        return value == null ? null : value.atZone(zoneId).toInstant().toEpochMilli();
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
