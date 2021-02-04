package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;

@EqualsAndHashCode(callSuper = true)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class LocalDateFromIntTransformer extends AbstractColumnTransformer<LocalDate, Integer> {

    private ZoneId zoneId = ZoneId.of("UTC");

    @Override
    public LocalDate transformValue(Integer value) {
        return value == null ? null : LocalDate.ofEpochDay(value.longValue());
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Collections.singletonList(Integer.class);
    }

    @Override
    public ColumnType getType() {
        return ColumnType.DATE;
    }
}
