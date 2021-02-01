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
public class LocalDateFromLongTransformer extends AbstractColumnTransformer<LocalDate, Long> {

    private ZoneId zoneId = ZoneId.of("UTC");

    @Override
    public LocalDate transformValue(Long value) {
        return value == null ? null : LocalDateTime.ofInstant(Instant.ofEpochMilli(value),
                this.zoneId).toLocalDate();
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Collections.singletonList(Long.class);
    }

    @Override
    public ColumnType getType() {
        return ColumnType.DATE;
    }
}
