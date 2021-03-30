package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Collections;

@EqualsAndHashCode(callSuper = true)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class StringTimestampFromLongTransformer extends AbstractColumnTransformer<String, Long> {

    private DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    private ZoneId zoneId = ZoneId.of("UTC");

    @Override
    public String transformValue(Long value) {
        return  LocalDateTime.ofInstant(Instant.ofEpochMilli(value),
                this.zoneId).format(dateTimeFormatter);
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Collections.singletonList(Long.class);
    }

    @Override
    public ColumnType getType() {
        return ColumnType.TIMESTAMP;
    }
}
